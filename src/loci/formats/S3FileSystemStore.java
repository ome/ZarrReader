package loci.formats;

/*-
 * #%L
 * Implementation of Bio-Formats readers for the next-generation file formats
 * %%
 * Copyright (C) 2020 - 2022 Open Microscopy Environment
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */

import com.bc.zarr.ZarrConstants;
import com.bc.zarr.ZarrUtils;
import com.bc.zarr.storage.Store;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.ListIterator;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;

public class S3FileSystemStore implements Store {

    private Path root;
    AmazonS3 client;
    public static final String ENDPOINT_PROTOCOL= "https://";
    protected static final Logger LOGGER =
        LoggerFactory.getLogger(S3FileSystemStore.class);

    public S3FileSystemStore(String path, FileSystem fileSystem) {
        if (fileSystem == null) {
            root = Paths.get(path);
        } else {
            root = fileSystem.getPath(path);
        }
        setupClient();
    }
    
    public void updateRoot(String path) {
      root = Paths.get(path);
    }
    
    public String getRoot() {
      return root.toString();
    }

    private void setupClient() {
      String[] pathSplit = root.toString().split(File.separator);
      String endpoint = ENDPOINT_PROTOCOL + pathSplit[1] + File.separator;
      try {   
        client = AmazonS3ClientBuilder.standard()
          .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "auto"))
          .withPathStyleAccessEnabled(true)
          .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials())).build();
      } catch (Exception e) {
        LOGGER.info("Exception caught while constructing S3 client", e);
      } 
      
    }
    
    public void close() {
      if (client != null) {
        client.shutdown();
      }
    }

    public S3FileSystemStore(Path rootPath) {
        root = rootPath;
        setupClient();
    }

    @Override
    public InputStream getInputStream(String key) throws IOException {
        // Get the base bucket name from splitting the root path and removing the prefixed protocol and end-point
        String[] pathSplit = root.toString().split(File.separator);
        String bucketName =  pathSplit[2];
        
        // Append the desired key onto the remaining prefix
        String key2 = root.toString().substring(root.toString().indexOf(pathSplit[3]), root.toString().length()) + File.separator + key;

        try {   
          S3Object o = client.getObject(bucketName, key2);
          S3ObjectInputStream responseStream = o.getObjectContent();
          return responseStream;
        } catch (Exception e) {
          LOGGER.info( "Unable to locate or access key: " + key2, e);
        }

      return null;
    }

    @Override
    public OutputStream getOutputStream(String key) throws IOException {
        final Path filePath = root.resolve(key);
        final Path dir = filePath.getParent();
        Files.createDirectories(dir);
        return Files.newOutputStream(filePath);
    }

    @Override
    public void delete(String key) throws IOException {
        final Path toBeDeleted = root.resolve(key);
        if (Files.isDirectory(toBeDeleted)) {
            ZarrUtils.deleteDirectoryTreeRecursively(toBeDeleted);
        }
        if (Files.exists(toBeDeleted)){
            Files.delete(toBeDeleted);
        }
        if (Files.exists(toBeDeleted)|| Files.isDirectory(toBeDeleted)) {
            throw new IOException("Unable to initialize " + toBeDeleted.toAbsolutePath().toString());
        }
    }

    @Override
    public TreeSet<String> getArrayKeys() throws IOException {
        return getKeysFor(ZarrConstants.FILENAME_DOT_ZARRAY);
    }

    @Override
    public TreeSet<String> getGroupKeys() throws IOException {
        return getKeysFor(ZarrConstants.FILENAME_DOT_ZGROUP);
    }

    /**
     * Copied from {@com.bc.zarr.storage.FileSystemStorage#getKeysEndingWith(String).
     *
     * @param suffix
     * @return
     * @throws IOException
     */
    public TreeSet<String> getKeysEndingWith(String suffix) throws IOException {
        return (TreeSet<String>)Files.walk(this.root).filter((path) -> {
            return path.toString().endsWith(suffix);
        }).map((path) -> {
            return this.root.relativize(path).toString();
        }).collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Copied from {@com.bc.zarr.storage.FileSystemStorage#getRelativeLeafKeys(String).
     *
     * @param key
     * @return
     * @throws IOException
     */
    public Stream<String> getRelativeLeafKeys(String key) throws IOException {
        Path walkingRoot = this.root.resolve(key);
        return Files.walk(walkingRoot).filter((path) -> {
            return !Files.isDirectory(path, new LinkOption[0]);
        }).map((path) -> {
            return walkingRoot.relativize(path).toString();
        }).map(ZarrUtils::normalizeStoragePath).filter((s) -> {
            return s.trim().length() > 0;
        });
    }

    private TreeSet<String> getKeysFor(String suffix) throws IOException {
      TreeSet<String> keys = new TreeSet<String>();

      // Get the base bucket name from splitting the root path and removing the prefixed protocol and end-point
      String[] pathSplit = root.toString().split(File.separator);
      String bucketName =  pathSplit[2];
      
      // Append the desired key onto the remaining prefix
      String key2 = root.toString().substring(root.toString().indexOf(pathSplit[3]), root.toString().length());

      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucketName)
          .withPrefix(key2)
        ;

      ObjectListing listObjectsResponse = null;
      String lastKey = null;
        
      do {
        if ( listObjectsResponse != null ) {
          listObjectsRequest = listObjectsRequest
             .withMarker(lastKey)
          ; 
        }

        listObjectsResponse = client.listObjects(listObjectsRequest); 
        List<S3ObjectSummary> objects = listObjectsResponse.getObjectSummaries();

        // Iterate over results
        ListIterator<S3ObjectSummary> iterVals = objects.listIterator();
        while (iterVals.hasNext()) {
          S3ObjectSummary object = (S3ObjectSummary) iterVals.next();
          String k = object.getKey();
          if (k.contains(suffix)) {
            String key = k.substring(k.indexOf(key2) + key2.length() + 1, k.indexOf(suffix));
            if (!key.isEmpty()) {
              keys.add(key.substring(0, key.length()-1));
            }
          }
          lastKey = k;
        }
      } while ( listObjectsResponse.isTruncated() );
      
      return keys;
    }
}