package loci.formats;

import com.bc.zarr.ZarrConstants;
import com.bc.zarr.ZarrUtils;
import com.bc.zarr.storage.Store;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.TreeSet;

public class S3FileSystemStore implements Store {

    private Path root;

    public S3FileSystemStore(String path, FileSystem fileSystem) {
        if (fileSystem == null) {
            root = Paths.get(path);
        } else {
            root = fileSystem.getPath(path);
        }
    }

    public S3FileSystemStore(Path rootPath) {
        root = rootPath;
    }

    @Override
    public InputStream getInputStream(String key) throws IOException {
        final Path path = root.resolve(key);
        if (Files.isReadable(path)) {
            return Files.newInputStream(path);
        } else {
        
        String[] pathSplit = root.toString().split(File.separator);
        String endpoint = "https://" + pathSplit[1] + File.separator;
        String bucketName =  pathSplit[2];
        String key2 = root.toString().substring(root.toString().indexOf(pathSplit[3]), root.toString().length()) + File.separator + key;

        URI endpoint_uri;
        try {
          endpoint_uri = new URI(endpoint);
          final S3Configuration config = S3Configuration.builder()
            .pathStyleAccessEnabled(true)
            .build();
          AwsCredentials credentials = AnonymousCredentialsProvider.create().resolveCredentials();
          S3Client client = S3Client.builder()
            .endpointOverride(endpoint_uri)
            .serviceConfiguration(config)
            .region(Region.EU_WEST_1) // Ignored but required by the client
            .credentialsProvider(StaticCredentialsProvider.create(credentials)).build();
          GetObjectRequest getRequest = GetObjectRequest.builder().bucket(bucketName).key(key2).build();
          ResponseInputStream<GetObjectResponse> responseStream = client.getObject(getRequest, ResponseTransformer.toInputStream());
          responseStream = client.getObject(getRequest, ResponseTransformer.toInputStream());
          return responseStream;
        } catch (URISyntaxException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }  
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

    private TreeSet<String> getKeysFor(String suffix) throws IOException {
      TreeSet<String> keys = new TreeSet<String>();
      
      String[] pathSplit = root.toString().split(File.separator);
      
      String endpoint = "https://" + pathSplit[1] + File.separator;
      String bucketName =  pathSplit[2];
      String key2 = root.toString().substring(root.toString().indexOf(pathSplit[3]), root.toString().length());
  
      //TODO: Reused connection code
      URI endpoint_uri;
      try {
        endpoint_uri = new URI(endpoint);
  
        final S3Configuration config = S3Configuration.builder()
          .pathStyleAccessEnabled(true)
          .build();
        AwsCredentials credentials = AnonymousCredentialsProvider.create().resolveCredentials();
        S3Client client = S3Client.builder()
          .endpointOverride(endpoint_uri)
          .serviceConfiguration(config)
          .region(Region.EU_WEST_1) // Ignored but required by the client
          .credentialsProvider(StaticCredentialsProvider.create(credentials)).build();
        List<S3Object> list = client.listObjects(ListObjectsRequest.builder()
          .bucket(bucketName)
          .prefix(key2)
          .build()).contents();
        int n = list.size();

        for (int i = 0; i < n; i++) {
          S3Object object = list.get(i);
          String k = object.key();
          if (k.contains(suffix)) {
            String key = k.substring(k.indexOf(key2) + key2.length() + 1, k.indexOf(suffix));
            if (!key.isEmpty()) {
              keys.add(key.substring(0, key.length()-1));
            }
          }
        }
      } catch (URISyntaxException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return keys;
    }
}
