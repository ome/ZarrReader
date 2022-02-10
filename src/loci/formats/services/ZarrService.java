package loci.formats.services;

/*-
 * #%L
 * OME-Zarr Bio-Formats reader
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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import loci.common.services.Service;
import loci.formats.FormatException;
import loci.formats.meta.MetadataRetrieve;
import loci.formats.services.ZarrService.Compression;

public interface ZarrService extends Service {
  
  enum Compression {
    NONE,
    ZLIB
  }
  

  /**
   * Gets the text string for when Zarr implementation has not been found.
   */
  public String getNoZarrMsg();

  /**
   * Gets shape of Zarr as an array of dimensions.
   * @return  shape.
   */
  public int [] getShape();

  /**
   * Gets the chunk size as an array of dimensions.
   * @return  chunk size.
   */
  public int [] getChunkSize();

  /**
   * Gets the image pixel type.
   * @return                    pixel type.
   */
  public int getPixelType();

  /**
   * Closes the file.
   */
  public void close() throws IOException;

  /**
  * Reads values from the Zarr Array
  * @return     Buffer of bytes read.
  * @param      shape           int array representing the shape of each dimension
  * @param      offset          buffer for bytes.
  */
  public Object readBytes(int [] shape, int [] offset) throws FormatException, IOException;

  /**
  * Writes values to the Zarr Array
  * @param      buf            values to be written in a one dimensional array
  * @param      shape           int array representing the shape of each dimension
  * @param      x               the offset for each dimension
  */
  void saveBytes(Object data, int[] shape, int[] offset) throws FormatException, IOException;

  public void open(String file) throws IOException, FormatException;

  boolean isLittleEndian();

  boolean isOpen() throws IOException;

  String getID() throws IOException;

  public void create(String id, MetadataRetrieve meta, int[] chunks) throws IOException;

  void create(String id, MetadataRetrieve meta, int[] chunks, Compression compression) throws IOException;

  public Map<String, Object> getGroupAttr(String path) throws IOException, FormatException;
  
  public Map<String, Object> getArrayAttr(String path) throws IOException, FormatException;
  
  public Set<String> getGroupKeys(String path) throws IOException, FormatException;
  
  public Set<String> getArrayKeys(String path) throws IOException, FormatException;
}
