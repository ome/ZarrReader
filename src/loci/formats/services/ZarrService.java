package loci.formats.services;

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
