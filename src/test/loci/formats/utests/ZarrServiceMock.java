package test.loci.formats.utests;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import loci.common.services.AbstractService;
import loci.formats.FormatException;
import loci.formats.meta.MetadataRetrieve;
import loci.formats.services.ZarrService;

public class ZarrServiceMock extends AbstractService
implements ZarrService {
  public static final String NO_ZARR_MSG = "JZARR is required to read Zarr files.";
  
  private int[] shape;
  private int[] chunkSize;
  private int pixelType;
  private boolean isLittleEndian;
  private boolean isOpen = false;
  private String id;
  
  @Override
  public String getNoZarrMsg() {
    // TODO Auto-generated method stub
    return NO_ZARR_MSG;
  }

  @Override
  public int[] getShape() {
    return shape;
  }

  @Override
  public int[] getChunkSize() {
    return chunkSize;
  }

  @Override
  public int getPixelType() {
    return pixelType;
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
  }

  @Override
  public Object readBytes(int[] shape, int[] offset) throws FormatException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void saveBytes(Object data, int[] shape, int[] offset) throws FormatException, IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void open(String file) throws IOException, FormatException {
    isOpen = true;
    id = file;
  }

  @Override
  public boolean isLittleEndian() {
    return isLittleEndian;
  }

  @Override
  public boolean isOpen() throws IOException {
    return isOpen;
  }

  @Override
  public String getID() throws IOException {
    return id;
  }

  @Override
  public void create(String id, MetadataRetrieve meta, int[] chunks) throws IOException {
    // TODO 
    
  }

  @Override
  public void create(String id, MetadataRetrieve meta, int[] chunks, Compression compression) throws IOException {
    // TODO 
    
  }

  @Override
  public Map<String, Object> getGroupAttr(String path) throws IOException, FormatException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, Object> getArrayAttr(String path) throws IOException, FormatException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<String> getGroupKeys(String path) throws IOException, FormatException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<String> getArrayKeys(String path) throws IOException, FormatException {
    // TODO Auto-generated method stub
    return null;
  }

}
