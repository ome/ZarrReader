package test.loci.formats.utests;

import static org.junit.Assert.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import loci.common.DataTools;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.in.ZarrReader;
import loci.formats.services.ZarrService;


/**
 * Tests the functionality of ZarrReader
 */
public class ZarrReaderTest {
  
  @Mock
  private ZarrService zarrService;
  
  @InjectMocks
  private ZarrReaderMock reader;
  
  private File file;
  private int[] shape = {8, 16, 32, 512, 1024};
  
  @BeforeClass
  public void setUp() throws Exception {
    zarrService = Mockito.mock(ZarrService.class);
    reader = new ZarrReaderMock(zarrService);
    file = File.createTempFile("tileTest", ".zarr");
    String rootPath = file.getAbsolutePath();
    
    Map<String, Object> topLevelAttributes = new HashMap<String, Object>();
    ArrayList<Object> multiscales = new ArrayList<Object>();
    Map<String, Object> datasets = new HashMap<String, Object>();
    ArrayList<Object> multiscalePaths = new ArrayList<Object>();
    Map<String, Object> multiScale1 = new HashMap<String, Object>();
    Map<String, Object> multiScale2 = new HashMap<String, Object>();
    Map<String, Object> multiScale3 = new HashMap<String, Object>();
    multiScale1.put("path", "0");
    multiScale2.put("path", "1");
    multiScale3.put("path", "2");
    multiscalePaths.add(multiScale1);
    multiscalePaths.add(multiScale2);
    multiscalePaths.add(multiScale3);
    datasets.put("datasets", multiscalePaths);
    multiscales.add(datasets);
    topLevelAttributes.put("multiscales", multiscales);
    
    when(zarrService.getGroupAttr(rootPath)).thenReturn(topLevelAttributes);
    when(zarrService.getShape()).thenReturn(shape);
    when(zarrService.getPixelType()).thenReturn(0);
    reader.setId(file.getAbsolutePath());
  }
  
  @AfterClass
  public void tearDown() throws Exception {
    reader.close();
    file.delete();
  }
  
  @Test
  public void testIsThisType() {
    assertTrue(reader.isThisType("test/path/data.zarr", true));
    assertTrue(reader.isThisType("test/path/data.ZARR", true));
    assertTrue(reader.isThisType("test/path/data.zarr/0/0", true));
    assertTrue(reader.isThisType("test/path/data.zarr/0/0/0.0.0.0.0", true));
    assertTrue(reader.isThisType("test/path/data.zarr/0/0/.zattrs", true));
    assertFalse(reader.isThisType("test/path/data.zar", true));
  }
  
  @Test
  public void testGetOptimalTileHeight() {
    int [] expectedChunks = {128, 256, 8, 8, 8};
    when(zarrService.getChunkSize()).thenReturn(expectedChunks);
    assertEquals(expectedChunks[1], reader.getOptimalTileHeight());
  }
  
  @Test
  public void testGetOptimalTileWidth() {
    int [] expectedChunks = {128, 256, 8, 8, 8};
    when(zarrService.getChunkSize()).thenReturn(expectedChunks);
    assertEquals(expectedChunks[0], reader.getOptimalTileWidth());
  }
  
  @Test
  public void testDimensions() {
    assertEquals(shape[4], reader.getSizeX());
    assertEquals(shape[3], reader.getSizeY());
    assertEquals(shape[2], reader.getSizeZ());
    assertEquals(shape[1], reader.getSizeC());
    assertEquals(shape[0], reader.getSizeT());
  }
  
  @Test
  public void testOpenBytes() {
      int[] readerShape = {1, 1, 1, shape[3], shape[4]};
      int[] readerOffset = {0, 0, 0, 0, 0};
      int[] expectedPixelValues = new int[1024*512];
      for (int i = 0; i < expectedPixelValues.length; i++) {
        expectedPixelValues[i] = i;
      }
      byte[] buf = new byte[1024*512*4];
      byte[] expectedBuf = DataTools.intsToBytes(expectedPixelValues, false);
      try {
        when(zarrService.readBytes(readerShape, readerOffset)).thenReturn(expectedPixelValues);
        when(zarrService.getPixelType()).thenReturn(4);
        buf = reader.openBytes(0, buf);
        assertEquals(expectedBuf, buf);
        
        when(zarrService.readBytes(readerShape, readerOffset)).thenReturn(expectedBuf);
        when(zarrService.getPixelType()).thenReturn(0);
        buf = new byte[1024*512];
        buf = reader.openBytes(0, buf);
        assertEquals(expectedBuf, buf);
      } catch (FormatException | IOException e) {
        fail("Unexpected exception thrown while reading bytes");
      } 
  }
  
  @Test
  public void testGetDomains() {
    assertEquals(FormatTools.NON_SPECIAL_DOMAINS, reader.getDomains());
  }
  
  @Test
  public void testSetSeries() {
    assertEquals(0, reader.getSeries());
    reader.setSeries(1);
    assertEquals(1, reader.getSeries());
  }

  @Test
  public void testResolutionCount() {
    try {
      reader.close();
      reader.setFlattenedResolutions(false);
      reader.setId(file.getAbsolutePath());
    } catch (IOException | FormatException e) {
      fail("Unexpected exception while setting flattenedResolutions");
    }
    assertEquals(3, reader.getResolutionCount());
    assertEquals(1, reader.getSeriesCount());
    try {
      reader.close();
      reader.setFlattenedResolutions(true);
      reader.setId(file.getAbsolutePath());
    } catch (IOException | FormatException e) {
      fail("Unexpected exception while setting flattenedResolutions");
    }
    assertEquals(1, reader.getResolutionCount());
    assertEquals(3, reader.getSeriesCount());
  }
  
}
