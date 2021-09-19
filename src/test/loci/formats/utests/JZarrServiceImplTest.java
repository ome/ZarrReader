package test.loci.formats.utests;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;

import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.services.JZarrServiceImpl;
import ucar.ma2.InvalidRangeException;

public class JZarrServiceImplTest {
  
  JZarrServiceImpl jzarrService;
  String testID = "testID";
  private static MockedStatic<ZarrGroup> zarrGroupStatic;
  private static MockedStatic<ZarrArray> zarrArrayStatic;
  
  @Mock
  ZarrArray zarrArray;
  
  @Mock
  ZarrGroup zarrGroup;
  
  @BeforeMethod
  public void setup() {
    jzarrService = new JZarrServiceImpl(null);
    zarrArray = Mockito.mock(ZarrArray.class);
    zarrGroup = Mockito.mock(ZarrGroup.class);
    jzarrService.open(testID, zarrArray);
    
    zarrGroupStatic = Mockito.mockStatic(ZarrGroup.class);
    zarrGroupStatic.when(() -> ZarrGroup.open("path")).thenReturn(zarrGroup);
    
    zarrArrayStatic = Mockito.mockStatic(ZarrArray.class);
    zarrArrayStatic.when(() -> ZarrArray.open("path")).thenReturn(zarrArray);
  }
  
  @AfterMethod
  public void teardown() {
    try {
      jzarrService.close();
      zarrGroupStatic.close();
      zarrArrayStatic.close();
    } catch (IOException e) {
      fail("IOException thrown while closing JZarrServiceImpl");
    }
  }
  
  @DataProvider(name = "pixelTypes")
  public Object[][] createPixelTypes() {
    return new Object[][] {
      {FormatTools.INT8, DataType.i1},
      {FormatTools.INT16, DataType.i2},
      {FormatTools.INT32, DataType.i4},
      {FormatTools.UINT8, DataType.u1},
      {FormatTools.UINT16, DataType.u2},
      {FormatTools.UINT32, DataType.u4},
      {FormatTools.FLOAT, DataType.f4},
      {FormatTools.DOUBLE, DataType.f8},
    };
  }

  @Test(dataProvider = "pixelTypes")
  public void testZarrPixelType(int omePixelType, DataType jzarrPixelType) {
    assertEquals(jzarrPixelType, jzarrService.getZarrPixelType(omePixelType));
  }
  
  @Test(dataProvider = "pixelTypes")
  public void testOMEPixelType(int omePixelType, DataType jzarrPixelType) {
    assertEquals(omePixelType, jzarrService.getOMEPixelType(jzarrPixelType));
  }
  
  @Test(dataProvider = "pixelTypes")
  public void testPixelType(int omePixelType, DataType jzarrPixelType) {
    when(zarrArray.getDataType()).thenReturn(jzarrPixelType);
    assertEquals(omePixelType, jzarrService.getPixelType());
  }
  
  @Test
  public void testIsLittleEndian() {
    when(zarrArray.getByteOrder()).thenReturn(ByteOrder.BIG_ENDIAN);
    assertEquals(false, jzarrService.isLittleEndian());
    when(zarrArray.getByteOrder()).thenReturn(ByteOrder.LITTLE_ENDIAN);
    assertEquals(true, jzarrService.isLittleEndian());
  }
  
  @Test
  public void testGetNoZarrMessage() {
    assertEquals(JZarrServiceImpl.NO_ZARR_MSG, jzarrService.getNoZarrMsg());
  }
  
  @Test
  public void testGetShape() {
    int[] expectedShape = {1024, 1024, 32, 32, 32};
    when(zarrArray.getShape()).thenReturn(expectedShape);
    assertEquals(expectedShape, jzarrService.getShape());
  }
  
  @Test
  public void testGetChunkSize() {
    int[] expectedChunks = {256, 256, 8, 8, 8};
    when(zarrArray.getChunks()).thenReturn(expectedChunks);
    assertEquals(expectedChunks, jzarrService.getChunkSize());
  }
  
  @Test
  public void testGetGroupAttributes() {
    Map<String, Object> emptyAttributes = new HashMap<String, Object>();
    Map<String, Object> attributes = new HashMap<String, Object>();
    attributes.put("AttributeKey1", "AttributeValue1");
    attributes.put("AttributeKey2", "AttributeValue2");

    try {
      assertEquals(jzarrService.getGroupAttr("path"), emptyAttributes);
      when(zarrGroup.getAttributes()).thenReturn(attributes);
      assertEquals(jzarrService.getGroupAttr("path"), attributes);
    } catch (IOException | FormatException e) {
      fail("Unexpected exception thrown while retrieving group attributes");
      e.printStackTrace();
    }
  }

  @Test
  public void testGetGroupKeys() {
    Set<String> emptyKeys = new HashSet<String>();
    Set<String> keys = new HashSet<String>();
    keys.add("Key1");
    keys.add("Key2");

    try {
      assertEquals(jzarrService.getGroupKeys("path"), emptyKeys);
      when(zarrGroup.getGroupKeys()).thenReturn(keys);
      assertEquals(jzarrService.getGroupKeys("path"), keys);
    } catch (IOException | FormatException e) {
      fail("Unexpected exception thrown while retrieving group keys");
      e.printStackTrace();
    }
  }

  @Test
  public void testGetArrayAttributes() {
    Map<String, Object> emptyAttributes = new HashMap<String, Object>();
    Map<String, Object> attributes = new HashMap<String, Object>();
    attributes.put("AttributeKey1", "AttributeValue1");
    attributes.put("AttributeKey2", "AttributeValue2");

    try {
      assertEquals(jzarrService.getArrayAttr("path"), emptyAttributes);
      when(zarrArray.getAttributes()).thenReturn(attributes);
      assertEquals(jzarrService.getArrayAttr("path"), attributes);
    } catch (IOException | FormatException e) {
      fail("Unexpected exception thrown while retrieving array attributes");
      e.printStackTrace();
    }
  }
  
  @Test
  public void testGetArrayKeys() {
    Set<String> emptyKeys = new HashSet<String>();
    Set<String> keys = new HashSet<String>();
    keys.add("Key1");
    keys.add("Key2");

    try {
      assertEquals(jzarrService.getArrayKeys("path"), emptyKeys);
      when(zarrGroup.getArrayKeys()).thenReturn(keys);
      assertEquals(jzarrService.getArrayKeys("path"), keys);
    } catch (IOException | FormatException e) {
      fail("Unexpected exception thrown while retrieving array keys");
      e.printStackTrace();
    }
  }
  
  @Test
  public void testGetID() {
    assertEquals(testID, jzarrService.getID());
    JZarrServiceImpl nullJzarrService = new JZarrServiceImpl(null);
    assertEquals(null, nullJzarrService.getID());
    nullJzarrService.open("TestGetID", zarrArray);
    assertEquals("TestGetID", nullJzarrService.getID());
  }

  @Test
  public void testIsOpen() {
    assertEquals(true, jzarrService.isOpen());
    JZarrServiceImpl nullJzarrService = new JZarrServiceImpl(null);
    assertEquals(false, nullJzarrService.isOpen());
    nullJzarrService.open("isOpenTestID", zarrArray);
    assertEquals(true, nullJzarrService.isOpen());
  }

  @Test
  public void testReadBytes() {
    int[] expectedBytes = {256, 256, 8, 8, 8};
    int[] shape = {1024, 1024, 32, 32, 32};
    int[] offset = {0, 0, 0, 0, 0};
    try {
      when(zarrArray.read(shape, offset)).thenReturn(expectedBytes);
      assertEquals(expectedBytes, jzarrService.readBytes(shape, offset));
    } catch (IOException e) {
      fail("Unexpected exception on JZarrServiceImpl readBytes");
      e.printStackTrace();
    } catch (InvalidRangeException e) {
      fail("Unexpected InvalidRangeException on ZarrArray read");
      e.printStackTrace();
    } catch (FormatException e) {
      fail("Unexpected FormatException on JZarrServiceImpl readBytes");
      e.printStackTrace();
    }
  }

  @Test
  public void testSaveBytes() {
    int[] data = {256, 256, 8, 8, 8};
    int[] shape = {1024, 1024, 32, 32, 32};
    int[] offset = {0, 0, 0, 0, 0};
    try {
      jzarrService.saveBytes(data, shape, offset);
      verify(zarrArray).write(data, shape, offset);
    } catch (IOException e) {
      fail("Unexpected IOException on JZarrServiceImpl saveBytes");
      e.printStackTrace();
    } catch (InvalidRangeException e) {
      fail("Unexpected InvalidRangeException on ZarrArray write");
      e.printStackTrace();
    } catch (FormatException e) {
      fail("Unexpected FormatException on JZarrServiceImpl saveBytes");
      e.printStackTrace();
    }
  }
}
