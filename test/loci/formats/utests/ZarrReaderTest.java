package test.loci.formats.utests;

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

import static org.junit.Assert.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import loci.common.Location;
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
    String canonicalPath = new Location(rootPath).getCanonicalPath();

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
    datasets.put("axes", Arrays.asList("t", "c", "z", "y", "x"));
    multiscales.add(datasets);
    topLevelAttributes.put("multiscales", multiscales);

    when(zarrService.getGroupAttr(canonicalPath)).thenReturn(topLevelAttributes);
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
    int [] expectedChunks = {8, 8, 8, 256, 128};
    when(zarrService.getChunkSize()).thenReturn(expectedChunks);
    assertEquals(expectedChunks[3], reader.getOptimalTileHeight());
  }
  
  @Test
  public void testGetOptimalTileWidth() {
    int [] expectedChunks = {8, 8, 8, 256, 128};
    when(zarrService.getChunkSize()).thenReturn(expectedChunks);
    assertEquals(expectedChunks[4], reader.getOptimalTileWidth());
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
        buf = new byte[1024*512*4];
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
  
  @Test
  public void testParseOmeroMetadataWithIntegerValues() {
    Map<String, Object> omeroMetadata = new HashMap<>();
    omeroMetadata.put("id", 1);
    omeroMetadata.put("name", "Test Image");
    omeroMetadata.put("version", "0.1");

    ArrayList<Object> channels = new ArrayList<>();
    Map<String, Object> channel = new HashMap<>();
    channel.put("active", true);
    channel.put("coefficient", 1);
    channel.put("color", "FFFFFF");
    channel.put("family", "linear");
    channel.put("inverted", false);
    channel.put("label", "Channel 1");

    Map<String, Object> window = new HashMap<>();
    window.put("start", 0);
    window.put("end", 255);
    window.put("min", 0);
    window.put("max", 255);
    channel.put("window", window);

    channels.add(channel);
    omeroMetadata.put("channels", channels);

    Map<String, Object> rdefs = new HashMap<>();
    rdefs.put("defaultT", 0);
    rdefs.put("defaultZ", 0);
    rdefs.put("model", "color");
    omeroMetadata.put("rdefs", rdefs);

    Map<String, Object> test = new HashMap<>();
    test.put("omero", omeroMetadata);
    try {
      reader.parseOmeroMetadata(test);
    } catch (IOException | FormatException e) {
      fail("Unexpected exception while parsing Omero metadata with Integer values");
    }
  }

  @Test
  public void testParseOmeroMetadataWithDoubleValues() {
    Map<String, Object> omeroMetadata = new HashMap<>();
    omeroMetadata.put("id", 1);
    omeroMetadata.put("name", "Test Image");
    omeroMetadata.put("version", "0.1");

    ArrayList<Object> channels = new ArrayList<>();
    Map<String, Object> channel = new HashMap<>();
    channel.put("active", true);
    channel.put("coefficient", 1.0);
    channel.put("color", "FFFFFF");
    channel.put("family", "linear");
    channel.put("inverted", false);
    channel.put("label", "Channel 1");

    Map<String, Object> window = new HashMap<>();
    window.put("start", 0.0);
    window.put("end", 255.0);
    window.put("min", 0.0);
    window.put("max", 255.0);
    channel.put("window", window);

    channels.add(channel);
    omeroMetadata.put("channels", channels);

    Map<String, Object> rdefs = new HashMap<>();
    rdefs.put("defaultT", 0);
    rdefs.put("defaultZ", 0);
    rdefs.put("model", "color");
    omeroMetadata.put("rdefs", rdefs);

    Map<String, Object> test = new HashMap<>();
    test.put("omero", omeroMetadata);
    try {
      reader.parseOmeroMetadata(test);
    } catch (IOException | FormatException e) {
      fail("Unexpected exception while parsing Omero metadata with Double values");
    }
  }
}
