/*
 * #%L
 * OME Bio-Formats package for reading and converting biological file formats.
 * %%
 * Copyright (C) 2020 Open Microscopy Environment:
 *   - Board of Regents of the University of Wisconsin-Madison
 *   - Glencoe Software, Inc.
 *   - University of Dundee
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

package loci.formats.in;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.FileUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.bc.zarr.ZarrUtils;

import loci.common.DataTools;
import loci.common.Location;
import loci.common.RandomAccessInputStream;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.common.xml.XMLTools;
import loci.formats.CoreMetadata;
import loci.formats.FormatException;
import loci.formats.FormatReader;
import loci.formats.FormatTools;
import loci.formats.MetadataTools;
import loci.formats.MissingLibraryException;
import loci.formats.SubResolutionFormatReader;
import loci.formats.meta.MetadataStore;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.services.JZarrServiceImpl;
import ome.xml.meta.MetadataConverter;
import loci.formats.services.OMEXMLService;
import loci.formats.services.ZarrService;


public class ZarrReader extends FormatReader {

  private transient ZarrService zarrService;
  private ArrayList<String> arrayPaths;
  private HashMap<Integer, ArrayList<String>> resSeries = new HashMap<Integer, ArrayList<String>>();
  private HashMap<String, Integer> resCounts = new HashMap<String, Integer>();
  private HashMap<String, Integer> resIndexes = new HashMap<String, Integer>();
  
  public ZarrReader() {
    super("Zarr", "zarr");
    suffixSufficient = false;
    domains = new String[] {FormatTools.UNKNOWN_DOMAIN};
  }
  
  /* @see loci.formats.IFormatReader#isThisType(String, boolean) */
  @Override
  public boolean isThisType(String name, boolean open) {
    Location zarrFolder = new Location(name).getParentFile();
    // if (zarrFolder != null && zarrFolder.exists() && zarrFolder.getAbsolutePath().indexOf(".zarr") > 0) {
    if (zarrFolder != null && zarrFolder.getAbsolutePath().indexOf(".zarr") > 0) {
      return true;
    }
    return super.isThisType(name, open);
  }

  /* @see loci.formats.IFormatReader#getOptimalTileHeight() */
  @Override
  public int getOptimalTileHeight() {
    FormatTools.assertId(currentId, true, 1);
    return zarrService.getChunkSize()[1];
  }
  
  /* @see loci.formats.IFormatReader#getOptimalTileHeight() */
  @Override
  public int getOptimalTileWidth() {
    FormatTools.assertId(currentId, true, 1);
    return zarrService.getChunkSize()[0];
  }
  
  /* @see loci.formats.FormatReader#initFile(String) */
  @Override
  protected void initFile(String id) throws FormatException, IOException {
    super.initFile(id);
    final MetadataStore store = makeFilterMetadata();
    Location zarrFolder = new Location( id ).getParentFile();
    String zarrPath = zarrFolder.getAbsolutePath();
    String zarrRootPath = zarrPath.substring(0, zarrPath.indexOf(".zarr") + 5);
    String name = zarrRootPath.substring(zarrRootPath.lastIndexOf(File.separator)+1, zarrRootPath.length() - 5);
    Location omeMetaFile = new Location( zarrRootPath, name+".ome.xml" );
    
    initializeZarrService(zarrPath);
    
    /*
     * Open OME metadata file
     * TODO: Old code to either be reworked with writer or removed entirely
     */
    if (omeMetaFile.exists()) {
      Document omeDocument = null;
      try (RandomAccessInputStream measurement =
          new RandomAccessInputStream(omeMetaFile.getAbsolutePath())) {
        try {
          omeDocument = XMLTools.parseDOM(measurement);
        }
        catch (ParserConfigurationException e) {
          throw new IOException(e);
        }
        catch (SAXException e) {
          throw new IOException(e);
        }
      }
      omeDocument.getDocumentElement().normalize();
      
      OMEXMLService service = null;
      String xml = null;
      try
      {
        xml = XMLTools.getXML( omeDocument );
      }
      catch (TransformerException e2 )
      {
        LOGGER.debug( "", e2 );
      }
      OMEXMLMetadata omexmlMeta = null;
      try
      {
        service = new ServiceFactory().getInstance( OMEXMLService.class );
        omexmlMeta = service.createOMEXMLMetadata( xml );
      }
      catch (DependencyException | ServiceException | NullPointerException e1 )
      {
        LOGGER.debug( "", e1 );
      }
  
      int numDatasets = omexmlMeta.getImageCount();
  
      int oldSeries = getSeries();
      core.clear();
      for (int i=0; i<numDatasets; i++) {
        CoreMetadata ms = new CoreMetadata();
        core.add(ms);
  
        setSeries(i);
  
        Integer w = omexmlMeta.getPixelsSizeX(i).getValue();
        Integer h = omexmlMeta.getPixelsSizeY(i).getValue();
        Integer t = omexmlMeta.getPixelsSizeT(i).getValue();
        Integer z = omexmlMeta.getPixelsSizeZ(i).getValue();
        Integer c = omexmlMeta.getPixelsSizeC(i).getValue();
        if (w == null || h == null || t == null || z == null | c == null) {
          throw new FormatException("Image dimensions not found");
        }
  
        Boolean endian = null;
        String pixType = omexmlMeta.getPixelsType(i).toString();
        ms.dimensionOrder = omexmlMeta.getPixelsDimensionOrder(i).toString();
        ms.sizeX = w.intValue();
        ms.sizeY = h.intValue();
        ms.sizeT = t.intValue();
        ms.sizeZ = z.intValue();
        ms.sizeC = c.intValue();
        ms.imageCount = getSizeZ() * getSizeC() * getSizeT();
        ms.littleEndian = endian == null ? false : !endian.booleanValue();
        ms.rgb = false;
        ms.interleaved = false;
        ms.indexed = false;
        ms.falseColor = true;
        ms.pixelType = FormatTools.pixelTypeFromString(pixType);
        ms.orderCertain = true;
        if (omexmlMeta.getPixelsSignificantBits(i) != null) {
          ms.bitsPerPixel = omexmlMeta.getPixelsSignificantBits(i).getValue();
        }
      }
      setSeries(oldSeries);
      MetadataConverter.convertMetadata( omexmlMeta, store );
    }
    else {

      // Parse base level attributes
      Map<String, Object> attr = zarrService.getGroupAttr(zarrRootPath);
      int attrIndex = 0;
      if (attr != null && !attr.isEmpty()) {
        parseResolutionCount(zarrRootPath, "");
        String jsonAttr = ZarrUtils.toJson(attr, true);
        store.setXMLAnnotationValue(jsonAttr, attrIndex);
        String xml_id = MetadataTools.createLSID("AttributesAnnotation:", attrIndex); 
        store.setXMLAnnotationID(xml_id, attrIndex);
      }
      
      // Parse group attributes
      for (String key: zarrService.getGroupKeys(zarrRootPath)) {
        Map<String, Object> attributes = zarrService.getGroupAttr(zarrRootPath+File.separator+key);
        if (attributes != null && !attributes.isEmpty()) {
          parseResolutionCount(zarrRootPath, key);
          attrIndex++;
          String jsonAttr = ZarrUtils.toJson(attributes, true);
          store.setXMLAnnotationValue(jsonAttr, attrIndex);
          String xml_id = MetadataTools.createLSID("AttributesAnnotation:"+key, attrIndex); 
          store.setXMLAnnotationID(xml_id, attrIndex);
        }
      }
      
      // Parse array attributes
      for (String key: zarrService.getArrayKeys(zarrRootPath)) {
        Map<String, Object> attributes = zarrService.getArrayAttr(zarrRootPath+File.separator+key);
        if (attributes != null && !attributes.isEmpty()) {
          attrIndex++;
          String jsonAttr = ZarrUtils.toJson(attributes, true);
          store.setXMLAnnotationValue(jsonAttr, attrIndex);
          String xml_id = MetadataTools.createLSID("AttributesAnnotation:"+key, attrIndex); 
          store.setXMLAnnotationID(xml_id, attrIndex);
        }
      }

      arrayPaths = new ArrayList<String>();
      arrayPaths.addAll(zarrService.getArrayKeys(zarrRootPath));
      
      orderArrayPaths(zarrRootPath);
      
      core.clear();
      int resolutionTotal = 0;
      for (int i=0; i<arrayPaths.size(); i++) {
        int resolutionCount = 1;
        if (resCounts.get(zarrRootPath+File.separator+arrayPaths.get(i)) != null) {
          resolutionCount = resCounts.get(zarrRootPath+File.separator+arrayPaths.get(i));
        }
        int resolutionIndex= 0;
        if (resIndexes.get(zarrRootPath+File.separator+arrayPaths.get(i)) != null) {
          resolutionIndex = resIndexes.get(zarrRootPath+File.separator+arrayPaths.get(i));
        }
        
        CoreMetadata ms = new CoreMetadata();
        core.add(ms);
    
        if (hasFlattenedResolutions()) {
          setSeries(i);
        }
        else {
          setSeries(coreIndexToSeries(i));
          setResolution(resolutionIndex);
          if (i == resolutionTotal + resolutionCount - 1) {
            resolutionTotal += resolutionCount;
          }
        }
        
        ms.pixelType = zarrService.getPixelType();
        int[] shape = zarrService.getShape();
        
        ms.sizeX = shape[4];
        ms.sizeY = shape[3];
        ms.sizeT = shape[0];
        ms.sizeZ = shape[2];
        ms.sizeC = shape[1];
        ms.dimensionOrder = "XYCZT";
        ms.imageCount = getSizeZ() * getSizeC() * getSizeT();
        ms.littleEndian = zarrService.isLittleEndian();
        ms.rgb = false;
        ms.interleaved = false;
        ms.resolutionCount = resolutionCount;
      }
    }
    MetadataTools.populatePixels( store, this, true );

  }

  /* @see loci.formats.FormatReader#reopenFile() */
  @Override
  public void reopenFile() throws IOException {
    try {
      initializeZarrService(currentId);
    }
    catch (FormatException e) {
      throw new IOException(e);
    }
  }

  // -- Helper methods --

  private void initializeZarrService(String id) throws IOException, FormatException {
//    try {
//      ServiceFactory factory = new ServiceFactory();
//      zarrService = factory.getInstance(ZarrService.class);
//      zarrService.open(id);
//    } catch (DependencyException e) {
//      throw new MissingLibraryException(ZarrServiceImpl.NO_ZARR_MSG, e);
//    }
    zarrService = new JZarrServiceImpl();
    openZarr();
  }
  
  @Override
  public byte[] openBytes(int no, byte[] buf, int x, int y, int w, int h) throws FormatException, IOException {
    FormatTools.checkPlaneParameters(this, no, buf.length, x, y, w, h);
    int[] coordinates = getZCTCoords(no);
    int [] shape = {1, 1, 1, h, w};
    int [] offsets = {coordinates[2], coordinates[1], coordinates[0], y, x};
    Object image = zarrService.readBytes(shape, offsets);

    boolean little = zarrService.isLittleEndian();
    int bpp = FormatTools.getBytesPerPixel(zarrService.getPixelType());
    if (image instanceof byte[]) {
      buf = (byte []) image;
    }
    else if (image instanceof short[]) {
      short[] data = (short[]) image;
      for (int row = 0; row < h; row++) {
        int base = row * w * bpp;
        for (int i = 0; i < w; i++) {
          DataTools.unpackBytes(data[(row * w) + i + x], buf, base + 2 * i, 2, little);
        }
      }
    }
    else if (image instanceof int[]) {
      int[] data = (int[]) image;
      for (int row = 0; row < h; row++) {
        int base = row * w * bpp;
        for (int i = 0; i < w; i++) {
          DataTools.unpackBytes(data[(row * w) + i + x], buf, base + 4 * i, 4, little);
        }
      }
    }
    else if (image instanceof float[]) {
      float[] data = (float[]) image;
      for (int row = 0; row < h; row++) {
        int base = row * w * bpp;
        for (int i = 0; i < w; i++) {
          int value = Float.floatToIntBits(data[(row * w) + i + x]);
          DataTools.unpackBytes(value, buf, base + 4 * i, 4, little);
        }
      }
    }
    else if (image instanceof double[]) {
      double[] data = (double[]) image;
      for (int row = 0; row < h; row++) {
        int base = row * w * bpp;
        for (int i = 0; i < w; i++) {
          long value = Double.doubleToLongBits(data[(row * w) + i + x]);
          DataTools.unpackBytes(value, buf, base + 8 * i, 8, little);
        }
      }
    }
    return buf;
  }
  
  @Override
  public void setSeries(int no) {
    super.setSeries(no);
    openZarr();
  }
  
  private void openZarr() {
    try {
      if (currentId != null && zarrService != null) {
        String zarrRootPath = currentId.substring(0, currentId.indexOf(".zarr")+5);
        String newZarrPath = zarrRootPath;
        if (arrayPaths != null) {
          newZarrPath += File.separator + arrayPaths.get(series);
          zarrService.open(newZarrPath);
        }
      }
    } catch (IOException | FormatException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  private void orderArrayPaths(String root) {
    for (int i = 0; i < resSeries.size(); i++) {
      for (String arrayPath: resSeries.get(i)) {
        arrayPaths.remove(arrayPath);
      }
      for (String arrayPath: resSeries.get(i)) {
        arrayPaths.add(arrayPath);
      }
    }
  }
  
  private void parseResolutionCount(String root, String key) throws IOException, FormatException {
    String path = key.isEmpty() ? root : root + File.separator + key;
    Map<String, Object> attr = zarrService.getGroupAttr(path);
    ArrayList<Object> multiscales = (ArrayList<Object>) attr.get("multiscales");
    if (multiscales != null) {
      Map<String, Object> datasets = (Map<String, Object>) multiscales.get(0);
      ArrayList<Object> multiscalePaths = (ArrayList<Object>)datasets.get("datasets");
      resSeries.put(resCounts.size(), new ArrayList<String>());
      for (int i = 0; i < multiscalePaths.size(); i++) {
        Map<String, Object> multiScale = (Map<String, Object>) multiscalePaths.get(i);
        String scalePath = (String) multiScale.get("path");
        int numRes = multiscalePaths.size();
        if (i == 0) {
          resCounts.put(path+File.separator+scalePath, numRes);
        }
        resIndexes.put(path+File.separator+scalePath, i);
        ArrayList<String> list = resSeries.get(resCounts.size() - 1);
        list.add(key.isEmpty() ? scalePath : key + File.separator + scalePath);
        resSeries.put(resCounts.size() - 1, list);
      }
    }
  }
  
  /* @see loci.formats.IFormatReader#getUsedFiles(boolean) */
  @Override
  public String[] getUsedFiles(boolean noPixels) {
    FormatTools.assertId(currentId, true, 1);
    String zarrRootPath = currentId.substring(0, currentId.indexOf(".zarr") + 5);
    ArrayList<String> usedFiles = new ArrayList<String>();
    usedFiles.add(zarrRootPath);
    File folder = new File(zarrRootPath);
    Collection<File> libs = FileUtils.listFiles(folder, null, true);
    for (File file : libs) {
      usedFiles.add(file.getAbsolutePath());
  }
    String[] fileArr = new String[usedFiles.size()];
    fileArr = usedFiles.toArray(fileArr);
    return fileArr;
  }

}
