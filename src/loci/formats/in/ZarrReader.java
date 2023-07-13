
package loci.formats.in;

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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.FileVisitOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.bc.zarr.JZarrException;
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
import loci.formats.meta.MetadataStore;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.services.JZarrServiceImpl;
import ome.xml.meta.MetadataConverter;
import ome.xml.meta.MetadataRoot;
import ome.xml.model.MapAnnotation;
import ome.xml.model.OME;
import ome.xml.model.Plate;
import ome.xml.model.Screen;
import ome.xml.model.StructuredAnnotations;
import ome.xml.model.primitives.NonNegativeInteger;
import ome.xml.model.primitives.PositiveInteger;
import loci.formats.services.OMEXMLService;
import loci.formats.services.ZarrService;


public class ZarrReader extends FormatReader {

  protected transient ZarrService zarrService;
  private ArrayList<String> arrayPaths= new ArrayList<String>();
  private HashMap<Integer, ArrayList<String>> resSeries = new HashMap<Integer, ArrayList<String>>();
  private HashMap<String, Integer> resCounts = new HashMap<String, Integer>();
  private HashMap<String, Integer> resIndexes = new HashMap<String, Integer>();
  private String dimensionOrder = "XYZCT";
  private int wellCount = 0;
  private int wellSamplesCount = 0;
  private HashMap<String, ArrayList<String>> pathArrayDimensions = new HashMap<String, ArrayList<String>>();
  private boolean planesPrePopulated = false;
  private boolean hasSPW = false;

  public ZarrReader() {
    super("Zarr", "zarr");
    suffixSufficient = false;
    domains = new String[] {FormatTools.UNKNOWN_DOMAIN};
  }

  /* @see loci.formats.IFormatReader#getRequiredDirectories(String[]) */
  @Override
  public int getRequiredDirectories(String[] files)
    throws FormatException, IOException
  {
    return 1;
  }

  /* @see loci.formats.IFormatReader#isThisType(String, boolean) */
  @Override
  public boolean isThisType(String name, boolean open) {
    Location zarrFolder = new Location(name);
    if (zarrFolder != null && zarrFolder.getAbsolutePath().toLowerCase().indexOf(".zarr") > 0) {
      return true;
    }
    return super.isThisType(name, open);
  }

  /* @see loci.formats.IFormatReader#close() */
  @Override
  public void close() throws IOException {
    arrayPaths.clear();
    resSeries.clear();
    resCounts.clear();
    resIndexes.clear();
    if (zarrService != null) {
      zarrService.close();
    }
    super.close();
  }

  /* @see loci.formats.IFormatReader#getOptimalTileHeight() */
  @Override
  public int getOptimalTileHeight() {
    FormatTools.assertId(currentId, true, 1);
    int[] chunkSizes = zarrService.getChunkSize();
    return chunkSizes[chunkSizes.length - 2];
  }

  /* @see loci.formats.IFormatReader#getOptimalTileHeight() */
  @Override
  public int getOptimalTileWidth() {
    FormatTools.assertId(currentId, true, 1);
    int[] chunkSizes = zarrService.getChunkSize();
    return chunkSizes[chunkSizes.length - 1];
  }

  /* @see loci.formats.FormatReader#initFile(String) */
  @Override
  protected void initFile(String id) throws FormatException, IOException {
    super.initFile(id);
    final MetadataStore store = makeFilterMetadata();
    Location zarrFolder = new Location(id);
    String zarrPath = zarrFolder.getAbsolutePath();
    String zarrRootPath = zarrPath.substring(0, zarrPath.indexOf(".zarr") + 5);
    String name = zarrRootPath.substring(zarrRootPath.lastIndexOf(File.separator)+1, zarrRootPath.length() - 5);
    Location omeMetaFile = new Location( zarrRootPath + File.separator + "OME", "METADATA.ome.xml" );
    String canonicalPath = new Location(zarrRootPath).getCanonicalPath();

    initializeZarrService(canonicalPath);

    ArrayList<String> omeSeriesOrder = new ArrayList<String>();
    if(omeMetaFile.exists()) {
      parseOMEXML(omeMetaFile, store, omeSeriesOrder);
    }
    // Parse base level attributes
    Map<String, Object> attr = zarrService.getGroupAttr(canonicalPath);
    int attrIndex = 0;
    if (attr != null && !attr.isEmpty()) {
      parseResolutionCount(zarrRootPath, "");
      parseOmeroMetadata(zarrRootPath, "");
      String jsonAttr;
      try {
        jsonAttr = ZarrUtils.toJson(attr, true);
        store.setXMLAnnotationValue(jsonAttr, attrIndex);
        String xml_id = MetadataTools.createLSID("Annotation", attrIndex);
        store.setXMLAnnotationID(xml_id, attrIndex);
      } catch (JZarrException e) {
        LOGGER.warn("Failed to convert attributes to JSON");
        e.printStackTrace();
      }
    }

    // Parse group attributes
    Set<String> groupKeys = zarrService.getGroupKeys(canonicalPath);
    List<String> orderedGroupKeys = reorderGroupKeys(groupKeys, omeSeriesOrder);
    for (String key: orderedGroupKeys) {
      Map<String, Object> attributes = zarrService.getGroupAttr(canonicalPath+File.separator+key);
      if (attributes != null && !attributes.isEmpty()) {
        parseResolutionCount(zarrRootPath, key);
        parseLabels(zarrRootPath, key);
        parseImageLabels(zarrRootPath, key);
        attrIndex++;
        String jsonAttr;
        try {
          jsonAttr = ZarrUtils.toJson(attributes, true);
          store.setXMLAnnotationValue(jsonAttr, attrIndex);
          String xml_id = MetadataTools.createLSID("Annotation", attrIndex);
          store.setXMLAnnotationID(xml_id, attrIndex);
        } catch (JZarrException e) {
          LOGGER.warn("Failed to convert attributes to JSON");
          e.printStackTrace();
        }
      }
    }

    // Parse array attributes
    for (String key: zarrService.getArrayKeys(canonicalPath)) {
      Map<String, Object> attributes = zarrService.getArrayAttr(canonicalPath+File.separator+key);
      if (attributes != null && !attributes.isEmpty()) {
        attrIndex++;
        String jsonAttr;
        try {
          jsonAttr = ZarrUtils.toJson(attributes, true);
          store.setXMLAnnotationValue(jsonAttr, attrIndex);
          String xml_id = MetadataTools.createLSID("Annotation", attrIndex);
          store.setXMLAnnotationID(xml_id, attrIndex);
        } catch (JZarrException e) {
          LOGGER.warn("Failed to convert attributes to JSON");
          e.printStackTrace();
        }
      }
    }

    arrayPaths = new ArrayList<String>();
    arrayPaths.addAll(zarrService.getArrayKeys(canonicalPath));
    orderArrayPaths(zarrRootPath);

    core.clear();
    int resolutionTotal = 0;
    for (int i=0; i<arrayPaths.size(); i++) {
      int resolutionCount = 1;
      if (resCounts.get(arrayPaths.get(i)) != null) {
        resolutionCount = resCounts.get(arrayPaths.get(i));
      }
      int resolutionIndex= 0;
      if (resIndexes.get(arrayPaths.get(i)) != null) {
        resolutionIndex = resIndexes.get(arrayPaths.get(i));
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
      if (shape.length < 5) {
        shape = get5DShape(shape);
      }

      ms.sizeX = shape[4];
      ms.sizeY = shape[3];
      ms.sizeT = shape[0];
      ms.sizeZ = shape[2];
      ms.sizeC = shape[1];
      ArrayList<String> pathDimensions = pathArrayDimensions.get(arrayPaths.get(i));
      if (pathDimensions != null && !pathDimensions.isEmpty()) {
        ms.sizeX = shape[pathDimensions.indexOf("x")];
        ms.sizeY = shape[pathDimensions.indexOf("y")];
        ms.sizeT = shape[pathDimensions.indexOf("t")];
        ms.sizeZ = shape[pathDimensions.indexOf("z")];
        ms.sizeC = shape[pathDimensions.indexOf("c")];
        String newDimOrder = "";
        for (int d = 1; d < pathDimensions.size() + 1; d++) {
          newDimOrder += pathDimensions.get(pathDimensions.size() - d).toUpperCase();
        }
        dimensionOrder = newDimOrder;
      }
      ms.dimensionOrder = dimensionOrder;
      ms.imageCount = getSizeZ() * getSizeC() * getSizeT();
      ms.littleEndian = zarrService.isLittleEndian();
      ms.rgb = false;
      ms.interleaved = false;
      ms.resolutionCount = resolutionCount;
    }

    MetadataTools.populatePixels( store, this, !planesPrePopulated );
    for (int i = 0; i < getSeriesCount(); i++) {
      store.setImageName(arrayPaths.get(seriesToCoreIndex(i)), i);
      store.setImageID(MetadataTools.createLSID("Image", i), i);
    }
    parsePlate(zarrRootPath, "", store);

    setSeries(0);
  }

  private List<String> reorderGroupKeys(Set<String> groupKeys, List<String> originalKeys) {
    // Reorder group keys to maintain the original order from the OME-XML provided by bioformats2raw
    if (originalKeys.isEmpty() || !groupKeys.containsAll(originalKeys)) {
      LOGGER.warn("Mismatch with group key paths and original OME-XML metadata, original ordering wont be maintained");
      return reorderGroupKeys(groupKeys);
    }
    List<String> groupKeysList = new ArrayList<String>();
    groupKeys.removeAll(originalKeys);
    groupKeysList.addAll(originalKeys);
    groupKeysList.addAll(groupKeys);
    return groupKeysList;
  }

  private List<String> reorderGroupKeys(Set<String> groupKeys) {
    // Reorder group keys to avoid order such A/1, A/10, A/11, A/12, A/2, A/20, A/3, A/4 
    List<String> groupKeysList = new ArrayList<String>();
    groupKeysList.addAll(groupKeys);
    Collections.sort(groupKeysList, keyComparator);
    return groupKeysList;
  }
  
  private static Comparator<String> keyComparator = (a,b)->{
    String[] aParts = a.split("/");
    String[] bParts = b.split("/");
    
    int numParts = aParts.length - bParts.length;
    if (numParts != 0) return numParts;
    
    for (int i = 0; i < aParts.length; i++) {
      String aPart = aParts[i];
      String bPart = bParts[i];
      
      boolean isAInt = isInteger(aPart);
      boolean isBInt = isInteger(bPart);
      if (isAInt && !isBInt) return -1;
      if (!isAInt && isBInt) return 1;
      
      if (isAInt) {
        int numResult = Integer.compare(Integer.valueOf(aPart), Integer.valueOf(bPart));
        if (numResult != 0) return numResult;
      }
      else {
        int stringResult = aPart.compareTo(bPart);
        if (stringResult != 0) return stringResult;
      }
    }
    
    return 0;
  };
  
  private static boolean isInteger(String s) {
    if(s.isEmpty()) return false;
    for(int i = 0; i < s.length(); i++) {
      if(i == 0 && s.charAt(i) == '-') {
        if(s.length() == 1) return false;
        else continue;
      }
      if(Character.digit(s.charAt(i), 10) < 0) return false;
    }
    return true;
  }

  /**
   * In the event that .zarray does not contain a 5d shape
   * The dimensions of the original shape will be assumed based on tczyx
   * @param originalShape as found in .zarray
   * @return a 5D shape to be used within the reader
   */
  private int[] get5DShape(int [] originalShape) {
    int [] shape = new int[] {1,1,1,1,1};
    int shapeIndex = 4;
    for (int s = originalShape.length - 1; s >= 0; s--) {
      shape[shapeIndex] = originalShape[s];
      shapeIndex --;
    }
    return shape;
  }

  /**
   * In the event that .zarray does not contain a 5d shape
   * The 5D shape will be reduced to match the original representation
   * @param shape5D - the 5D representation used within this reader
   * @param size of the shape required by jzarr
   * @return a 5D shape to be used within the reader
   */
  private int[] getOriginalShape(int [] shape5D, int size) {
    int [] shape = new int[size];
    int shape5DIndex = 4;
    for (int s = shape.length - 1; s >= 0; s--) {
      shape[s] = shape5D[shape5DIndex];
      shape5DIndex --;
    }
    return shape;
  }

  /* @see loci.formats.FormatReader#reopenFile() */
  @Override
  public void reopenFile() throws IOException {
    try {
      String canonicalPath = new Location(currentId).getCanonicalPath();
      initializeZarrService(canonicalPath);
    }
    catch (FormatException e) {
      throw new IOException(e);
    }
  }

  protected void initializeZarrService(String rootPath) throws IOException, FormatException {
    zarrService = new JZarrServiceImpl(rootPath);
    openZarr();
  }

  @Override
  public byte[] openBytes(int no, byte[] buf, int x, int y, int w, int h) throws FormatException, IOException {
    FormatTools.checkPlaneParameters(this, no, buf.length, x, y, w, h);
    int[] coordinates = getZCTCoords(no);
    int [] shape = {1, 1, 1, h, w};
    int zarrArrayShapeSize = zarrService.getShape().length;
    if (zarrArrayShapeSize < 5) {
      shape = getOriginalShape(shape, zarrArrayShapeSize);
    }
    int zIndex = 4 - dimensionOrder.indexOf("Z");
    int cIndex = 4 - dimensionOrder.indexOf("C");
    int tIndex = 4 - dimensionOrder.indexOf("T");
    int [] offsets = {1, 1, 1, y, x};
    offsets[zIndex] = coordinates[0];
    offsets[cIndex] = coordinates[1];
    offsets[tIndex] = coordinates[2];
    if (zarrArrayShapeSize < 5) {
      offsets = getOriginalShape(offsets, zarrArrayShapeSize);
    }
    Object image = zarrService.readBytes(shape, offsets);

    boolean little = zarrService.isLittleEndian();
    int bpp = FormatTools.getBytesPerPixel(zarrService.getPixelType());
    if (image instanceof byte[]) {
      byte [] data = (byte []) image;
      for (int i = 0; i < data.length; i++) {
        DataTools.unpackBytes(data[i], buf, i, 1, little);
      }
    }
    else if (image instanceof short[]) {
      short[] data = (short[]) image;
      for (int row = 0; row < h; row++) {
        int base = row * w * bpp;
        for (int i = 0; i < w; i++) {
          DataTools.unpackBytes(data[(row * w) + i], buf, base + 2 * i, 2, little);
        }
      }
    }
    else if (image instanceof int[]) {
      int[] data = (int[]) image;
      for (int row = 0; row < h; row++) {
        int base = row * w * bpp;
        for (int i = 0; i < w; i++) {
          DataTools.unpackBytes(data[(row * w) + i], buf, base + 4 * i, 4, little);
        }
      }
    }
    else if (image instanceof float[]) {
      float[] data = (float[]) image;
      for (int row = 0; row < h; row++) {
        int base = row * w * bpp;
        for (int i = 0; i < w; i++) {
          int value = Float.floatToIntBits(data[(row * w) + i]);
          DataTools.unpackBytes(value, buf, base + 4 * i, 4, little);
        }
      }
    }
    else if (image instanceof double[]) {
      double[] data = (double[]) image;
      for (int row = 0; row < h; row++) {
        int base = row * w * bpp;
        for (int i = 0; i < w; i++) {
          long value = Double.doubleToLongBits(data[(row * w) + i]);
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

  @Override
  public void setResolution(int no) {
    super.setResolution(no);
    openZarr();
  }

  private void openZarr() {
    try {
      if (currentId != null && zarrService != null) {
        String zarrRootPath = currentId.substring(0, currentId.indexOf(".zarr")+5);
        String newZarrPath = zarrRootPath;
        if (arrayPaths != null && !arrayPaths.isEmpty()) {
          int seriesIndex = seriesToCoreIndex(series);
          if (!hasFlattenedResolutions()) {
            seriesIndex += resolution;
          }
          newZarrPath += File.separator + arrayPaths.get(seriesIndex);
          String canonicalPath = new Location(newZarrPath).getCanonicalPath();
          zarrService.open(canonicalPath);
        }
      }
    } catch (IOException | FormatException e) {
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
    String canonicalPath = new Location(path).getCanonicalPath();
    Map<String, Object> attr = zarrService.getGroupAttr(canonicalPath);
    ArrayList<Object> multiscales = (ArrayList<Object>) attr.get("multiscales");
    if (multiscales != null) {
      for (int x = 0; x < multiscales.size(); x++) {
        Map<String, Object> datasets = (Map<String, Object>) multiscales.get(x);
        List<Object> multiscaleAxes = (List<Object>)datasets.get("axes");
        ArrayList<String> pathDimensions = new ArrayList<String> ();
        if (multiscaleAxes != null) {
          for (int i = 0; i < multiscaleAxes.size(); i++) {
            if (multiscaleAxes.get(i) instanceof String) {
              String axis = (String) multiscaleAxes.get(i);
              addGlobalMeta(MetadataTools.createLSID("Axis", x, i), axis);
              pathDimensions.add(axis.toLowerCase());
            }
            else if (multiscaleAxes.get(i) instanceof HashMap) {
              HashMap<String, String> axis = (HashMap<String, String>) multiscaleAxes.get(i);
              String type = axis.get("type");
              addGlobalMeta(MetadataTools.createLSID("Axis type", x, i), type);
              String name = axis.get("name");
              addGlobalMeta(MetadataTools.createLSID("Axis name", x, i), name);
              String units = axis.get("units");
              addGlobalMeta(MetadataTools.createLSID("Axis units", x, i), units);
              pathDimensions.add(name.toLowerCase());
            }
          }
          if (pathDimensions.size() < 5) {
            // Fill missing dimensions
            if (!pathDimensions.contains("x")) pathDimensions.add(0, "x");
            if (!pathDimensions.contains("y")) pathDimensions.add(0, "y");
            if (!pathDimensions.contains("c")) pathDimensions.add(0, "c");
            if (!pathDimensions.contains("t")) pathDimensions.add(0, "t");
            if (!pathDimensions.contains("z")) pathDimensions.add(0, "z");
          }
        }
        ArrayList<Object> multiscalePaths = (ArrayList<Object>)datasets.get("datasets");
        resSeries.put(resCounts.size(), new ArrayList<String>());
        for (int i = 0; i < multiscalePaths.size(); i++) {
          Map<String, Object> multiScale = (Map<String, Object>) multiscalePaths.get(i);
          String scalePath = (String) multiScale.get("path");
          int numRes = multiscalePaths.size();
          if (i == 0) {
            resCounts.put(key.isEmpty() ? scalePath : key + File.separator + scalePath, numRes);
          }
          resIndexes.put(key.isEmpty() ? scalePath : key + File.separator + scalePath, i);
          ArrayList<String> list = resSeries.get(resCounts.size() - 1);
          list.add(key.isEmpty() ? scalePath : key + File.separator + scalePath);
          resSeries.put(resCounts.size() - 1, list);
          pathArrayDimensions.put(key.isEmpty() ? scalePath : key + File.separator + scalePath, pathDimensions);
        }
        List<Object> coordinateTransformations = (List<Object>)datasets.get("coordinateTransformations");
        if (coordinateTransformations != null) {
          for (int i = 0; i < coordinateTransformations.size(); i++) {
              HashMap<String, Object> transformation = (HashMap<String, Object>) coordinateTransformations.get(i);
              String type = (String)transformation.get("type");
              addGlobalMeta(MetadataTools.createLSID("Coordinate Transformation type", x, i), type);
              ArrayList<Object> scale = (ArrayList<Object>)transformation.get("scale");
              if (scale != null)addGlobalMeta(MetadataTools.createLSID("Coordinate Transformation scale", x, i), scale);
              ArrayList<Object> translation = (ArrayList<Object>)transformation.get("translation");
              if (translation != null)addGlobalMeta(MetadataTools.createLSID("Coordinate Transformation translation", x, i), translation);
          }
        }
      }
    }
  }

  private void parsePlate(String root, String key, MetadataStore store) throws IOException, FormatException {
    String path = key.isEmpty() ? root : root + File.separator + key;
    String canonicalPath = new Location(path).getCanonicalPath();
    Map<String, Object> attr = zarrService.getGroupAttr(canonicalPath);
    Map<Object, Object> plates = (Map<Object, Object>) attr.get("plate");
    if (plates != null) {
      ArrayList<Object> columns = (ArrayList<Object>)plates.get("columns");
      ArrayList<Object> rows = (ArrayList<Object>)plates.get("rows");
      ArrayList<Object> wells = (ArrayList<Object>)plates.get("wells");
      ArrayList<Object>  acquisitions = (ArrayList<Object> )plates.get("acquisitions");
      String plateName = (String) plates.get("name");
      String fieldCount = (String) plates.get("filed_count");

      String plate_id =  MetadataTools.createLSID("Plate", 0);
      store.setPlateID(plate_id, 0);
      store.setPlateName(plateName, 0);
      HashMap<Integer, Integer> acqIdsIndexMap = new HashMap<Integer, Integer>();
      if (acquisitions != null) {
        for (int a = 0; a < acquisitions.size(); a++) {
          Map<String, Object> acquistion = (Map<String, Object>) acquisitions.get(a);
          Integer acqId = (Integer) acquistion.get("id");
          String acqName = (String) acquistion.get("name");
          String acqStartTime = (String) acquistion.get("starttime");
          Integer maximumfieldcount = (Integer) acquistion.get("maximumfieldcount");
          acqIdsIndexMap.put(acqId, a);
          store.setPlateAcquisitionID(
              MetadataTools.createLSID("PlateAcquisition", 0, acqId), 0, a);
          if (maximumfieldcount != null) {
            store.setPlateAcquisitionMaximumFieldCount(new PositiveInteger(maximumfieldcount), 0, a);
          }
        }
      }
      
//      TODO: Likely remove as values unused
//      for (int c = 0; c < columns.size(); c++) {
//        Map<String, Object> column = (Map<String, Object>) columns.get(c);
//        String colName = (String) column.get("name");
//      }
//      for (int r = 0; r < rows.size(); r++) {
//        Map<String, Object> row = (Map<String, Object>) rows.get(r);
//        String rowName = (String) row.get("name");
//      }
      
      //Create empty wells for each row and column
      wellCount  = rows.size() * columns.size();
      for (int r = 0; r < rows.size(); r++) {
        for (int c = 0; c < columns.size(); c++) {
          int wellIndex = (r * columns.size()) + c;
          String well_id =  MetadataTools.createLSID("Well", 0, wellIndex);
          store.setWellID(well_id, 0, wellIndex);
          store.setWellRow(new NonNegativeInteger(r), 0, wellIndex);
          store.setWellColumn(new NonNegativeInteger(c), 0, wellIndex);
        }
      }
      for (int w = 0; w < wells.size(); w++) {
        Map<String, Object> well = (Map<String, Object>) wells.get(w);
        String wellPath = (String) well.get("path");

        // column_index & row_index stored as Integer in bioformats2raw 0.3
        Integer wellColIndex = (Integer) well.get("column_index");
        Integer wellRowIndex = (Integer) well.get("row_index");
        if (wellColIndex == null && wellRowIndex == null) {
          // columnIndex & rowIndex stored as Integer in OME-NGFF v0.4
          wellColIndex = (Integer) well.get("columnIndex");
          wellRowIndex = (Integer) well.get("rowIndex");
        }
        if (wellColIndex == null || wellRowIndex == null) {
          // for OME-NGFF v0.2 parse row and column index from the path
          String[] parts = wellPath.split("/");
          String wellRow = parts[parts.length - 2];
          String wellCol = parts[parts.length - 1];
          wellRowIndex = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".indexOf(wellRow.toUpperCase());
          if (wellRowIndex == -1) {
            wellRowIndex = Integer.parseInt(wellRow);
          }
          wellColIndex = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".indexOf(wellCol.toUpperCase());
          if (wellColIndex == -1) {
            wellColIndex = Integer.parseInt(wellCol);
          }
        }
        int wellIndex = (wellRowIndex * columns.size()) + wellColIndex;
        store.setWellExternalIdentifier(wellPath, 0, wellIndex);
        parseWells(root, wellPath, store, 0, wellIndex, acqIdsIndexMap);
      }
    }
  }

  private void parseWells(String root, String key, MetadataStore store, int plateIndex, int wellIndex,
      HashMap<Integer, Integer> acqIdsIndexMap) throws IOException, FormatException {
    String path = key.isEmpty() ? root : root + File.separator + key;
    String canonicalPath = new Location(path).getCanonicalPath();
    Map<String, Object> attr = zarrService.getGroupAttr(canonicalPath);
    Map<Object, Object> wells = (Map<Object, Object>) attr.get("well");
    if (wells != null) {
      ArrayList<Object> images = (ArrayList<Object>)wells.get("images");
      for (int i = 0; i < images.size(); i++) {
        Map<String, Object> image = (Map<String, Object>) images.get(i);
        String imagePath = (String) image.get("path");
        Integer acquisition = (Integer) image.get("acquisition");
        if (acqIdsIndexMap.containsKey(acquisition)) {
          acquisition = acqIdsIndexMap.get(acquisition);
        }
        String site_id = MetadataTools.createLSID("WellSample", plateIndex, wellIndex, i);
        store.setWellSampleID(site_id, plateIndex, wellIndex, i);
        store.setWellSampleIndex(new NonNegativeInteger(i), plateIndex, wellIndex, i);
        String imageRefPath = "" + i;
        if (key != null && !key.isEmpty()) {
          imageRefPath = key + File.separator + i;
        }
        if (resCounts.containsKey(imageRefPath + File.separator + "0")) {
          imageRefPath += File.separator + "0";
        }
        String imageID = MetadataTools.createLSID("Image", coreIndexToSeries(arrayPaths.indexOf(imageRefPath)));
        store.setWellSampleImageRef(imageID, plateIndex, wellIndex, i);
        if (acquisition != null && acquisition >= 0) {
          store.setPlateAcquisitionWellSampleRef(site_id, plateIndex, (int) acquisition, i);
        }
        wellSamplesCount++;
      }
    }
  }

  private void parseLabels(String root, String key) throws IOException, FormatException {
    String path = key.isEmpty() ? root : root + File.separator + key;
    String canonicalPath = new Location(path).getCanonicalPath();
    Map<String, Object> attr = zarrService.getGroupAttr(canonicalPath);
    ArrayList<Object> labels = (ArrayList<Object>) attr.get("labels");
    if (labels != null) {
      for (int l = 0; l < labels.size(); l++) {
        String label = (String) labels.get(l);
      }
    }
  }

  private void parseImageLabels(String root, String key) throws IOException, FormatException {
    String path = key.isEmpty() ? root : root + File.separator + key;
    String canonicalPath = new Location(path).getCanonicalPath();
    Map<String, Object> attr = zarrService.getGroupAttr(canonicalPath);
    Map<String, Object> imageLabel = (Map<String, Object>) attr.get("image-label");
    if (imageLabel != null) {
      String version = (String) imageLabel.get("version");
      ArrayList<Object> colors = (ArrayList<Object>)imageLabel.get("colors");
      if (colors == null) {
        colors = (ArrayList<Object>)imageLabel.get("color");
      }
      if (colors != null) {
        for (int c = 0; c < colors.size(); c++) {
          Map<String, Object> color = (Map<String, Object>) colors.get(c);
          Integer labelValue = (Integer) color.get("label-value");
          ArrayList<Object> rgba = (ArrayList<Object>) color.get("rgba");
        }
      }
      ArrayList<Object> properties = (ArrayList<Object>)imageLabel.get("properties");
      if (properties != null) {
        for (int p = 0; p < properties.size(); p++) {
          Map<String, Object> prop = (Map<String, Object>) properties.get(p);
          Integer labelValue = (Integer) prop.get("label-value");
          Double area = (Double) prop.get("area (pixels)");
          String propClass = (String) prop.get("class");
        }
      }
    }
    ArrayList<Object> sources = (ArrayList<Object>) attr.get("source");
    if (sources != null) {
      for (int s = 0; s < sources.size(); s++) {
        Map<String, Object> source = (Map<String, Object>) sources.get(s);
        ArrayList<Object> imagePaths = (ArrayList<Object>)source.get("image");
        for (int p = 0; p < imagePaths.size(); p++) {
          String imagePath = (String) imagePaths.get(p);
        }
      }
    }
  }

  private void parseOmeroMetadata(String root, String key) throws IOException, FormatException {
    String path = key.isEmpty() ? root : root + File.separator + key;
    String canonicalPath = new Location(path).getCanonicalPath();
    Map<String, Object> attr = zarrService.getGroupAttr(canonicalPath);
    Map<String, Object> omeroMetadata = (Map<String, Object>) attr.get("omero");
    if (omeroMetadata != null) {
      Integer id = (Integer) omeroMetadata.get("id");
      String name = (String) omeroMetadata.get("name");
      String version = (String) omeroMetadata.get("version");
      ArrayList<Object> channels = (ArrayList<Object>)omeroMetadata.get("channels");
      for (int i = 0; i < channels.size(); i++) {
        Map<String, Object> channel = (Map<String, Object>) channels.get(i);
        Boolean channelActive = (Boolean) channel.get("active");
        Double channelCoefficient = (Double) channel.get("coefficient");
        String channelColor = (String) channel.get("color");
        String channelFamily = (String) channel.get("family");
        Boolean channelInverted = (Boolean) channel.get("inverted");
        String channelLabel = (String) channel.get("label");
        Map<String, Object> window = (Map<String, Object>)channel.get("window");
        if (window != null) {
          Double windowStart = getDouble(window, "start");
          Double windowEnd = getDouble(window, "end");
          Double windowMin = getDouble(window, "min");
          Double windowMax = getDouble(window, "max");
        }
      }
      Map<String, Object> rdefs = (Map<String, Object>)omeroMetadata.get("rdefs");
      if (rdefs != null) {
        Integer defaultT = (Integer) rdefs.get("defaultT");
        Integer defaultZ = (Integer) rdefs.get("defaultZ");
        String model = (String) rdefs.get("model");
      }
    }
  }

  private void parseOMEXML(Location omeMetaFile, MetadataStore store, ArrayList<String> origSeries) throws IOException, FormatException {
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
      Hashtable originalMetadata = service.getOriginalMetadata(omexmlMeta);
      if (originalMetadata != null) metadata = originalMetadata;
      planesPrePopulated = true;
    }
    catch (DependencyException | ServiceException | NullPointerException e1 )
    {
      LOGGER.debug( "", e1 );
    }

    int numDatasets = omexmlMeta.getImageCount();

    // Map of the well location for each imageReference
    // Later we will map the series index to the imageReference
    // This allows us to maintain the series order when parsing the Zarr groups
    Map<String, String> imageRefPaths = new HashMap<String, String>();
    for (int plateIndex = 0; plateIndex < omexmlMeta.getPlateCount(); plateIndex++) { 
      for (int wellIndex = 0; wellIndex < omexmlMeta.getWellCount(plateIndex); wellIndex++) { 
        NonNegativeInteger col = omexmlMeta.getWellColumn(plateIndex, wellIndex);
        NonNegativeInteger row = omexmlMeta.getWellRow(plateIndex, wellIndex);
        
        String rowLetter = getRowString(row.getValue());
        String expectedPath = rowLetter + File.separator + (col.getValue() + 1) + File.separator  + "0";
        for (int wellSampleIndex = 0; wellSampleIndex < omexmlMeta.getWellSampleCount(plateIndex, wellIndex); wellSampleIndex++) { 
          String imageRef = omexmlMeta.getWellSampleImageRef(plateIndex, wellIndex, wellSampleIndex);
          imageRefPaths.put(imageRef, expectedPath);
        }
      }
    }
    
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

      String imageId = omexmlMeta.getImageID(i);
      if (!imageRefPaths.isEmpty() && imageRefPaths.containsKey(imageId)) {
        String expectedZarrPath = imageRefPaths.get(imageId);
        origSeries.add(expectedZarrPath);
      }

      Boolean endian = zarrService.isLittleEndian();
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

    // Remove old PyramidResolution annotations
    OME root = (OME) omexmlMeta.getRoot();
    StructuredAnnotations annotations = root.getStructuredAnnotations();
    if (annotations != null) {
      int numMapAnnotations = annotations.sizeOfMapAnnotationList();
      int index = 0;
      for (int i = 0; i < numMapAnnotations; i++) {
        MapAnnotation mapAnnotation = annotations.getMapAnnotation(index);
        String namespace = mapAnnotation.getNamespace();
        if (namespace != null && namespace.toLowerCase().contains("pyramidresolution")) {
          annotations.removeMapAnnotation(mapAnnotation);
        }
        else {
          index++;
        }
      }
      root.setStructuredAnnotations(annotations);
      omexmlMeta.setRoot((MetadataRoot) root);
    }
    
    // Remove old Screen and Plate metadata
    int screenSize = root.sizeOfScreenList();
    for (int i = 0; i < screenSize; i++) {
      root.removeScreen(root.getScreen(i));
    }
    
    int plateSize = root.sizeOfPlateList();
    for (int i = 0; i < plateSize; i++) {
      root.removePlate(root.getPlate(i));
    }
    
    omexmlMeta.setRoot((MetadataRoot) root);
 
    MetadataConverter.convertMetadata( omexmlMeta, store );
  }

  public static String getRowString(int rowIndex) {
    StringBuilder sb = new StringBuilder();
    if (rowIndex == 0) sb.append('A');
    while (rowIndex > 0) {
        sb.append((char)('A' + (rowIndex % 26)));
        rowIndex /= 26;
    }
    return sb.reverse().toString();
  }

  private Double getDouble(Map<String, Object> src, String key) {
    Number val = (Number) src.get(key);
    if (val == null) {
      return null;
    }
    return val.doubleValue();
  }

  /* @see loci.formats.IFormatReader#getUsedFiles(boolean) */
  @Override
  public String[] getUsedFiles(boolean noPixels) {
    FormatTools.assertId(currentId, true, 1);
    String zarrRootPath = currentId.substring(0, currentId.indexOf(".zarr") + 5);
    ArrayList<String> usedFiles = new ArrayList<String>();
    try (Stream<Path> paths = Files.walk(Paths.get(zarrRootPath), FileVisitOption.FOLLOW_LINKS)) {
      paths.filter(Files::isRegularFile)
              .forEach(path -> usedFiles.add(path.toFile().getAbsolutePath()));
    } catch (IOException e) {
      e.printStackTrace();
    }

    String[] fileArr = new String[usedFiles.size()];
    fileArr = usedFiles.toArray(fileArr);
    return fileArr;
  }

  /* @see loci.formats.SubResolutionFormatReader#getDomains() */
  @Override
  public String[] getDomains() {
    FormatTools.assertId(currentId, true, 1);
    return hasSPW ? new String[] {FormatTools.HCS_DOMAIN} :
      FormatTools.NON_SPECIAL_DOMAINS;
  }

}
