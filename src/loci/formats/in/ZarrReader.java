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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
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
import ome.xml.model.StructuredAnnotations;
import ome.xml.model.primitives.NonNegativeInteger;
import ome.xml.model.primitives.PositiveInteger;
import ome.xml.model.primitives.Timestamp;
import loci.formats.services.OMEXMLService;
import loci.formats.services.ZarrService;


public class ZarrReader extends FormatReader {

  public static final String QUICK_READ_KEY = "omezarr.quick_read";
  public static final boolean QUICK_READ_DEFAULT = false;
  public static final String SAVE_ANNOTATIONS_KEY = "omezarr.save_annotations";
  public static final boolean SAVE_ANNOTATIONS_DEFAULT = false;
  public static final String LIST_PIXELS_KEY = "omezarr.list_pixels";
  public static final boolean LIST_PIXELS_DEFAULT = true;
  public static final String LIST_PIXELS_ENV_KEY = "OME_ZARR_LIST_PIXELS";
  public static final String INCLUDE_LABELS_KEY = "omezarr.include_labels";
  public static final boolean INCLUDE_LABELS_DEFAULT = false;
  public static final String ALT_STORE_KEY = "omezarr.alt_store";
  public static final String ALT_STORE_DEFAULT = null;
  protected transient ZarrService zarrService;
  private ArrayList<String> arrayPaths = new ArrayList<String>();
  
  // The below fields are only required for initialization and are not required to be serialized
  private transient ArrayList<String> groupKeys = new ArrayList<String>(); 
  private transient HashMap<Integer, ArrayList<String>> resSeries = new HashMap<Integer, ArrayList<String>>(); // can be removed
  private transient HashMap<String, Integer> resCounts = new HashMap<String, Integer>(); // can be removed
  private transient HashSet<Integer> uniqueResCounts = new HashSet<Integer>(); // can be removed
  private transient HashMap<String, Integer> resIndexes = new HashMap<String, Integer>(); // can be removed
  private transient HashMap<String, ArrayList<String>> pathArrayDimensions = new HashMap<String, ArrayList<String>>(); // can be removed
  
  private String dimensionOrder = "XYZCT";
  private int wellCount = 0;
  private int wellSamplesCount = 0;
  private boolean planesPrePopulated = false;
  private boolean hasSPW = false;
  private transient int currentOpenZarr = -1;

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
    groupKeys.clear();
    resSeries.clear();
    resCounts.clear();
    uniqueResCounts.clear();
    resIndexes.clear();
    pathArrayDimensions.clear();
    if (zarrService != null) {
      zarrService.close();
    }
    planesPrePopulated = false;
    hasSPW = false;
    currentOpenZarr = -1;
    wellCount = 0;
    wellSamplesCount = 0;
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
    LOGGER.debug("ZarrReader attempting to initialize file: {}", id);
    final MetadataStore store = makeFilterMetadata();
    Location zarrFolder = new Location(id);
    String zarrPath = zarrFolder.getAbsolutePath();
    String zarrRootPath = zarrPath.substring(0, zarrPath.indexOf(".zarr") + 5);
    String name = zarrRootPath.substring(zarrRootPath.lastIndexOf(File.separator)+1, zarrRootPath.length() - 5);
    Location omeMetaFile = new Location( zarrRootPath + File.separator + "OME", "METADATA.ome.xml" );
    String canonicalPath = new Location(zarrRootPath).getCanonicalPath();

    initializeZarrService();
    reloadOptionsFile(zarrRootPath);

    ArrayList<String> omeSeriesOrder = new ArrayList<String>();
    if(omeMetaFile.exists()) {
      LOGGER.debug("ZarrReader parsing existing OME-XML");
      parseOMEXML(omeMetaFile, store, omeSeriesOrder);
    }
    // Parse base level attributes
    Map<String, Object> attr = zarrService.getGroupAttr(canonicalPath);
    int attrIndex = 0;
    if (attr != null && !attr.isEmpty()) {
      parseResolutionCount(zarrRootPath, "", attr);
      parseOmeroMetadata(attr);
      if (saveAnnotations()) {
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
    }
    generateGroupKeys(attr, canonicalPath);

    // Parse group attributes
    if (groupKeys.isEmpty()) {
      LOGGER.debug("ZarrReader adding group keys from ZarrService");
      groupKeys.addAll(zarrService.getGroupKeys(canonicalPath));
    }

    List<String> orderedGroupKeys = reorderGroupKeys(groupKeys, omeSeriesOrder);
    for (String key: orderedGroupKeys) {
      Map<String, Object> attributes = zarrService.getGroupAttr(canonicalPath+File.separator+key);
      if (attributes != null && !attributes.isEmpty()) {
        parseResolutionCount(zarrRootPath, key, attributes);
        parseLabels(zarrRootPath, attributes);
        parseImageLabels(zarrRootPath, attributes);
        attrIndex++;
        if (saveAnnotations()) {
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
    }

    // Parse array attributes
    generateArrayKeys(attr, canonicalPath);
    if (arrayPaths.isEmpty()) {
      LOGGER.debug("ZarrReader adding Array Keys from ZarrService");
      arrayPaths.addAll(zarrService.getArrayKeys(canonicalPath));
    }
    orderArrayPaths(zarrRootPath);

    if (saveAnnotations()) {
      for (String key: arrayPaths) {
        Map<String, Object> attributes = zarrService.getArrayAttr(zarrRootPath+File.separator+key);
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
    }

    core.clear();
    int resolutionTotal = 0;
    
    HashMap<Integer, int[]> resShapes = new HashMap<Integer, int[]>();
    int pixelType = -1;

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

      boolean openZarr = true;
      if (quickRead() && resShapes.containsKey(resolutionIndex) && !arrayPaths.get(i).toLowerCase().contains("label")) {
        openZarr = false;
      }
        
      if (hasFlattenedResolutions()) {
        setSeries(i, openZarr);
      }
      else {
        setSeries(coreIndexToSeries(i), openZarr);
        setResolution(resolutionIndex, openZarr);
        if (i == resolutionTotal + resolutionCount - 1) {
          resolutionTotal += resolutionCount;
        }
      }
      
      int[] shape;
      if (openZarr) {
        LOGGER.debug("ZarrReader opening Zarr to get Shape");
        pixelType = zarrService.getPixelType();
        ms.pixelType = pixelType;
        shape = zarrService.getShape();
        if (shape.length < 5) {
          shape = get5DShape(shape);
        } 
        resShapes.put(resolutionIndex, shape);
      }
      else {
        ms.pixelType = pixelType;
        shape = resShapes.get(resolutionIndex);
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
    parsePlate(attr, zarrRootPath, "", store);
    setSeries(0);
    LOGGER.debug("ZarrReader initialization complete");
  }
  
  private List<String> reorderGroupKeys(ArrayList<String> groupKeys, List<String> originalKeys) {
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

  private List<String> reorderGroupKeys(ArrayList<String> groupKeys) {
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
  private static int[] getOriginalShape(int [] shape5D, int size) {
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
      initializeZarrService();
    }
    catch (FormatException e) {
      throw new IOException(e);
    }
  }

  protected void initializeZarrService() throws IOException, FormatException {
    zarrService = new JZarrServiceImpl(altStore());
    openZarr();
  }

  @Override
  public byte[] openBytes(int no, byte[] buf, int x, int y, int w, int h) throws FormatException, IOException {
    FormatTools.checkPlaneParameters(this, no, buf.length, x, y, w, h);
    openZarr();
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
    setSeries(no, false);
  }
  
  public void setSeries(int no, boolean openZarr) {
    super.setSeries(no);
    if (openZarr) {
      openZarr();
    }
  }

  @Override
  public void setResolution(int no) {
    setResolution(no, false);
  }
  
  public void setResolution(int no, boolean openZarr) {
    super.setResolution(no);
    if (openZarr) {
      openZarr();
    }
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
          if (seriesIndex != currentOpenZarr) {
            newZarrPath += File.separator + arrayPaths.get(seriesIndex);
            String canonicalPath = new Location(newZarrPath).getCanonicalPath();
            LOGGER.debug("Opening zarr for series {} at path: {}", seriesIndex, canonicalPath);
            zarrService.open(canonicalPath);
            currentOpenZarr = seriesIndex;
          }
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
        if (includeLabels() || !arrayPath.toLowerCase().contains("labels")) {
          arrayPaths.add(arrayPath);
        }
      }
    }
  }

  private void parseResolutionCount(String root, String key, Map<String, Object> attr) throws IOException, FormatException {
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
            uniqueResCounts.add(numRes);
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

  private void generateArrayKeys(Map<String, Object> attr, String canonicalPath) {
    if (uniqueResCounts.size() != 1) {
      LOGGER.debug("Cannout automatically generate ArrayKeys as resolution counts differ");
    }
    Map<Object, Object> plates = (Map<Object, Object>) attr.get("plate");
    if (plates != null) {
      ArrayList<Object> columns = (ArrayList<Object>)plates.get("columns");
      ArrayList<Object> rows = (ArrayList<Object>)plates.get("rows");
      Integer fieldCount = (Integer) plates.get("field_count");
      for (Object row: rows) {
        String rowName = ((Map<String, String>) row).get("name");
        for (Object column: columns) {
          String columnName = ((Map<String, String>) column).get("name");
          for (int i = 0; i < fieldCount; i++) {
            int resolutionCount = (Integer)(uniqueResCounts.toArray())[0];
            for (int j = 0; j < resolutionCount; j++) {
              String key = rowName + File.separator + columnName + File.separator + i + File.separator + j;
              if (Files.isDirectory(Paths.get(canonicalPath+File.separator+key))) {
                arrayPaths.add(rowName + File.separator + columnName + File.separator + i + File.separator + j);
              }
              else {
                LOGGER.debug("Skipping array path as sparse data: {}", key);
              }
            }
          }
        }
      }
    }
  }

  private void generateGroupKeys(Map<String, Object> attr, String canonicalPath) {
    Map<Object, Object> plates = (Map<Object, Object>) attr.get("plate");
    if (plates != null) {
      ArrayList<Object> columns = (ArrayList<Object>)plates.get("columns");
      ArrayList<Object> rows = (ArrayList<Object>)plates.get("rows");
      Integer fieldCount = (Integer) plates.get("field_count");

      for (Object row: rows) {
        String rowName = ((Map<String, String>) row).get("name");
        if (Files.isDirectory(Paths.get(canonicalPath+File.separator+rowName))) {
          groupKeys.add(rowName);
        }
        else {
          LOGGER.debug("Skipping group key as sparse data: {}", rowName);
        }
        for (Object column: columns) {
          String columnName = ((Map<String, String>) column).get("name");
          String columnKey = rowName + File.separator + columnName;
          if (Files.isDirectory(Paths.get(canonicalPath+File.separator+columnKey))) {
            groupKeys.add(columnKey);
          }
          else {
            LOGGER.debug("Skipping group key as sparse data: {}", columnKey);
          }
          for (int i = 0; i < fieldCount; i++) {
            String key = rowName + File.separator + columnName + File.separator + i;
            if (Files.isDirectory(Paths.get(canonicalPath+File.separator+key))) {
              groupKeys.add(key);
            }
            else {
              LOGGER.debug("Skipping group key as sparse data: {}", key);
            }
          }
        }
      }
    }
  }

  private void parsePlate(Map<String, Object> attr, String root, String key, MetadataStore store) throws IOException, FormatException {
    Map<Object, Object> plates = (Map<Object, Object>) attr.get("plate");
    if (plates != null) {
      ArrayList<Object> columns = (ArrayList<Object>)plates.get("columns");
      ArrayList<Object> rows = (ArrayList<Object>)plates.get("rows");
      ArrayList<Object> wells = (ArrayList<Object>)plates.get("wells");
      ArrayList<Object>  acquisitions = (ArrayList<Object> )plates.get("acquisitions");
      String plateName = (String) plates.get("name");
      Integer fieldCount = (Integer) plates.get("field_count");

      String plate_id =  MetadataTools.createLSID("Plate", 0);
      store.setPlateID(plate_id, 0);
      store.setPlateName(plateName, 0);
      HashMap<Integer, Integer> acqIdsIndexMap = new HashMap<Integer, Integer>();
      if (acquisitions != null) {
        for (int a = 0; a < acquisitions.size(); a++) {
          Map<String, Object> acquistion = (Map<String, Object>) acquisitions.get(a);
          Integer acqId = (Integer) acquistion.get("id");
          String acqName = (String) acquistion.get("name");
          String acqDescription = (String) acquistion.get("description");
          Integer acqStartTime = (Integer) acquistion.get("starttime");
          Integer acqEndTime = (Integer) acquistion.get("endtime");
          Integer maximumfieldcount = (Integer) acquistion.get("maximumfieldcount");
          acqIdsIndexMap.put(acqId, a);
          store.setPlateAcquisitionID(
              MetadataTools.createLSID("PlateAcquisition", 0, acqId), 0, a);
          if (acqName != null) {
            store.setPlateAcquisitionName(acqName, 0, a);
          }
          if (acqDescription != null) {
            store.setPlateAcquisitionDescription(acqDescription, 0, a);
          }
          if (maximumfieldcount != null) {
            store.setPlateAcquisitionMaximumFieldCount(new PositiveInteger(maximumfieldcount), 0, a);
          }
          if (acqStartTime != null) {
            store.setPlateAcquisitionStartTime(new Timestamp(acqStartTime.toString()), 0, a);
          }
          if (acqEndTime != null) {
            store.setPlateAcquisitionEndTime(new Timestamp(acqEndTime.toString()), 0, a);
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

  private void parseLabels(String root, Map<String, Object> attr) throws IOException, FormatException {
    ArrayList<Object> labels = (ArrayList<Object>) attr.get("labels");
    if (labels != null) {
      for (int l = 0; l < labels.size(); l++) {
        String label = (String) labels.get(l);
      }
    }
  }

  private void parseImageLabels(String root, Map<String, Object> attr) throws IOException, FormatException {
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
          Number area = (Number) prop.get("area (pixels)");
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

  public void parseOmeroMetadata(Map<String, Object> attr) throws IOException, FormatException {
    Map<String, Object> omeroMetadata = (Map<String, Object>) attr.get("omero");
    if (omeroMetadata != null) {
      Integer id = (Integer) omeroMetadata.get("id");
      String name = (String) omeroMetadata.get("name");
      String version = (String) omeroMetadata.get("version");
      ArrayList<Object> channels = (ArrayList<Object>)omeroMetadata.get("channels");
      for (int i = 0; i < channels.size(); i++) {
        Map<String, Object> channel = (Map<String, Object>) channels.get(i);
        Boolean channelActive = (Boolean) channel.get("active");
        Number channelCoefficient = (Number) channel.get("coefficient");
        String channelColor = (String) channel.get("color");
        String channelFamily = (String) channel.get("family");
        Boolean channelInverted = (Boolean) channel.get("inverted");
        String channelLabel = (String) channel.get("label");
        Map<String, Object> window = (Map<String, Object>)channel.get("window");
        if (window != null) {
          Number windowStart = getDouble(window, "start");
          Number windowEnd = getDouble(window, "end");
          Number windowMin = getDouble(window, "min");
          Number windowMax = getDouble(window, "max");
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
      if (saveAnnotations()) {
        Hashtable originalMetadata = service.getOriginalMetadata(omexmlMeta);
        if (originalMetadata != null) metadata = originalMetadata;
      }
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
        for (int wellSampleIndex = 0; wellSampleIndex < omexmlMeta.getWellSampleCount(plateIndex, wellIndex); wellSampleIndex++) { 
          String expectedPath = rowLetter + File.separator + (col.getValue() + 1) + File.separator  + wellSampleIndex;
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

    OME root = (OME) omexmlMeta.getRoot();
    
    // Optionally remove all annotations
    if (!saveAnnotations()) {
      root.setStructuredAnnotations(null);
      omexmlMeta.setRoot((MetadataRoot) root);
    }
    else {
      // Remove old PyramidResolution annotations
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

  private Number getDouble(Map<String, Object> src, String key) {
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
    int rootPathLength = zarrRootPath.length();
    ArrayList<String> usedFiles = new ArrayList<String>();
    reloadOptionsFile(zarrRootPath);

    boolean skipPixels = noPixels || !listPixels() || !systemEnvListPixels();
    boolean includeLabels = includeLabels();
    try (Stream<Path> paths = Files.walk(Paths.get(zarrRootPath), FileVisitOption.FOLLOW_LINKS)) {
      paths.filter(Files::isRegularFile) 
      .forEach(path -> {
        if (
         (!skipPixels && includeLabels) ||
         (!skipPixels && !includeLabels && (path.toString().toLowerCase().lastIndexOf("labels")<rootPathLength) ||
         (skipPixels && includeLabels && (path.endsWith(".zgroup") || path.endsWith(".zattrs") || path.endsWith(".xml"))) ||
         (skipPixels && !includeLabels && (path.toString().toLowerCase().lastIndexOf("labels")<rootPathLength) &&(path.endsWith(".zgroup") || path.endsWith(".zattrs") || path.endsWith(".xml")))))
          {
            usedFiles.add(path.toFile().getAbsolutePath());
          }
      }
      );
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

  /* @see loci.formats.FormatReader#initFile(String) */
  @Override
  protected ArrayList<String> getAvailableOptions() {
    ArrayList<String> optionsList = super.getAvailableOptions();
    optionsList.add(SAVE_ANNOTATIONS_KEY);
    optionsList.add(LIST_PIXELS_KEY);
    optionsList.add(QUICK_READ_KEY);
    optionsList.add(INCLUDE_LABELS_KEY);
    optionsList.add(ALT_STORE_KEY);
    return optionsList;
  }

  /**
   * Used to decide if all the zarr metadata is additionally stored as XML annotations
   * @return boolean true if all metadata should be saved as an annotation, default is false
   */
  public boolean saveAnnotations() {
    MetadataOptions options = getMetadataOptions();
    if (options instanceof DynamicMetadataOptions) {
      return ((DynamicMetadataOptions) options).getBoolean(
          SAVE_ANNOTATIONS_KEY, SAVE_ANNOTATIONS_DEFAULT);
    }
    return SAVE_ANNOTATIONS_DEFAULT;
  }
  
  /**
   * Used to decide if getUsedFiles should list all of the pixel chunks 
   * @return boolean true if the full list of files including pixels should be returned, default is true
   */
  public boolean listPixels() {
    MetadataOptions options = getMetadataOptions();
    if (options instanceof DynamicMetadataOptions) {
      return ((DynamicMetadataOptions) options).getBoolean(
          LIST_PIXELS_KEY, LIST_PIXELS_DEFAULT);
    }
    return LIST_PIXELS_DEFAULT;
  }
  
  /**
   * Used to decide if performance improvements are applied during initialization
   * This makes assumptions about the data, assuming that the shape of images remains consistent
   * @return boolean true if performance improvements should be applied, default is false
   */
  public boolean quickRead() {
    MetadataOptions options = getMetadataOptions();
    if (options instanceof DynamicMetadataOptions) {
      return ((DynamicMetadataOptions) options).getBoolean(
          QUICK_READ_KEY, QUICK_READ_DEFAULT);
    }
    return QUICK_READ_DEFAULT;
  }
 
  /**
   * Used to decide if images stored in the label sub folder should be included in the list of images
   * @return boolean true if images in the label folder should be included, default is false
   */
  public boolean includeLabels() {
    MetadataOptions options = getMetadataOptions();
    if (options instanceof DynamicMetadataOptions) {
      return ((DynamicMetadataOptions) options).getBoolean(
          INCLUDE_LABELS_KEY, INCLUDE_LABELS_DEFAULT);
    }
    return INCLUDE_LABELS_DEFAULT;
  }
  
  /**
   * Used to provide the location of an alternative file store where the data is located
   * @return String representing the root path of the alternative file store or null if no alternative location exist
   */
  public String altStore() {
    MetadataOptions options = getMetadataOptions();
    if (options instanceof DynamicMetadataOptions) {
      return ((DynamicMetadataOptions) options).get(
          ALT_STORE_KEY, ALT_STORE_DEFAULT);
    }
    return ALT_STORE_DEFAULT;
  }

  private boolean systemEnvListPixels() {
    String value = System.getenv(LIST_PIXELS_ENV_KEY);
    if (value != null && value.equalsIgnoreCase("true")) return true;
    if (value != null &&  value.toLowerCase().equals("false")) return false;
    return LIST_PIXELS_DEFAULT;
  }
  
  /**
   * Reloads the bfoptions file so that the options are able to be read for each getUsedFiles
   * Otherwise the options are read when initialised and saved as part of the memo file
   * @param id of the options file to reload
   */
  private void reloadOptionsFile(String id) {
    String optionsFile = DynamicMetadataOptions.getMetadataOptionsFile(id);
    if (optionsFile != null) {
      MetadataOptions options = getMetadataOptions();
      if (options != null && options instanceof DynamicMetadataOptions) {
        try {
          ((DynamicMetadataOptions) options).loadOptions(optionsFile, getAvailableOptions());
        } catch (Exception e) {
          LOGGER.warn("Exception while attempting to read metadata options file", e);
        }
      }
    }
  }
}
