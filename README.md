# OMEZarrReader

The OMEZarrReader provides support for reading data following the [NGFF](https://ngff.openmicroscopy.org/)
specificaiton. It uses the [JZarr](https://github.com/zarr-developers/jzarr) library from [Maven Central](https://central.sonatype.com/artifact/dev.zarr/jzarr/) for accessing the underlying Zarr data.

## Installation

### Fiji Update Site

The OMEZarrReader has been added to the Bio-Formats development update site
(https://sites.imagej.net/Bio-Formats). Be aware that activating it will use
a newer version of Bio-Formats.

### Maven

The jar from this repository can be used in your own code by enabling the OME artifactory repositry:

```
      <repositories>
        <repository>
          <snapshots>
            <enabled>false</enabled>
          </snapshots>
          <id>central</id>
          <name>repo</name>
          <url>https://artifacts.openmicroscopy.org/artifactory/repo</url>
        </repository>
      </repositories>
```
and adding the dependency:
```
<dependency>
    <groupId>ome</groupId>
    <artifactId>OMEZarrReader</artifactId>
    <version>${OMEZarrReader.version}</version>
</dependency>
```

### Releases

Release versions of OMEZarrReader are also available directly from the [OME artifactory](https://artifacts.openmicroscopy.org/artifactory/webapp/browserepo.html?0&pathId=ome.releases:ome/OMEZarrReader)

### Bio-Formats tools

If you would like to use OMEZarrReader with the bftools suite, you will need to set the `BC_CP`
environment variable to include the jar which includes all dependencies:

```
BF_CP=target/OMEZarrReader-with-dependencies.jar showinf -nopix your.ome.zarr/.zattrs
```

## Reader specific options

The OMEZarrReader has a number of reader specific options which can be used to customise the reader behaviour. This options can be used in the same manner as the reader options for Bio-Formats outlined [here](https://bio-formats.readthedocs.io/en/latest/formats/options.html#usage). 

The list of available options are below:

| Option | Default | Description |
| --- | --- | --- |
| `omezarr.quick_read` | false | Improves the read performance by limiting the number of files that are parsed. This assumes that the shape and resolution count of all images in a plate remains constant  |
| `omezarr.save_annotations` | false | Determines if all the Zarr JSON metadata should be stored as XML annotations in the OME Model |
| `omezarr.list_pixels` | false | Used to decide if getUsedFiles should list all of the pixel chunks |
| `omezarr.include_labels` | false | Used to decide if images stored in the label sub folder should be included in the list of images |
