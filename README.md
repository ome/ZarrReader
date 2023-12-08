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

### Bio-Formats tools

If you would like to use OMEZarrReader with the bftools suite, you will need to set the `BC_CP`
environment variable to include the jar which includes all dependencies:

```
BF_CP=target/OMEZarrReader-with-dependencies.jar showinf -nopix your.ome.zarr/.zattrs
```

## Known Issues/TODO list
- Currently working on packaging, discovered issue when connecting to S3 using packaged jar
- S3 File System Store is likely not ideal sceanrio, other options to be investigated
- S3 access currently very inefficient
- Odd issue with data being lost when decompressing bytes in jzarr, an ugly hack is currently in place
- Identification of S3 location needs updating
- Refactor code to remove duplication
- Parse colours for labels
