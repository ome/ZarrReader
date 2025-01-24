# OMEZarrReader

The OMEZarrReader provides support for reading data following the [NGFF](https://ngff.openmicroscopy.org/)
specificaiton. It uses the [JZarr](https://github.com/zarr-developers/jzarr) library from [Maven Central](https://central.sonatype.com/artifact/dev.zarr/jzarr/) for accessing the underlying Zarr data.

## Installation

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

Release versions of OMEZarrReader are available directly from the [OME artifactory](https://artifacts.openmicroscopy.org/ui/repos/tree/General/ome.releases/ome/OMEZarrReader)

### Bio-Formats tools

If you would like to use OMEZarrReader with the bftools suite, you will need to set the `BC_CP`
environment variable to include the jar which includes all dependencies:

```
BF_CP=target/OMEZarrReader-with-dependencies.jar showinf -nopix your.ome.zarr/.zattrs
```

## Reader specific options

The OMEZarrReader has a number of reader specific options in version 0.4.0 which can be used to customise the reader behaviour. This options can be used in the same manner as the reader options for Bio-Formats outlined [here](https://bio-formats.readthedocs.io/en/latest/formats/options.html#usage). 

The new default behaviour of the `omezarr.include_labels` option introduced in v0.4.0 represents a change in behaviour from the v0.3 releases. Previously any Zarr arrays found in the labels folder would by default be represented as an additional image series. With the current default settings, Zarr arrays in the labels folder will no longer be included in the list of image series. Changing this setting to `true` will revert to the previous behaviour. 

**Note:** If you had imported data with labels into OMERO using version v0.3 or earlier then you will need to ensure that the `omezarr.include_labels` option is set to true. You can do this by adding a `bfoptions` file to the fileset. This will require running psql commands to update the database to include the new `bfoptions` file. If you need help with this scenario then please contact us on [image.sc](https://forum.image.sc/).

In version v0.5.0 a new option `omezarr.alt_store` was added. This allows for the source of an alternative file store to be configured. Setting the option means the pixel data to be read from a different source than originally used when initialising the reader. The initial implementation was intended for use with the [IDR] (https://idr.openmicroscopy.org/), allowing IDR to read data directly from an S3 location. The current implementation only allows for S3 access when using a public https endpoint with unauthenticated access using anonymous credentials. A more complete, general purpose implementation will follow in a future release.

An example of how this could be used would be to download locally a public dataset such as [6001240.zarr] (https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.4/idr0062A/6001240.zarr). Setting the option as below, either via the API or using a `bfoptions` file, will allow you to call setID on the local file but have the pixel data read from the public S3 endpoint.
 
```
omezarr.alt_store = https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.4/idr0062A/6001240.zarr
```

The list of available options are below:

| Option | Default | Description |
| --- | --- | --- |
| `omezarr.quick_read` | false | Improves the read performance by limiting the number of files that are parsed. This assumes that the shape and resolution count of all images in a plate remains constant  |
| `omezarr.save_annotations` | false | Determines if all the Zarr JSON metadata should be stored as XML annotations in the OME Model |
| `omezarr.list_pixels` | true | Used to decide if getUsedFiles should list all of the pixel chunks |
| `omezarr.include_labels` | false | Used to decide if images stored in the label sub folder should be included in the list of images |
| `omezarr.alt_store` | null | Used to provide the location of an alternative file store where the data is located |
