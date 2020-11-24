# ZarrReader

Requires custom jzarr: https://github.com/dgault/jzarr/tree/ome-zarr
Requires Bio-Formats update to add reader: https://github.com/ome/bioformats/pull/3639

Known Issues/TODO list:
- Currently working on packaging, discovered issue when connecting to S3 using packaged jar
- S3 File System Store is likely not ideal sceanrio, other options to be investigated
- S3 access currently very inefficient
- Odd issue with data being lost when decompressing bytes in jzarr, an ugly hack is currently in place
- Identification of S3 location needs updating
- Refactor code to remove duplication
- Parse colours for labels
