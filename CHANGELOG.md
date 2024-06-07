0.5.0 (June 2024)
-----------------

- Updated GitHub Actions to v4 and now deploying snapshot on tag [#80](https://github.com/ome/ZarrReader/pull/80)
- Added Java 17 to the test matrix [#81](https://github.com/ome/ZarrReader/pull/81)
- Bumped the Bio-Formats version to 7.1.0 [#71](https://github.com/ome/ZarrReader/pull/71)
- Introduced S3FileSystemStore and a new option (omezarr.alt_store) for reading directly from S3 [#82](https://github.com/ome/ZarrReader/pull/82)

0.4.1 (February 2024)
---------------------

- Reduced the number of fields saved in memo files [#78](https://github.com/ome/ZarrReader/pull/78)

0.4.0 (December 2023)
---------------------

- Updated JZarr dependency to use dev.zarr:jzarr 0.4.0 [#54](https://github.com/ome/ZarrReader/pull/54)
- Removed duplicate declaration from POM file [#56](https://github.com/ome/ZarrReader/pull/56)
- Use canonical path for calls to ZarrService [#57](https://github.com/ome/ZarrReader/pull/57)
- Updated GitHub actions checkout to V3 [#59](https://github.com/ome/ZarrReader/pull/59)
- Updated ReadMe installation instructions and requirements [#62](https://github.com/ome/ZarrReader/pull/62)
- Implemented performance improvements and introduced new reader options [#64](https://github.com/ome/ZarrReader/pull/64)
- Bumped the Bio-Formats version to 7.0.0 [#65](https://github.com/ome/ZarrReader/pull/65)
- Bumped dev.zarr:jzarr to 0.4.2 [#66](https://github.com/ome/ZarrReader/pull/66)

0.3.1 (March 2023)
------------------

- Updated GitHub Actions output commands [#42](https://github.com/ome/ZarrReader/pull/42)
- Upgraded GitHub Actions setup-java action [#47](https://github.com/ome/ZarrReader/pull/47)
- Fixed handling of pre-existing plate metadata to avoid duplicate keys [#49](https://github.com/ome/ZarrReader/pull/49)

0.3.0 (June 2022)
-----------------

- Updated a number of dependencies [#38](https://github.com/ome/ZarrReader/pull/38)

0.2.0 (May 2022)
------------------

- Add OME-XML metadata support [#31](https://github.com/ome/ZarrReader/pull/31)

0.1.5 (April 2022)
------------------

- Fix HCS validation [#24](https://github.com/ome/ZarrReader/pull/24)

0.1.4 (April 2022)
------------------

- Remove dependency [#29](https://github.com/ome/ZarrReader/pull/29)