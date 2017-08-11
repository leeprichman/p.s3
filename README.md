[![Build Status](https://travis-ci.org/andrewrech/p.s3.svg?branch=master)](https://travis-ci.org/andrewrech/p.s3) [![codecov.io](https://codecov.io/github/andrewrech/p.s3/coverage.svg?branch=master)](https://codecov.io/github/andrewrech/p.s3?branch=master) [![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/p.S3)](http://cran.r-project.org/package=p.S3) ![](https://img.shields.io/badge/version-0.0.1-blue.svg)

# p.s3

Fast, basic AWS S3 CLI in R.

## Description

Parallelized basic [Amazon Web Services][AWS] [S3][S3] operations from R using [GNU Parallel][P] and the [AWS CLI][CLI], including S3 indexing to a data table. The purpose is to allow fast, fine-grained control of versioned S3 buckets from R.

[AWS]: https://aws.amazon.com/
[S3]: https://aws.amazon.com/s3/
[P]: https://www.gnu.org/software/parallel/
[CLI]: https://aws.amazon.com/cli/


## Installation

```r
devtools::install_github("andrewrech/p.s3")
```

### Requirements

* Configured [AWS command line interface][AWS]
* [GNU Parallel][P]

## Manifest

* `import_S3_json`: Import AWS CLI S3 list-object-versions json output to a data table.
* `index_S3_objects`: Index S3 objects from a list of S3 buckets.
* `print_S3_statistics`: Print statistics about S3 buckets.
* `delete_S3_object_version`: Delete S3 object versions.
* `get_S3_object_version`: Download S3 object versions.
* `restore_S3_object_version`: Restore S3 object versions from AWS Glacier.
* `copy_S3_object_version`: Copying S3 object versions.
* `aws_query_string_auth_url`: Generating URLs for S3 resources with query string request authentication.

## Bugs

## Authors

[Andrew J. Rech](http://info.rech.io)

## License

GNU General Public License v3.0