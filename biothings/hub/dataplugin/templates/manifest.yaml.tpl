---
version: '0.3'
__metadata__: # Optional.
  license_url: https://example.com/  # Optional. Put your license url here
  licence: ABCXYZ # Optional. Your license name
  url: https://example.com/  # Your site url
  description: # Optional. More description for this data plugin
requires:  # Optional. Listing all extra packages if need
#  - pandas
#  - numpy
dumper:
  data_url:  # A string, or a list of strings, containins URLs. Currently supported protocols are: http, https, and ftp. http/https must not be mixed with ftp (only one protocol supported)
#    - https://s3.pgkb.org/data/annotations.zip
#    - https://s3.pgkb.org/data/drugLabels.zip
#    - https://s3.pgkb.org/data/occurrences.zip
  uncompress: true  # true|false. tells the studio to try to uncompress downloaded data. Currently supports zip, gz, bz2 and xz format.
  schedule: '* * * * * */10'  # Optional. Will trigger the scheduling of the dumper, so it automatically checks for new data on a regular basis.
  # Format is the same as crontabs, with the addition of an Optionall sixth parameter for scheduling by the seconds.
  release: versions:get_release  # Optional. Format: "module:function", refers to a function returning a string as a release.
{% if not multi_uploaders %}
uploader: # Tells the studio how to parse and upload data once it's been dumped locally
  parser: parser:load_data  # Format "module:fuction", where function takes a data folder path as argument
  mapping: mapping:custom_mapping  # Optional. Points to a module:classmethod_name that can be used to specify a custom ElasticSearch mapping.
  ignore_duplicates: false  # What to do if duplicates are found (parser returns dict with same _id). Can be either error|ignore|merge.
  {% if parallelizer %}parallelizer: parallelizer:custom_jobs  # Optional.  If multiple input files exist, using the exact same parser, the uploader can be parallelized using that option{% end %}
{% else %}
uploaders:  # Tells the studio how to parse and upload data once it's been dumped locally
  - name: data1  # The name of uploader, must difference with other uploader
    parser: parser:load_data  # Format "module:fuction", where function takes a data folder path as argument
    mapping: mapping:custom_mapping  # Points to a module:classmethod_name that can be used to specify a custom ElasticSearch mapping.
    ignore_duplicates: false  # What to do if duplicates are found (parser returns dict with same _id). Can be either error|ignore|merge.
    {% if parallelizer %}parallelizer: parallelizer:custom_jobs  # Optional.  If multiple input files exist, using the exact same parser, the uploader can be parallelized using that option{% end %}
#  - name: data2
#    parser: parser:load_data2
#    ignore_duplicates: false
{% end %}
