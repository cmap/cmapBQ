# BQ_toolkit
Collections of scripts to work with Google BigQuery

Author: Anup Jonchhe (anup@broadinstitute.org)

Contains a python package cmapBQ with tools to convert GCT(x) files to parquet, upload to BQ, validate tables against GCT 
files and sumbit queries to BigQuery

## Installation
cmapBQ can be added to the current virtual environment by running the following command from witin the repo directory

`python setup.py install` 

Note: Updates to the repo are not represented in the venv unless this command is run again in the updated repo directory.


## Query

Query related functions can be found within `cmapBQ.query` module. 

## Structure

Runnable modules are placed within the `cmapBQ/tools` directory. These modules must have a main() function.

### Tools

A list of available tools can be found by running the `cmapBQ` command without arguments. All tools should support 
`cmapBQ [toolname] --help` syntax
