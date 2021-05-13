# dbnascent_build

This repository is intended for building and updating DBNascent.

It pulls from a metadata table and quality control data from samples.

The metadata table is currently in Google Sheets, but a subset has been included in this repository for testing (db_subsample.tsv).

The quality control data is present on Fiji, but the data corresponding to the testing subset has been included in this repository (qc/).

## Usage

In order to generate the database, you must have a pre-existing SQLite database by installing SQLite and creating a database at the location given in the config file. The database should be empty before initiating the python script.
