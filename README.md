# dbnascent_build

This repository is intended for building and updating DBNascent.

It pulls from a metadata table and quality control data from samples.

The metadata table is currently in Google Sheets, but a subset has been included in this repository for testing (db_subsample.tsv).

The quality control data is present on Fiji, but the data corresponding to the testing subset has been included in this repository (qc/).

## Usage

In order to generate the database, you must have a pre-existing SQLite database by installing SQLite and creating a database at the location given in the config file. The database should be empty before initiating the python script.

The main notebook for building is dbnascent_initial_build.ipynb. This pulls in the config.txt file which defines field equivalencies between the metadata table and the sql database fields, as well as file paths for relevant inputs. It also uses the orm.py and utils.py to define objects and functions for the database. Several other documents (organisms.txt and searcheq.txt) provide relatively static information for several database tables.

The database can be queried with defined fields and filtering specifications with the query_printout.ipynb notebook for input into DESeq2 or other applications.
