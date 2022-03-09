# dbnascent_build

This repository is intended for building and updating DBNascent.

It pulls from metadata tables, quality control data, and bidirectional call data from samples. All data is present on Fiji.

## Usage

The main scripts for building the database are db_global_add_update.py and db_paper_add_update.py, combined in the db_build_full.sbatch script. These scripts pull in the config_build.txt file which defines field equivalencies between the metadata table and the sql database fields, as well as file paths for relevant inputs. They also use the dborm.py and dbutils.py to define objects and functions for the database. Several other documents (organisms.txt and searcheq.txt) provide relatively static information for several database tables.

The database can be queried with defined fields and filtering specifications with query_printout.py for input into DESeq2 or other applications. This script relies on the config_query.txt config file, as well as the dborm and dbutils.
