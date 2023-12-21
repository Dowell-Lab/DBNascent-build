#!/usr/bin/env python
#
# Filename: searcheq_build.py
# Description: Build the search table
# Authors: Lynn Sanford <lynn.sanford@colorado.edu>
#

# Commentary:
#
# This file contains code for retrieving all unique
# values in searchable fields and formatting it for
# inputting into database as searchEquiv table
#

# Code:

# Import
import csv
import sqlalchemy as sql
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'global_files'))
import dborm
import dbutils

# Load config file
config = dbutils.load_config(
    "/home/lsanford/DBNascent-build/config/config_build.txt"
)
files = config["file_locations"]

# Create database connection object
dbconnect = dbutils.dbnascentConnection(files["database"], files["credentials"])

# Delete current searcheq table
dbconnect.delete_tables([dborm.searchEquiv.__table__,])
dbconnect.add_tables()

# Reflect database tables and define searchable fields
dbtables = [
    "organisms",
    "tissues",
    "papers",
    "samples",
    "sampleEquiv",
    "genetics",
    "conditions",
]

fields = [
    "organism",
    "genome_build",
    "tissue",
    "cell_origin_type",
    "protocol",
    "library",
    "spikein",
    "paper_name",
    "year",
    "first_author",
    "last_author",
    "srr",
    "sample_name",
    "sample_type",
    "cell_type",
    "strain",
    "genotype",
    "construct",
    "condition_type",
    "treatment",
    "duration_unit",
    "replicate",
    "single_paired",
    "control_experimental",
    "sample_qc_score",
    "sample_data_score",
]

full_dbdump = []
for table in dbtables:
    dbdump = dbconnect.reflect_table(table)
    for entry in dbdump:
        full_dbdump.append(entry)

search_table = []
for dbentry in full_dbdump:
    for field in fields:
        if field in dbentry.keys():
            if dbentry[field]:
                dictentry = {
                    "search_term": dbentry[field],
                    "db_term": dbentry[field],
                    "search_field": field,
                }
                search_table.append(dictentry)

# Read in additional manually curated search terms
search_keys = dbutils.load_keys(config,"searchequiv")
search_manual_path = files["searcheq_manual"]
searcheqs = dbutils.Metatable(search_manual_path)
searcheqs.key_replace(search_keys["in"], search_keys["match"])

# Append manual to automatically generated and ensure unique
for entry in searcheqs.data:
    search_table.append(entry)
search_table_full = dbutils.Metatable(search_table)
searcheqs_unique = search_table_full.unique(search_keys["db"])

searcheq_full_path = files["searcheq_table"]
with open(searcheq_full_path, 'w') as outfile:
    outfile.write('search_term\tdb_term\tsearch_field\n')    
    for entry in searcheqs_unique:
        outfile.write('\t'.join(entry.values()) + '\n')

# If not already present, add data to database
searcheqs_to_add = dbutils.entry_update(
                       dbconnect,
                       "searchEquiv",
                       search_keys["db"],
                       searcheqs_unique
                   )
if len(searcheqs_to_add) > 0:
    searcheqs_to_add = dbutils.format_for_db_add(dbconnect,searcheqs_to_add)
    dbconnect.engine.execute(
        dborm.searchEquiv.__table__.insert(),
        searcheqs_to_add
    )
