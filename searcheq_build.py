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
import dbutils
import dborm

# Load config file
config = dbutils.load_config(
    "/scratch/Shares/dowell/dbnascent/DBNascent-build/config_build.txt"
)

# Create database connection object
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]

dbconnect = dbutils.dbnascentConnection(db_url, creds)

# Reflect database tables and define searchable fields
dbtables = [
    "organismInfo",
    "tissueDetails",
    "exptMetadata",
    "sampleID",
    "geneticInfo",
    "conditionInfo",
    "sampleAccum",
]

fields = [
    "organism",
    "genome_build",
    "tissue",
    "cell_origin_type",
    "protocol",
    "library",
    "spikein",
    "paper_id",
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
    "samp_qc_score",
    "samp_data_score",
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
            dictentry = {
                "search_term": dbentry[field],
                "db_term": dbentry[field],
                "search_field": field,
            }
            search_table.append(dictentry)

# Read in additional manually curated search terms
search_keys = list(dict(config["searcheq keys"]).values())
dbsearch_keys = list(dict(config["searcheq keys"]).keys())
search_manual_path = config["file_locations"]["searcheq_manual"]
searcheqs = dbutils.Metatable(search_manual_path)
searcheqs.key_replace(search_keys, dbsearch_keys)

# Append manual to automatically generated and ensure unique
for entry in searcheqs.data:
    search_table.append(entry)
searcheqs_unique = search_table.unique(dbsearch_keys)

searcheq_full_path = config["file_locations"]["searcheq_table"]
with open(searcheq_full_path, 'w') as outfile:
    outfile.write('search_term\tdb_term\tsearch_field\n')    
    for entry in searcheqs_unique:
        outfile.write('\t'.join(entry.values()) + '\n')

# If not already present, add data to database
searcheqs_to_add = dbutils.entry_update(
                       dbconnect,
                       "searchEquiv",
                       dbsearch_keys,
                       searcheqs_unique
                   )
if len(searcheqs_to_add) > 0:
    dbconnect.engine.execute(
        dborm.searchEquiv.__table__.insert(),
        searcheqs_to_add
    )