#!/usr/bin/env python
# coding: utf-8

# query_printout.py
# This allows a user to make a query and print out results

import csv
import os
import re
import shutil
import numpy as np
import sqlalchemy as sql
import dbutils
import dborm

### User input variables
user_input_fields = ["paper_id", "organism", "cell_type", "genotype", "construct", ]
user_input_filters = {"genotype": ["IS NOT NULL"],
                      #"baseline_control_expt": ['in ("control","baseline")'],
#                      "cell_type": ['in ("HeLa")'],
#                      "samp_qc_score": ['< 3'],
#                      "paper_id": ['= "Andrysik2017identification"'],
                      }
outfile = "/Users/lysa8537/db_query_outputs/unique_genetic_manips.tsv"


# Figure out which search fields are from which tables
# If a field is in multiple tables, only the first one found is used
config = dbutils.load_config(
    "/scratch/Shares/dowell/dbnascent/db_build/config.txt")
query_join_keys = dict(config["query_join keys"])
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]
dbconnect = dbutils.dbnascentConnection(db_url, creds)

# Find all table columns and make a dict
db_query_names = {}
db_filter_names = {}
tables_to_join = []
db_tables = dict(dborm.Base.metadata.tables)
for key in db_tables:
    for column in user_input_fields:
        if column in db_tables[key].columns.keys():
            if column not in db_query_names.keys():
                db_query_names[column] = key + "." + column
            if key not in tables_to_join:
                if key not in query_join_keys["existing"]:
                    tables_to_join.append(key)
    for filtkey in user_input_filters:
        if filtkey in db_tables[key].columns.keys():
            if filtkey not in db_filter_names.keys():
                db_filter_names[filtkey] = key + "." + filtkey
            if key not in tables_to_join:
                if key not in query_join_keys["existing"]:
                    tables_to_join.append(key)

# Build raw SQlite query string
query_build = (
    "SELECT DISTINCT "
)
i = 0
for key in db_query_names:
    if i == 0:
        query_build = query_build + db_query_names[key]
        i = 1
    else:
        query_build = query_build + ", " + db_query_names[key]

query_build = (
    query_build
    + " FROM linkIDs"
    + " INNER JOIN exptMetadata ON exptMetadata.expt_id = linkIDs.expt_id"
    + " INNER JOIN sampleID ON sampleID.sample_id = linkIDs.sample_id"
)

for tab in tables_to_join:
    query_build = query_build + " " + query_join_keys[tab.lower()]

i = 0
for key in user_input_filters.keys():
    for value in user_input_filters[key]:
        if i == 0:
            query_build = query_build + " WHERE " + db_filter_names[key] + " " + value
            i = 1
        else:
            query_build = query_build + " AND " + db_filter_names[key] + " " + value

# Connect to database and query
results = dbconnect.session.execute(sql.text(query_build)).fetchall()

# Write out results of query (and query itself)
with open(outfile, "w") as f:
    w = csv.writer(f, delimiter='\t')
    w.writerow((list(db_query_names.keys())))
    for data in results:
        w.writerow(data)

with open(outfile[0:-4] + "_query.txt", "w") as f:
    f.write(query_build)
