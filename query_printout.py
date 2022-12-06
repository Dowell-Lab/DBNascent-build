#!/usr/bin/env python
#
# Filename: query_printout.py
# Description: Query database and print out results
# Authors: Lynn Sanford <lynn.sanford@colorado.edu>
#

# Commentary:
#
# This file contains code for setting up MYSQL query
# in order to query Dowell Lab Nascent Database
#

# Code:

# Import
import csv
import sqlalchemy as sql
import dbutils
import dborm

### User input variables ###
user_query_fields = ["sample_name",
#                     "srp",
                     "paper_id",
#                     "year",
                     "protocol",
#                     "library",
#                     "spikein",
                     "sample_type",
                     "cell_type",
                     "organism",
#                     "trim_read_depth",
#                     "exint_ratio",
#                     "distinct_tenmillion_prop",
#                     "genome_prop_cov",
#                     "avg_fold_cov",
                     "samp_qc_score",
                     "samp_data_score",
                     "replicate",
                     "control_experimental",
#                     "notes",
#                     "tfit_bidir_gc_prop",
#                     "num_tfit_bidir",
#                     "num_tfit_bidir_promoter",
#                     "num_tfit_bidir_intronic",
#                     "num_tfit_bidir_intergenic",
#                     "dreg_bidir_gc_prop",
#                     "num_dreg_bidir",
#                     "num_dreg_bidir_promoter",
#                     "num_dreg_bidir_intronic",
#                     "num_dreg_bidir_intergenic",
#                     "tfit_master_merge_incl",
#                     "dreg_master_merge_incl",
#                     "bidirflow_date",
#                     "dreg_date",
#                     "dreg_postprocess_date",
#                     "tfit_date",
#                     "tfit_prelim_date",
#                     "fcgene_date",
#                     "fcbidir_date",
#                     "nascentflow_date",
#                     "downfile_pipeline_date",
#                     "rseqc_date",
#                     "preseq_date",
#                     "treatment",
#                     "conc_intens",
#                     "start_time",
#                     "end_time",
#                     "time_unit",
#                     "duration",
#                     "duration_unit",
                    ]

user_filter_fields = {
#    "cell_type": ['IN ("U2OS","lymphoblast","IMR90")']
#    "organism": ['= "M. musculus"'],
#    "control_experimental": ['= "control"'],
#    "tfit_date": ['IS NOT NULL'],
#    "samp_qc_score": ['< 4'],
#     "treatment": ['= "DRB"'],
    }

outfile = "/Users/lysa8537/db_query_outputs/220829_mouse.tsv"


### Determine which search fields are from which tables
### If a field is in multiple tables, only the first one is used

# Load config file and connect to db
config = dbutils.load_config(
    "/scratch/Shares/dowell/dbnascent/DBNascent-build/config_query.txt")
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]
dbconnect = dbutils.dbnascentConnection(db_url, creds)

# Find and store all tables and fields necessary for query
db_query_fields = {}
db_filter_fields = {}
tables_to_join = []
query_join_keys = dict(config["query_join_keys"])
db_tables = dict(dborm.Base.metadata.tables)

for table in db_tables.keys():
    for field in user_query_fields:
        if field in db_tables[table].columns.keys():
            # If a field is found in an ORM table object,
            # store the field with table name attached
            if field not in db_query_fields.keys():
                db_query_fields[field] = table + "." + field
                # If that table is not already on the list of
                # tables to join, add it
                if table not in tables_to_join:
                    if table == "geneticInfo":
                        if "tissueDetails" not in tables_to_join:
                            tables_to_join.append("tissueDetails")
                    elif table not in query_join_keys["existing"]:
                        tables_to_join.append(table)
    for filtkey in user_filter_fields:
        if filtkey in db_tables[table].columns.keys():
            if filtkey not in db_filter_fields.keys():
                db_filter_fields[filtkey] = table + "." + filtkey
            if table not in tables_to_join:
                if table == "geneticInfo":
                    if "tissueDetails" not in tables_to_join:
                        tables_to_join.append("tissueDetails")
                elif table not in query_join_keys["existing"]:
                    tables_to_join.append(table)

### Build raw SQlite query string

# Initialize query string
query_build = (
    "SELECT DISTINCT "
)

# Add all query fields
i = 0
for field in db_query_fields:
    if i == 0:
        query_build = query_build + db_query_fields[field]
        i = 1
    else:
        query_build = query_build + ", " + db_query_fields[field]

# Add all tables
query_build = (
    query_build
    + " FROM linkIDs"
    + " INNER JOIN exptMetadata ON exptMetadata.expt_id = linkIDs.expt_id"
)

for table in tables_to_join:
    query_build = query_build + " " + query_join_keys[table.lower()]

# Add all filters
i = 0
for filtkey in user_filter_fields.keys():
    for value in user_filter_fields[filtkey]:
        if i == 0:
            query_build = (query_build + 
                           " WHERE " + 
                           db_filter_fields[filtkey] + 
                           " " + 
                           str(value)
                          )
            i = 1
        else:
            query_build = (query_build + 
                           " AND " + 
                           db_filter_fields[filtkey] + 
                           " " + 
                           str(value)
                          )


### Query database and print results

results = dbconnect.session.execute(sql.text(query_build)).fetchall()

# Write out results of query (and query itself)
with open(outfile, "w") as f:
    w = csv.writer(f, delimiter='\t')
    w.writerow((list(db_query_fields.keys())))
    for data in results:
        w.writerow(data)

with open(outfile[0:-4] + "_query.txt", "w") as f:
    f.write(query_build)
