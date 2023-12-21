#!/usr/bin/env python
#
# Filename: manual_query_printout.py
# Description: Query database and print out results
# Authors: Lynn Sanford <lynn.sanford@colorado.edu>
#

# Commentary:
#
# This file contains code for inputting a existing MySQL query
# in order to query Dowell Lab Nascent Database
#

# Code:

# Import
import csv
import sqlalchemy as sql
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '.', 'global_files'))
import dborm
import dbutils

# Load config file and connect to db
config = dbutils.load_config(
    "/scratch/Shares/dowell/dbnascent/DBNascent-build/config/config_query.txt")
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]
dbconnect = dbutils.dbnascentConnection(db_url, creds)

### User inputs ###
outfile = "/Users/lysa8537/db_query_outputs/high_quality_controls.tsv"

db_fields = ["paper_name",
             "sample_name",
             "cell_type",
             "sample_qc_score",
             "condition_type",
             "trim_read_depth",
#             "organism",
#             "treatment",
#             "conc_intens",
#             "start_time",
#             "end_time",
#             "time_unit",
#             "duration",
#             "duration_unit"
            ]

query_build = 'SELECT paper_name,sample_name,cell_type,sample_qc_score,condition_type,trim_read_depth FROM linkIDs INNER JOIN genetics ON genetics.id = linkIDs.genetic_id INNER JOIN organisms ON organisms.id = genetics.organism_id INNER JOIN samples ON samples.id = linkIDs.sample_id INNER JOIN conditionLink ON conditionLink.sample_id = linkIDs.sample_id INNER JOIN conditions ON conditions.id = conditionLink.condition_id WHERE organism = "H. sapiens" AND control_experimental = "control" AND sample_qc_score < 3 AND cell_type IN ("MCF7","lymphoblast","HeLa","K562","HEK293T Flp-In","HEK293","HEK293T") AND condition_type = "no treatment" ORDER BY cell_type;'

#query_build = 'SELECT paper_name,sample_name,cell_type,organism,treatment,conc_intens,start_time,end_time,time_unit,duration,duration_unit FROM linkIDs INNER JOIN conditionLink ON conditionLink.sample_id = linkIDs.sample_id INNER JOIN conditions ON conditions.id = conditionLink.condition_id INNER JOIN genetics ON genetics.id = linkIDs.genetic_id INNER JOIN organisms ON organisms.id = genetics.organism_id WHERE linkIDs.sample_id IN (SELECT conditionLink.sample_id FROM conditionLink INNER JOIN conditions ON conditions.id = conditionLink.condition_id WHERE treatment = "DRB") ORDER BY sample_name'

#query_build = 'SELECT paper_name,sample_name,cell_type FROM linkIDs INNER JOIN genetics ON genetics.id = linkIDs.genetic_id INNER JOIN organisms ON organisms.id = genetics.organism_id INNER JOIN samples ON samples.id = linkIDs.sample_id INNER JOIN bidirflowLink ON bidirflowLink.sample_id = linkIDs.sample_id INNER JOIN bidirflowRuns ON bidirflowRuns.id = bidirflowLink.bidirflow_id WHERE control_experimental = "control" AND organisms.organism = "H. sapiens" AND fcgene_date IS NOT NULL AND cell_type in (SELECT cell_type FROM linkIDs INNER JOIN genetics ON genetics.id = linkIDs.genetic_id INNER JOIN organisms ON organisms.id = genetics.organism_id INNER JOIN samples ON samples.id = linkIDs.sample_id INNER JOIN bidirflowLink ON bidirflowLink.sample_id = linkIDs.sample_id INNER JOIN bidirflowRuns ON bidirflowRuns.id = bidirflowLink.bidirflow_id WHERE control_experimental = "control" AND organisms.organism = "H. sapiens" AND fcgene_date IS NOT NULL GROUP BY cell_type HAVING COUNT(cell_type) > 5) ORDER BY cell_type'

### Query database and print results

results = dbconnect.session.execute(sql.text(query_build)).fetchall()

# Write out results of query (and query itself)
with open(outfile, "w") as f:
    w = csv.writer(f, delimiter='\t')
    w.writerow(db_fields)
    for data in results:
        w.writerow(data)

with open(outfile[0:-4] + "_query.txt", "w") as f:
    f.write(query_build)
