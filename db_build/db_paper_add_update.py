#!/usr/bin/env python
#
# Filename: db_paper_add_update.py
# Description: Changes to paper/sample DBNascent values
# Authors: Lynn Sanford <lynn.sanford@colorado.edu>
#
# Commentary:
#
# This file contains code for adding/changing paper-
# and sample-specific values in the Dowell Lab's
# Nascent Database.
#
# Parameters:
#
# This script takes the paper identifier to process
# as its sole argument
#
# Contents:
#
# Step 1: Define paths and database connection
# Step 2: Parse paper and sample metadata tables
# Step 3: Load in organism and tissue info already in database
# Step 4: Add bidir summary data, if present
# Step 5: Calculate all sample-related fields and prep for db addition
#         **THIS ALSO DEFINES METRICS FOR QC/NRO SCORE CALCULATION**
# Step 6: Add samples, papers, genetics, bidirs to database
# Step 7: Make sampleEquiv and linkIDs entries and add to database
# Step 8: Add condition info to database
# Step 9: Add nascentflow/bidirflow version data
# Step 10: Make match tables

# Code:

# Import
from os.path import exists
import re
import sys, os
#sys.path.append(os.path.join(os.getcwd(), '..', 'global_files'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'global_files'))
import dborm
import dbutils

### Step 1: Define paths and database connection ###

# Raise error if no argument given
if len(sys.argv) < 2:
   raise NameError("No paper identifier given")
else:
   paper_id = sys.argv[1]
#paper_id = "Zhu2018rna"

# Load config file and keys
config = dbutils.load_config(
    "/home/lsanford/DBNascent-build/config/config_build.txt"
)
files = config["file_locations"]
data_path = files["db_data"]

# Define database location and connection and metadata paths
dbconnect = dbutils.dbnascentConnection(
    files["database"],
    files["credentials"]
)

exptmeta_path = (
    str(files["db_data"])
    + str(paper_id)
    + "/metadata/expt_metadata.txt"
)
sampmeta_path = (
    str(files["db_data"])
    + str(paper_id)
    + "/metadata/sample_metadata.txt"
)

# Raise error if paper identifier is not valid
if not exists(exptmeta_path):
    sys.exit(("Paper metadata not present for " + paper_id))

# Back up entire database (optional)
# Should not use when building whole database
# May be useful for adding papers individually later
backupdir = files["backup_dir"]
# dbconnect.backup(backupdir, False)

### Step 2: Parse paper and sample metadata tables ###

# Read in keys, fix keys for each table
# (some keys in metatables don't match db table fields and 
#  some don't exist in metatables because they'll be calculated)
papers_keys = dbutils.load_keys(config,"papers",["organism_id","paper_qc_score","paper_nro_score"])
papers_keys["db"].remove("organism")
samples_keys = dbutils.load_keys(config,"metatable_samples","samples")
genetics_keys = dbutils.load_keys(config,"genetics",["organism_id","tissue_id"])
conditions_keys = dbutils.load_keys(config,"metatable_conditions","conditions")
bidirs_keys = dbutils.load_keys(config,"bidirs")
nascentflow_keys = dbutils.load_keys(config,"nascentflow")
bidirflow_keys = dbutils.load_keys(config,"bidirflow")

# Read in files
exptmeta = dbutils.Metatable(exptmeta_path)
sampmeta = dbutils.Metatable(sampmeta_path)

# Combine tables and replace metatable keys with db keys where necessary
sampmeta.key_replace(samples_keys["in"], samples_keys["match"])
for sample in sampmeta.data:
    sample.update(exptmeta.data[0])
    if not sample["sample_name"]:
        sample["sample_name"] = sample["srr"]

sampmeta.key_replace(papers_keys["in"], papers_keys["match"])
sampmeta.key_replace(genetics_keys["in"], genetics_keys["match"])

### Step 3: Load in organism and tissue info already in database ###

# Parse organisms from db tables and add ids
# Replace the 'organism' field with the correct database organism_id
org_dump = dbconnect.reflect_table("organisms")
org_dump = dbutils.format_for_db_add(dbconnect,org_dump)
sampmeta.data = dbutils.format_for_db_add(dbconnect,sampmeta.data)
sampmeta = dbutils.bulk_key_store_compare(
    sampmeta,
    org_dump,
    ["organism"],
    ["id"],
    ["organism_id"]
)

# Parse tissues from db tables and add ids
# Load tissue table keys and external location
# (database doesn't store any identifying cell line/organism info in 
#  that table, but it is in the original tissue table that was loaded in)
tissues_keys = dbutils.load_keys(config,"tissues",["organism","sample_type","cell_type"])
tissuetype_path = files["tissue_table"]

# Read in tissue tables and make sure entries are unique
# These keys are a little different than the others in that the "db" keys
# are actually the keys from the external tissue table and the "match" keys
# are the keys that are stored in the database
tissues_dump = dbconnect.reflect_table("tissues")
tissues_dump = dbutils.format_for_db_add(dbconnect,tissues_dump)
tissues = dbutils.Metatable(tissuetype_path)
tissues.key_replace(tissues_keys["in"], tissues_keys["match"])
tissues.data = dbutils.format_for_db_add(dbconnect,tissues.data)
tissues_unique = tissues.unique(tissues_keys["db"])

# Add tissue info to main dict and match to db tissues
# Replace the 'tissue' field with the correct database tissue_id
sampmeta = dbutils.bulk_key_store_compare(
    sampmeta,
    tissues_unique,
    ["organism","sample_type","cell_type"],
    tissues_keys["match"],
)

sampmeta = dbutils.bulk_key_store_compare(
    sampmeta,
    tissues_dump,
    tissues_keys["match"],
    ["id"],
    ["tissue_id"],
)


### Step 4: Add bidir summary data, if present ###

tfit_path = data_path + str(paper_id) + "/bidir_summary/tfit_stats.txt"
dreg_path = data_path + str(paper_id) + "/bidir_summary/dreg_stats.txt"

# Read in master merge list files and make paper_id lists
# for tfit and dreg separately
dreg_merge_files = [
    files["hg38_tfit_master_merge"],
    files["mm10_tfit_master_merge"],
]
tfit_merge_files = [
    files["hg38_dreg_master_merge"],
    files["mm10_dreg_master_merge"],
]
dreg_merge_ids = dbutils.merge_list_accum(dreg_merge_files)
tfit_merge_ids = dbutils.merge_list_accum(tfit_merge_files)

# Add bidir data, if available, otherwise add nulls
sampmeta = dbutils.add_bidir_info(
    sampmeta,
    tfit_path,
    "tfit",
    tfit_merge_ids,
    bidirs_keys["db"],
)
sampmeta = dbutils.add_bidir_info(
    sampmeta,
    dreg_path,
    "dreg",
    dreg_merge_ids,
    bidirs_keys["db"],
)

bidirs_unique = sampmeta.unique(bidirs_keys["db"])


### Step 5: Calculate all sample-related fields and prep for db addition ###

# Define thresholds for qc and data scores
# QC scores: four positions corresponding to:
# [trim read depth, duplication, (mapped*trim read depth), complexity]
# NRO scores: two positions corresponding to:
# [exon intron ratio, tfit call GC proportion]
samp_thresholds = {
    "qc5": [5000000, 0.95, 4000000, 0.05],
    "qc4": [10000000, 0.80, 8000000, 0.2],
    "qc3": [15000000, 0.65, 12000000, 0.35],
    "qc2": [20000000, 0.5, 16000000, 0.5],
    "nro5": [9, 0.40],
    "nro4": [7, 0.43],
    "nro3": [5, 0.47],
    "nro2": [3, 0.5]
}

# Scrape all QC data for each sample and add to sample dict
for sample in sampmeta.data:
    qc_dict = dbutils.scrape_all_qc(
        sample,
        data_path,
    )
    sample.update(qc_dict)
    # Calculate qc and data scores
    # Uses bidir summary stats for data scores if available
    score_dict = dbutils.sample_qc_calc(sample, samp_thresholds)
    sample.update(score_dict)
    # Parse replicate number
    rep_num = re.split(r"(\d+)", sample["replicate"])
    sample["replicate"] = rep_num[1]

paper_scores = dbutils.paper_qc_calc(sampmeta.data)

sampmeta.data = dbutils.format_for_db_add(dbconnect,sampmeta.data)

### Step 6: Add samples, papers, genetics, bidirs to database ###

# If not present, add sample info to database
try:
    samples_unique = sampmeta.unique(samples_keys["db"])
except KeyError:
    print(paper_id)

samples_to_add = dbutils.entry_update(
    dbconnect, "samples", samples_keys["db"], samples_unique
)
if len(samples_to_add) > 0:
    samples_to_add = dbutils.format_for_db_add(dbconnect,samples_to_add)
    dbconnect.engine.execute(dborm.samples.__table__.insert(), samples_to_add)

# If not present, add paper info to database
# Add organism id to match keys 
# (would normally use db keys but need to exclude paper qc/nro score fields 
# from comparison due to formatting int/float issues)
papers_keys["match"].append("organism_id")
papers_keys["match"].remove("organism")
papers_unique = sampmeta.unique(papers_keys["match"])

# Add paper scores and change datatypes for unique calc/db addition
for paper in papers_unique:
    paper["paper_qc_score"] = paper_scores["paper_qc_score"]
    paper["paper_nro_score"] = paper_scores["paper_nro_score"]

papers_to_add = dbutils.entry_update(
    dbconnect, "papers", papers_keys["match"], papers_unique
)
if len(papers_to_add) > 0:
    papers_to_add = dbutils.format_for_db_add(dbconnect,papers_to_add)
    dbconnect.engine.execute(dborm.papers.__table__.insert(), papers_to_add)

# If not present, add genetic info to database
genetics_unique = sampmeta.unique(genetics_keys["db"])
gene_to_add = dbutils.entry_update(
    dbconnect, "genetics", genetics_keys["db"], genetics_unique
)
if len(gene_to_add) > 0:
    gene_to_add = dbutils.format_for_db_add(dbconnect,gene_to_add)
    dbconnect.engine.execute(dborm.genetics.__table__.insert(), gene_to_add)

# If not present, add bidir info to database
bidirs_to_add = dbutils.entry_update(
    dbconnect, "bidirs", bidirs_keys["db"], bidirs_unique
)
if len(bidirs_to_add) > 0:
    bidirs_to_add = dbutils.format_for_db_add(dbconnect,bidirs_to_add)
    dbconnect.engine.execute(dborm.bidirs.__table__.insert(), bidirs_to_add)


### Step 7: Make sampleEquiv and linkIDs entries and add to database ###

# Connect to database again to refresh data and define fields
dbconnect = dbutils.dbnascentConnection(
    files["database"],
    files["credentials"]
)

equiv_keys = ["sample_id", "srr"]
link_keys = ["sample_id", "paper_id", "genetic_id", "bidir_id"]

# Scrape tables
samples_dump = dbutils.format_for_db_add(
    dbconnect,
    dbconnect.reflect_table("samples"),
)
papers_dump = dbutils.format_for_db_add(
    dbconnect,
    dbconnect.reflect_table("papers"),
)
genetics_dump = dbutils.format_for_db_add(
    dbconnect,
    dbconnect.reflect_table("genetics"),
)
bidirs_dump = dbutils.format_for_db_add(
    dbconnect,
    dbconnect.reflect_table("bidirs"),
)

# Link ids
sampmeta.data = dbutils.format_for_db_add(dbconnect,sampmeta.data)
sampmeta = dbutils.bulk_key_store_compare(
    sampmeta,
    samples_dump,
    samples_keys["db"],
    ["id"],
    ["sample_id"],
)
sampmeta = dbutils.bulk_key_store_compare(
    sampmeta,
    papers_dump,
    papers_keys["match"],
    ["id"],
    ["paper_id"],
)
sampmeta = dbutils.bulk_key_store_compare(
    sampmeta,
    genetics_dump,
    genetics_keys["db"],
    ["id"],
    ["genetic_id"],
)
sampmeta = dbutils.bulk_key_store_compare(
    sampmeta,
    bidirs_dump,
    bidirs_keys["db"],
    ["id"],
    ["bidir_id"],
)

# If not already present, add sampleEquiv entries to database
sampleequiv_unique = sampmeta.unique(equiv_keys)
if paper_id == "Hah2013enhancer":
    sampleequiv_unique = []
    equivs = {
        "SRR653421": ["SRR497904","SRR497905","SRR497906"],
        "SRR653422": ["SRR497907","SRR497908","SRR497909","SRR497910"],
        "SRR653423": ["SRR497911"],
        "SRR653424": ["SRR497912","SRR497913"],
        "SRR653425": ["SRR497914","SRR497915","SRR497916"],
        "SRR653426": ["SRR497917","SRR497918","SRR497919","SRR497920"],
    }
    for entry in sampmeta.data:
        if entry["srr"] in equivs.keys():
            sampleequiv_unique.append(
                {"sample_id": entry["sample_id"], "srr": entry["srr"]}
            )
            for val in equivs[entry["srr"]]:
                sampleequiv_unique.append(
                    {"sample_id": entry["sample_id"], "srr": val}
                )
sampleequiv_to_add = dbutils.entry_update(
    dbconnect, "sampleEquiv", equiv_keys, sampleequiv_unique
)
if len(sampleequiv_to_add) > 0:
    sampleequiv_to_add = dbutils.format_for_db_add(dbconnect,sampleequiv_to_add)
    dbconnect.engine.execute(dborm.sampleEquiv.__table__.insert(), sampleequiv_to_add)

# If not already present, add linked IDs to database
link_unique = sampmeta.unique(link_keys)
link_to_add = dbutils.entry_update(dbconnect, "linkIDs", link_keys, link_unique)
if len(link_to_add) > 0:
    link_to_add = dbutils.format_for_db_add(dbconnect,link_to_add)
    dbconnect.engine.execute(dborm.linkIDs.__table__.insert(), link_to_add)


### Step 8: Add condition info to database ###

# Extract condition data
sampmeta.key_replace(conditions_keys["in"], conditions_keys["match"])

# Parse metadata strings and store values with db keys
cond_table = dbutils.condition_processing(sampmeta.data)
for cond in cond_table:
    for key in cond.keys():
        cond[key] = str(cond[key])

# Extract unique conditions and store integer blanks correctly
conds = dbutils.Metatable(cond_table)
conds_unique = conds.unique(conditions_keys["db"])

# If not present, add condition metadata to database
cond_to_add = dbutils.entry_update(
    dbconnect, "conditions", conditions_keys["db"], conds_unique
)
if len(cond_to_add) > 0:
    cond_to_add = dbutils.format_for_db_add(dbconnect,cond_to_add)
    dbconnect.engine.execute(dborm.conditions.__table__.insert(), cond_to_add)


### Step 9: Add nascentflow/bidirflow version data ###

# Parse version yamls and set entries as strings for unique calc
nf_table = dbutils.add_version_info(sampmeta.data, data_path, "nascent", nascentflow_keys["db"])
for nf in nf_table:
    for key in nf.keys():
        nf[key] = str(nf[key])

bf_table = dbutils.add_version_info(sampmeta.data, data_path, "bidir", bidirflow_keys["db"])
for bf in bf_table:
    for key in bf.keys():
        bf[key] = str(bf[key])

# If not present, add nascentflow version info to database
nf_vers = dbutils.Metatable(nf_table)
nf_unique = nf_vers.unique(nascentflow_keys["db"])
nf_to_add = dbutils.entry_update(dbconnect, "nascentflowRuns", nascentflow_keys["db"], nf_unique)
if len(nf_to_add) > 0:
    nf_to_add = dbutils.format_for_db_add(dbconnect,nf_to_add)
    dbconnect.engine.execute(dborm.nascentflowRuns.__table__.insert(), nf_to_add)

# If not present, add bidirflow version info to database
bf_vers = dbutils.Metatable(bf_table)
bf_unique = bf_vers.unique(bidirflow_keys["db"])
bf_to_add = dbutils.entry_update(dbconnect, "bidirflowRuns", bidirflow_keys["db"], bf_unique)
if len(bf_to_add) > 0:
    bf_to_add = dbutils.format_for_db_add(dbconnect,bf_to_add)
    dbconnect.engine.execute(dborm.bidirflowRuns.__table__.insert(), bf_to_add)


### Step 10: Make match tables ###

# Connect to database again to refresh data and define fields
dbconnect = dbutils.dbnascentConnection(
    files["database"],
    files["credentials"]
)

# Make new key lists for database info dump
condlink_keys = dbutils.load_keys(config,"conditions")
condlink_keys["db"] = ["sample_id","condition_id"]
nflink_keys = dbutils.load_keys(config,"nascentflow")
nflink_keys["db"] = ["sample_id","nascentflow_id"]
bflink_keys = dbutils.load_keys(config,"bidirflow")
bflink_keys["db"] = ["sample_id","bidirflow_id"]

# Grab condition and version id info from database
cond_dump = dbconnect.reflect_table("conditions")
cond_dump = dbutils.format_for_db_add(dbconnect,cond_dump)
nf_dump = dbconnect.reflect_table("nascentflowRuns")
nf_dump = dbutils.format_for_db_add(dbconnect,nf_dump)
bf_dump = dbconnect.reflect_table("bidirflowRuns")
bf_dump = dbutils.format_for_db_add(dbconnect,bf_dump)

conds.data = dbutils.format_for_db_add(dbconnect,conds.data)
conds = dbutils.bulk_key_store_compare(
    conds,
    cond_dump,
    condlink_keys["match"],
    ["id"],
    ["condition_id"],
)
nf_vers.data = dbutils.format_for_db_add(dbconnect,nf_vers.data)
nf_vers = dbutils.bulk_key_store_compare(
    nf_vers,
    nf_dump,
    nflink_keys["match"],
    ["id"],
    ["nascentflow_id"],
)
bf_vers.data = dbutils.format_for_db_add(dbconnect,bf_vers.data)
bf_vers = dbutils.bulk_key_store_compare(
    bf_vers,
    bf_dump,
    bflink_keys["match"],
    ["id"],
    ["bidirflow_id"],
)

# If not present, add sample/condition ids to database
sampcond_unique = conds.unique(condlink_keys["db"])
sampcond_to_add = dbutils.entry_update(
    dbconnect, "conditionLink", condlink_keys["db"], sampcond_unique
)
if len(sampcond_to_add) > 0:
    dbconnect.engine.execute(dborm.conditionLink.__table__.insert(), sampcond_to_add)

# If not present, add sample and nascentflow ids to database
sampnf_unique = nf_vers.unique(nflink_keys["db"])
sampnf_to_add = dbutils.entry_update(
    dbconnect, "nascentflowLink", nflink_keys["db"], sampnf_unique
)
if len(sampnf_to_add) > 0:
    dbconnect.engine.execute(dborm.nascentflowLink.__table__.insert(), sampnf_to_add)

# If not present, add sample and bidirflow ids to database
sampbf_unique = bf_vers.unique(bflink_keys["db"])
sampbf_to_add = dbutils.entry_update(
    dbconnect, "bidirflowLink", bflink_keys["db"], sampbf_unique
)
if len(sampbf_to_add) > 0:
    dbconnect.engine.execute(dborm.bidirflowLink.__table__.insert(), sampbf_to_add)

# db_paper_add_update.py ends here
