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
# Step 3: Add bidir summary data, if present
# Step 4: If not present, add experimental metadata to database
# Step 5: If not present, add sample ids to database
# Step 6: If not present, add genetic info to database
# Step 7: If not present, add bidir summary info to database
# Step 8: Make linkIDs table and add to database
# Step 9: Add condition info to database
# Step 10: Make condition match table
# Step 11: Make sampleAccum table
# Step 12: Calculate and add paper qc/data scores
# Step 13: Add nascentflow/bidirflow version data
# Step 14: Connect version data to samples ###

# Code:

# Import
import dbutils
import dborm
from os.path import exists
import re
import sys

### Step 1: Define paths and database connection ###

# Raise error if no argument given
if len(sys.argv) < 2:
    raise NameError("No paper identifier given")
else:
    paper_id = sys.argv[1]

# Load config file
config = dbutils.load_config(
    "/scratch/Shares/dowell/dbnascent/DBNascent-build/config_build.txt"
)

# Define database location and connection and metadata paths
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]

dbconnect = dbutils.dbnascentConnection(db_url, creds)

exptmeta_path = (
    str(config["file_locations"]["db_data"])
    + str(paper_id)
    + "/metadata/expt_metadata.txt"
)
sampmeta_path = (
    str(config["file_locations"]["db_data"])
    + str(paper_id)
    + "/metadata/sample_metadata.txt"
)

# Raise error if paper identifier is not valid
if not exists(exptmeta_path):
    sys.exit(("Paper metadata not present for " + paper_id))

# Back up entire database (optional)
# Should not use when building whole database
# May be useful for adding papers individually later
backupdir = config["file_locations"]["backup_dir"]
# dbconnect.backup(backupdir, False)

### Step 2: Parse paper and sample metadata tables ###

# Load keys
expt_keys = list(dict(config["expt keys"]).values())
dbexpt_keys = list(dict(config["expt keys"]).keys())
samp_keys = list(dict(config["sample keys"]).values())
dbsamp_keys = list(dict(config["sample keys"]).keys())
genetic_keys = list(dict(config["genetic keys"]).values())
dbgenetic_keys = list(dict(config["genetic keys"]).keys())

# Read in files and make full metadata table
exptmeta = dbutils.Metatable(exptmeta_path)
sampmeta = dbutils.Metatable(sampmeta_path)

for sample in sampmeta.data:
    sample.update(exptmeta.data[0])
    if not sample["srz"]:
        sample["srz"] = sample["srr"]

# Replace text file keys with database keys and find unique entries
sampmeta.key_replace(samp_keys, dbsamp_keys)
sampmeta.key_replace(expt_keys, dbexpt_keys)
sampmeta.key_replace(genetic_keys, dbgenetic_keys)

expt_unique = sampmeta.unique(dbexpt_keys)
samp_unique = sampmeta.unique(dbsamp_keys)
gene_unique = sampmeta.unique(dbgenetic_keys)

### Step 3: Add bidir summary data, if present

data_path = config["file_locations"]["db_data"]
bidirsummary_keys = list(dict(config["bidirsummary keys"]).values())
dbbidirsummary_keys = list(dict(config["bidirsummary keys"]).keys())
tfitsummary_path = data_path + str(paper_id) + "/bidir_summary/tfit_stats.txt"
dregsummary_path = data_path + str(paper_id) + "/bidir_summary/dreg_stats.txt"

# Add Tfit bidir data, if available, otherwise add nulls
if exists(tfitsummary_path):
    tfit_summary = dbutils.Metatable(tfitsummary_path)
else:
    tfit_summary = dbutils.Metatable([])

for sample in sampmeta.data:
    dbutils.key_store_compare(
        sample,
        tfit_summary.data,
        [
            "sample_name",
        ],
        [
            "num_tfit_bidir",
            "num_tfit_bidir_promoter",
            "num_tfit_bidir_intronic",
            "num_tfit_bidir_intergenic",
            "tfit_bidir_gc_prop",
        ],
        addnull=True,
    )
    # Format proportion correctly
    if sample["tfit_bidir_gc_prop"]:
        sample["tfit_bidir_gc_prop"] = str(float(sample["tfit_bidir_gc_prop"]))

# Add dREG bidir data, if available, otherwise add nulls
if exists(dregsummary_path):
    dreg_summary = dbutils.Metatable(dregsummary_path)
else:
    dreg_summary = dbutils.Metatable([])

for sample in sampmeta.data:
    dbutils.key_store_compare(
        sample,
        dreg_summary.data,
        [
            "sample_name",
        ],
        [
            "num_dreg_bidir",
            "num_dreg_bidir_promoter",
            "num_dreg_bidir_intronic",
            "num_dreg_bidir_intergenic",
            "dreg_bidir_gc_prop",
        ],
        addnull=True,
    )
    # Format proportion correctly
    if sample["dreg_bidir_gc_prop"]:
        sample["dreg_bidir_gc_prop"] = str(float(sample["dreg_bidir_gc_prop"]))

# Read in master merge list files and make paper_id lists
# for tfit and dreg separately
dreg_merge_files = [
    config["file_locations"]["hg38_tfit_master_merge"],
    config["file_locations"]["mm10_tfit_master_merge"],
]
tfit_merge_files = [
    config["file_locations"]["hg38_dreg_master_merge"],
    config["file_locations"]["mm10_dreg_master_merge"],
]
dreg_merge_ids = dbutils.merge_list_accum(dreg_merge_files)
tfit_merge_ids = dbutils.merge_list_accum(tfit_merge_files)

# Add master merge info for all entries in paper (strings for unique comp)
for sample in sampmeta.data:
    if paper_id in dreg_merge_ids:
        sample["dreg_master_merge_incl"] = "1"
    else:
        sample["dreg_master_merge_incl"] = "0"
    if paper_id in tfit_merge_ids:
        sample["tfit_master_merge_incl"] = "1"
    else:
        sample["tfit_master_merge_incl"] = "0"
    for bidirkey in dbbidirsummary_keys:
        sample[bidirkey] = str(sample[bidirkey])

bidirsum_unique = sampmeta.unique(dbbidirsummary_keys)

### Step 4: If not present, add experimental metadata to database ###
expt_to_add = dbutils.entry_update(dbconnect, "exptMetadata", dbexpt_keys, expt_unique)
for expt in expt_to_add:  # Replace text 1/0 with boolean values
    for key in expt:
        if expt[key] == "1":
            expt[key] = True
        elif expt[key] == "0":
            expt[key] = False
if len(expt_to_add) > 0:
    dbconnect.engine.execute(dborm.exptMetadata.__table__.insert(), expt_to_add)

### Step 5: If not present, add sample ids to database ###

# Find max sample_id currently in db
dbsamp_dump = dbconnect.reflect_table("sampleID")
dbsamp = dbutils.Metatable(dbsamp_dump)
curr_id = 0
for dbsample in dbsamp.data:
    if dbsample["sample_id"] > curr_id:
        curr_id = dbsample["sample_id"]

# If not already present, add samples to database with new ids
samp_to_add = dbutils.entry_update(dbconnect, "sampleID", dbsamp_keys, samp_unique)
if len(samp_to_add) > 0:
    # Make hash of unique samples (srz OR srr if no srz) to id values
    samp_for_hash = dbutils.Metatable(samp_to_add)
    samp_id_hash = samp_for_hash.unique(["sample_name"])
    for idhash in samp_id_hash:
        curr_id = curr_id + 1
        idhash["sample_id"] = curr_id

    # Add sample id value to all srrs in a sample name based on hash
    for sample in samp_to_add:
        hash_entry = list(
            filter(
                lambda samp_id_hash: samp_id_hash["sample_name"]
                == sample["sample_name"],
                samp_id_hash,
            )
        )[0]
        sample["sample_id"] = hash_entry["sample_id"]

    # Add to database
    dbconnect.engine.execute(dborm.sampleID.__table__.insert(), samp_to_add)

### Step 6: If not present, add genetic info to database ###

# Grab tissue tables for matching tissuedetail_id
dbtissuetype_keys = list(dict(config["tissue type keys"]).keys())
tissuetype_path = config["file_locations"]["tissue_table"]
tissuetypes = dbutils.Metatable(tissuetype_path)
dbtiss_dump = dbconnect.reflect_table("tissueDetails")

# Add tissuedetail_id to keys for geneticInfo
dbgenetic_keys.append("tissuedetail_id")

# Do serial comparison to connect genetic info to tissue id
for gen in gene_unique:
    dbutils.key_store_compare(
        gen,
        tissuetypes.data,
        ["organism","sample_type","cell_type",],
        ["tissue", "cell_origin_type", "tissue_description", "disease",],
        addnull=True,
    )
    dbutils.key_store_compare(
        gen,
        dbtiss_dump,
        ["tissue", "cell_origin_type", "tissue_description", "disease",],   
        ["tissuedetail_id"],
        addnull=True,
    )

# Add genetic info to database
gene_to_add = dbutils.entry_update(
    dbconnect, "geneticInfo", dbgenetic_keys, gene_unique
)
if len(gene_to_add) > 0:
    dbconnect.engine.execute(dborm.geneticInfo.__table__.insert(), gene_to_add)

# Regenerate original dbgenetic_keys for future step
dbgenetic_keys = list(dict(config["genetic keys"]).keys())

### Step 7: If not present, add bidir summary info to database ###

bidirsum_to_add = dbutils.entry_update(
    dbconnect, "bidirSummary", dbbidirsummary_keys, bidirsum_unique
)
# Format values for input into db
for bidirsum in bidirsum_to_add:
    for key in ["tfit_master_merge_incl", "dreg_master_merge_incl"]:
        if bidirsum[key] == "1":
            bidirsum[key] = True
        elif bidirsum[key] == "0":
            bidirsum[key] = False
    for key in dbbidirsummary_keys:
        if bidirsum[key] == "None":
            bidirsum[key] = None
if len(bidirsum_to_add) > 0:
    dbconnect.engine.execute(dborm.bidirSummary.__table__.insert(), bidirsum_to_add)

### Step 8: Make linkIDs table and add to database ###

dbconnect = dbutils.dbnascentConnection(db_url, creds)
link_keys = [
    "sample_id",
    "genetic_id",
    "expt_id",
    "bidirsummary_id",
    "sample_name",
    "paper_id",
]

# Reflect IDs currently in database
dbsamp_dump = dbconnect.reflect_table("sampleID")
dbexpt_dump = dbconnect.reflect_table("exptMetadata")
dbgene_dump = dbconnect.reflect_table("geneticInfo")
dbbidirsum_dump = dbconnect.reflect_table("bidirSummary")

# Add IDs to main metatable for matching entries
for sample in sampmeta.data:
    dbutils.key_store_compare(sample, dbsamp_dump, dbsamp_keys, ["sample_id"])
    dbutils.key_store_compare(sample, dbexpt_dump, dbexpt_keys, ["expt_id"])
    dbutils.key_store_compare(sample, dbgene_dump, dbgenetic_keys, ["genetic_id"])
    dbutils.key_store_compare(
        sample, dbbidirsum_dump, dbbidirsummary_keys, ["bidirsummary_id"]
    )

# If not already present, add linked IDs to database
link_unique = sampmeta.unique(link_keys)
link_to_add = dbutils.entry_update(dbconnect, "linkIDs", link_keys, link_unique)
if len(link_to_add) > 0:
    dbconnect.engine.execute(dborm.linkIDs.__table__.insert(), link_to_add)

### Step 9: Add condition info to database ###

dbconnect = dbutils.dbnascentConnection(db_url, creds)
# cond_keys are as written in metadata, preparsing
# cond_full_keys are all keys in database, postparsing
cond_keys = list(dict(config["metatable condition keys"]).values())
dbcond_keys = list(dict(config["metatable condition keys"]).keys())
dbcond_keys.append("sample_name")
cond_full_keys = list(dict(config["condition keys"]).values())
dbcond_full_keys = list(dict(config["condition keys"]).keys())

sampmeta.key_replace(cond_keys, dbcond_keys)
cond_preparse = sampmeta.key_grab(dbcond_keys)

# Parse metadata strings and store values with db keys
cond_parsed = []
for condition in cond_preparse:
    if condition["treatment"]:
        cond_types = condition["condition_type"].split(";")
        treatments = condition["treatment"].split(";")
        times = condition["times"].split(";")

        # Check if lengths are equivalent
        if (len(cond_types) != len(treatments)) or (len(cond_types) != len(times)):
            raise SyntaxError("Treatment parsing error: " + paper_id)

        for i in range(len(cond_types)):
            new_cond = dict()
            treatment = treatments[i].split("(")
            time = times[i].split(",")
            new_cond["sample_name"] = condition["sample_name"]
            new_cond["condition_type"] = cond_types[i]
            new_cond["treatment"] = treatment[0]
            if len(treatment) > 1:
                new_cond["conc_intens"] = treatment[1].split(")")[0]
            else:
                new_cond["conc_intens"] = ""

            if len(time) > 1:
                new_cond["start_time"] = int(time[0])
                new_cond["end_time"] = int(time[1])
                new_cond["time_unit"] = time[2]

                # Calculate duration and units
                duration_list = dbutils.duration_calc(time)
                new_cond["duration"] = int(duration_list[0])
                new_cond["duration_unit"] = duration_list[1]
            else:
                new_cond["start_time"] = ""
                new_cond["end_time"] = ""
                new_cond["time_unit"] = ""
                new_cond["duration"] = ""
                new_cond["duration_unit"] = ""

            cond_parsed.append(new_cond)

    # If no treatment, store empty values
    else:
        new_cond = dict()
        new_cond["sample_name"] = condition["sample_name"]
        new_cond["condition_type"] = "no treatment"
        new_cond["treatment"] = ""
        new_cond["conc_intens"] = ""
        new_cond["start_time"] = ""
        new_cond["end_time"] = ""
        new_cond["time_unit"] = ""
        new_cond["duration"] = ""
        new_cond["duration_unit"] = ""
        cond_parsed.append(new_cond)

# Extract unique conditions and store integer blanks correctly
cond = dbutils.Metatable(cond_parsed)
cond_unique = cond.unique(dbcond_full_keys)
for condition in cond_unique:
    if condition["start_time"] == "":
        condition["start_time"] = None
    if condition["end_time"] == "":
        condition["end_time"] = None
    if condition["duration"] == "":
        condition["duration"] = None

# If not present, add condition metadata to database
cond_to_add = dbutils.entry_update(
    dbconnect, "conditionInfo", dbcond_full_keys, cond_unique
)
if len(cond_to_add) > 0:
    for condition in cond_to_add:
        if condition["start_time"] == "None":
            condition["start_time"] = None
        if condition["end_time"] == "None":
            condition["end_time"] = None
        if condition["duration"] == "None":
            condition["duration"] = None
    dbconnect.engine.execute(dborm.conditionInfo.__table__.insert(), cond_to_add)

### Step 10: Make condition match table ###

dbconnect = dbutils.dbnascentConnection(db_url, creds)

# Make new key list for database info dump
dbcond_add_keys = list(dict(config["condition keys"]).keys())
dbcond_add_keys.append("condition_id")

# Grab condition and sample id info from database
dbcond_dump = dbconnect.reflect_table("conditionInfo")
dbcond = dbutils.Metatable(dbcond_dump)
dbcond_data = dbcond.key_grab(dbcond_add_keys)
dbsamp_dump = dbconnect.reflect_table("sampleID")
dbsamp = dbutils.Metatable(dbsamp_dump)
name_id = dbsamp.unique(["sample_name", "sample_id"])

# Add sample ids to parsed condition table
for condition in cond_parsed:
    for sample in name_id:
        if condition["sample_name"] == sample["sample_name"]:
            condition["sample_id"] = sample["sample_id"]
    # Add condition_id to matching condition entries
    dbutils.key_store_compare(
        condition, dbcond_data, dbcond_full_keys, ["condition_id"]
    )

# If not present, add sample/condition ids to database
sampcond = dbutils.Metatable(cond_parsed)
sampcond_unique = sampcond.unique(["sample_id", "condition_id"])
sampcond_to_add = dbutils.entry_update(
    dbconnect, "sampleCondition", ["sample_id", "condition_id"], sampcond_unique
)
if len(sampcond_to_add) > 0:
    dbconnect.engine.execute(dborm.sampleCondition.__table__.insert(), sampcond_to_add)

### Step 11: Make sampleAccum table ###

dbconnect = dbutils.dbnascentConnection(db_url, creds)

# Load data dir and keys, then replace keys in metadata table
data_path = config["file_locations"]["db_data"]
accum_keys = list(dict(config["metatable accum keys"]).values())
dbaccum_keys = list(dict(config["metatable accum keys"]).keys())
dbaccum_full_keys = list(dict(config["accum keys"]).keys())
sampmeta.key_replace(accum_keys, dbaccum_keys)

# Scrape all QC data for each sample and add to sample dict
for sample in sampmeta.data:
    fastqc_dict = dbutils.scrape_fastqc(
        sample["paper_id"], sample["sample_name"], data_path, sample
    )
    picard_dict = dbutils.scrape_picard(
        sample["paper_id"], sample["sample_name"], data_path
    )
    mapstats_dict = dbutils.scrape_mapstats(
        sample["paper_id"], sample["sample_name"], data_path, sample
    )
    rseqc_dict = dbutils.scrape_rseqc(
        sample["paper_id"], sample["sample_name"], data_path
    )
    preseq_dict = dbutils.scrape_preseq(
        sample["paper_id"], sample["sample_name"], data_path
    )
    pileup_dict = dbutils.scrape_pileup(
        sample["paper_id"], sample["sample_name"], data_path
    )

    qc_dict = {
        **fastqc_dict,
        **picard_dict,
        **mapstats_dict,
        **rseqc_dict,
        **preseq_dict,
        **pileup_dict,
    }
    sample.update(qc_dict)

    # Define thresholds for qc and data scores
    # QC scores: four positions corresponding to:
    # [trim read depth, duplication, (mapped*trim read depth), complexity)]

    # Data scores: two positions corresponding to:
    # [exon intron ratio, tfit call GC proportion]

    samp_thresholds = {
        "qc5": [5000000, 0.95, 4000000, 0.05],
        "qc4": [10000000, 0.80, 8000000, 0.2],
        "qc3": [15000000, 0.65, 12000000, 0.35],
        "qc2": [20000000, 0.5, 16000000, 0.5],
        "data5": [9, 0.40],
        "data4": [7, 0.43],
        "data3": [5, 0.47],
        "data2": [3, 0.5],
    }

    # Calculate qc and data scores
    # Uses bidir summary stats for data scores if available
    score_dict = dbutils.sample_qc_calc(dbconnect, sample, samp_thresholds)
    sample.update(score_dict)

    # Parse replicate number
    rep_num = re.split(r"(\d+)", sample["replicate"])
    sample["replicate"] = rep_num[1]

# Change datatypes for unique calc
for sample in sampmeta.data:
    for key in sample:
        sample[key] = str(sample[key])

# If not present, add sample accum info to database
try:
    accum_unique = sampmeta.unique(dbaccum_full_keys)
except KeyError:
    print(paper_id)
accum_to_add = dbutils.entry_update(
    dbconnect, "sampleAccum", dbaccum_full_keys, accum_unique
)
if len(accum_to_add) > 0:
    for accsamp in accum_to_add:
        for key in ["rcomp", "unusable", "timecourse", "outlier"]:
            if accsamp[key] == "1":
                accsamp[key] = True
            elif accsamp[key] == "0":
                accsamp[key] = False
        for key in accsamp:
            if accsamp[key] == "None":
                accsamp[key] = None
    dbconnect.engine.execute(dborm.sampleAccum.__table__.insert(), accum_to_add)

### Step 12: Calculate and add paper qc/data scores ###
dbconnect = dbutils.dbnascentConnection(db_url, creds)
paper_scores = dbutils.paper_qc_calc(accum_unique)
dbconnect.engine.execute(
    dborm.exptMetadata.__table__.update().where(
        dborm.exptMetadata.__table__.c.paper_id == paper_id
    ),
    paper_scores,
)

### Step 13: Add nascentflow/bidirflow version data ###

dbconnect = dbutils.dbnascentConnection(db_url, creds)
nf_keys = list(dict(config["nascentflow keys"]).values())
dbnf_keys = list(dict(config["nascentflow keys"]).keys())
bf_keys = list(dict(config["bidirflow keys"]).values())
dbbf_keys = list(dict(config["bidirflow keys"]).keys())
dirpath = config["file_locations"]["db_data"]

# Parse version yamls and set entries as strings for unique calc
nftable = dbutils.add_version_info(dbconnect, paper_id, dirpath, "nascent", dbnf_keys)
for nfrun in nftable:
    for key in nfrun:
        nfrun[key] = str(nfrun[key])

bftable = dbutils.add_version_info(dbconnect, paper_id, dirpath, "bidir", dbbf_keys)
for bfrun in bftable:
    for key in bfrun:
        bfrun[key] = str(bfrun[key])

# If not present, add nascentflow version info to database
nf_vers = dbutils.Metatable(nftable)
nf_unique = nf_vers.unique(dbnf_keys)
nf_to_add = dbutils.entry_update(dbconnect, "nascentflowMetadata", dbnf_keys, nf_unique)
if len(nf_to_add) > 0:
    # Store null values correctly
    for nfrun in nf_to_add:
        for key in dbnf_keys:
            if nfrun[key] == "None":
                nfrun[key] = None
    dbconnect.engine.execute(dborm.nascentflowMetadata.__table__.insert(), nf_to_add)

# If not present, add bidirflow version info to database
bf_vers = dbutils.Metatable(bftable)
bf_unique = bf_vers.unique(dbbf_keys)
bf_to_add = dbutils.entry_update(dbconnect, "bidirflowMetadata", dbbf_keys, bf_unique)
if len(bf_to_add) > 0:
    # Store null values correctly
    for bfrun in bf_to_add:
        for key in dbbf_keys:
            if bfrun[key] == "None":
                bfrun[key] = None
    dbconnect.engine.execute(dborm.bidirflowMetadata.__table__.insert(), bf_to_add)

### Step 14: Connect version data to samples ###

dbconnect = dbutils.dbnascentConnection(db_url, creds)
dbnf_add_keys = list(dict(config["nascentflow keys"]).keys())
dbnf_add_keys.append("nascentflow_id")
dbbf_add_keys = list(dict(config["bidirflow keys"]).keys())
dbbf_add_keys.append("bidirflow_id")

# Extract current version data from database
dbnf_dump = dbconnect.reflect_table("nascentflowMetadata")
dbnf = dbutils.Metatable(dbnf_dump)
dbnf_data = dbnf.key_grab(dbnf_add_keys)
dbbf_dump = dbconnect.reflect_table("bidirflowMetadata")
dbbf = dbutils.Metatable(dbbf_dump)
dbbf_data = dbbf.key_grab(dbbf_add_keys)

# Add ids from version tables to data tables that have sample ids
for nfrun in nf_vers.data:
    dbutils.key_store_compare(nfrun, dbnf_data, dbnf_keys, ["nascentflow_id"])

for bfrun in bf_vers.data:
    dbutils.key_store_compare(bfrun, dbbf_data, dbbf_keys, ["bidirflow_id"])

# If not present, add sample and nascentflow ids to database
sampnf_unique = nf_vers.unique(["sample_id", "nascentflow_id"])
sampnf_to_add = dbutils.entry_update(
    dbconnect, "sampleNascentflow", ["sample_id", "nascentflow_id"], sampnf_unique
)
if len(sampnf_to_add) > 0:
    dbconnect.engine.execute(dborm.sampleNascentflow.__table__.insert(), sampnf_to_add)

# If not present, add sample and bidirflow ids to database
sampbf_unique = bf_vers.unique(["sample_id", "bidirflow_id"])
sampbf_to_add = dbutils.entry_update(
    dbconnect, "sampleBidirflow", ["sample_id", "bidirflow_id"], sampbf_unique
)
if len(sampbf_to_add) > 0:
    dbconnect.engine.execute(dborm.sampleBidirflow.__table__.insert(), sampbf_to_add)

# db_paper_add_update.py ends here
