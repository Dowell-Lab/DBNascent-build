#!/usr/bin/env python
# coding: utf-8

# Script for adding and updating DBNascent values

# Import
import dbutils
import dborm
import datetime
import re
import sys

if len(sys.argv) < 2:
    raise Error("No paper identifier given")
else:
    paper_id = sys.argv[1]

# Load config file
config = dbutils.load_config(
    "/scratch/Shares/dowell/dbnascent/DBNascent-build/config.txt")

# Define database location and (optionally) back up database
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]
backupdir = config["file_locations"]["backup_dir"]

dbconnect = dbutils.dbnascentConnection(db_url, creds)
# dbconnect.backup(backupdir, False)

# Add/update organism table
org_keys = list(dict(config["organism keys"]).values())
dborg_keys = list(dict(config["organism keys"]).keys())
orgtable_path = config["file_locations"]["organism_table"]

# Read in organism table and make sure entries are unique
orgs = dbutils.Metatable(orgtable_path)
orgs.key_replace(org_keys, dborg_keys)
orgs_unique = orgs.unique(dborg_keys)

# If not already present, add data to database
orgs_to_add = dbutils.entry_update(dbconnect,
                                   "organismInfo",
                                   dborg_keys,
                                   orgs_unique)
if len(orgs_to_add) > 0:
    dbconnect.engine.execute(
        dborm.organismInfo.__table__.insert(), orgs_to_add)

# Parse paper and sample metadata tables
dbconnect = dbutils.dbnascentConnection(db_url, creds)

expt_keys = list(dict(config["expt keys"]).values())
dbexpt_keys = list(dict(config["expt keys"]).keys())
samp_keys = list(dict(config["sample keys"]).values())
dbsamp_keys = list(dict(config["sample keys"]).keys())
genetic_keys = list(dict(config["genetic keys"]).values())
dbgenetic_keys = list(dict(config["genetic keys"]).keys())

exptmeta_path = (str(config["file_locations"]["db_data"]) + str(paper_id) +
                 "/metadata/expt_metadata.txt")
sampmeta_path = (str(config["file_locations"]["db_data"]) + str(paper_id) +
                 "/metadata/sample_metadata.txt")

# Read in experimental metadata
expt = dbutils.Metatable(exptmeta_path)

# Read in sample metadata and append experimental for whole metadata table
samp = dbutils.Metatable(sampmeta_path)
for entry in samp.data:
    entry.update(expt.data[0])
    if not entry["srz"]:
        entry["srz"] = entry["srr"]

# Replace text file keys with database keys and find unique entries
samp.key_replace(samp_keys, dbsamp_keys)
samp.key_replace(expt_keys, dbexpt_keys)
samp.key_replace(genetic_keys, dbgenetic_keys)

expt_unique = samp.unique(dbexpt_keys)
samp_unique = samp.unique(dbsamp_keys)
gene_unique = samp.unique(dbgenetic_keys)

# If not already present, add experimental metadata to database
expt_to_add = dbutils.entry_update(dbconnect,
                                   "exptMetadata",
                                   dbexpt_keys,
                                   expt_unique)
for entry in expt_to_add:
    for key in entry:
        if entry[key] == '1':
            entry[key] = True
        elif entry[key] == '0':
            entry[key] = False
if len(expt_to_add) > 0:
    dbconnect.engine.execute(
        dborm.exptMetadata.__table__.insert(), expt_to_add)

# Find max sample_id currently in db
dbsamp_dump = dbconnect.reflect_table("sampleID")
dbsamp = dbutils.Metatable(meta_path=None, dictlist=dbsamp_dump)
curr_id = 0
for entry in dbsamp.data:
    if entry["sample_id"] > curr_id:
        curr_id = entry["sample_id"]

# If not already present, add samples to database with new ids
samp_to_add = dbutils.entry_update(dbconnect,
                                   "sampleID",
                                   dbsamp_keys,
                                   samp_unique)
if len(samp_to_add) > 0:
    samps_meta = dbutils.Metatable(meta_path=None, dictlist=samp_to_add)
    samp_id_hash = samps_meta.unique(["sample_name"])
    for entry in samp_id_hash:
        curr_id = curr_id + 1
        entry["sample_id"] = curr_id

    for entry in samp_to_add:
        hash_entry = list(filter(lambda samp_id_hash: samp_id_hash["sample_name"]
                                 == entry["sample_name"], samp_id_hash))[0]
        entry["sample_id"] = hash_entry["sample_id"]

    dbconnect.engine.execute(
        dborm.sampleID.__table__.insert(), samp_to_add)

# If not already present, add genetic info to database
gene_to_add = dbutils.entry_update(dbconnect,
                                   "geneticInfo",
                                   dbgenetic_keys,
                                   gene_unique)
if len(gene_to_add) > 0:
    dbconnect.engine.execute(
        dborm.geneticInfo.__table__.insert(), gene_to_add)

# Make linkIDs table
dbconnect = dbutils.dbnascentConnection(db_url, creds)
link_keys = ["sample_id", "genetic_id", "expt_id",
             "sample_name", "paper_id"]

# Reflect IDs currently in database
dbsamp_dump = dbconnect.reflect_table("sampleID")
dbexpt_dump = dbconnect.reflect_table("exptMetadata")
dbgene_dump = dbconnect.reflect_table("geneticInfo")

# Add IDs for matching entries
for entry in samp.data:
    dbutils.key_store_compare(entry, dbsamp_dump,
                              dbsamp_keys, ["sample_id"])
    dbutils.key_store_compare(entry, dbexpt_dump,
                              dbexpt_keys, ["expt_id"])
    dbutils.key_store_compare(entry, dbgene_dump,
                              dbgenetic_keys, ["genetic_id"])

# If not already present, add linked IDs to database
link_unique = samp.unique(link_keys)
link_to_add = dbutils.entry_update(dbconnect,
                                   "linkIDs",
                                   link_keys,
                                   link_unique)
if len(link_to_add) > 0:
    dbconnect.engine.execute(
        dborm.linkIDs.__table__.insert(), link_to_add)

# Add condition info and build condition table
dbconnect = dbutils.dbnascentConnection(db_url, creds)
cond_keys = list(dict(config["metatable condition keys"]).values())
dbcond_keys = list(dict(config["metatable condition keys"]).keys())
dbcond_keys.append("sample_name")
cond_full_keys = list(dict(config["condition keys"]).values())
dbcond_full_keys = list(dict(config["condition keys"]).keys())

# Replace text file keys with database keys and separate conditions
samp.key_replace(cond_keys, dbcond_keys)
cond = samp.key_grab(dbcond_keys)

# Split metadata strings and store values with db keys
cond_parsed = []
for entry in cond:
    if entry["treatment"]:
        cond_types = entry["condition_type"].split(";")
        treatments = entry["treatment"].split(";")
        times = entry["times"].split(";")
        for i in range(len(cond_types)):
            new_entry = dict()
            tx = treatments[i].split("(")
            tm = times[i].split(",")
            new_entry["sample_name"] = entry["sample_name"]
            new_entry["condition_type"] = cond_types[i]
            new_entry["treatment"] = tx[0]
            if len(tx) > 1:
                new_entry["conc_intens"] = tx[1].split(")")[0]
            else:
                new_entry["conc_intens"] = ""

            if len(tm) > 1:
                new_entry["start_time"] = int(tm[0])
                new_entry["end_time"] = int(tm[1])
                new_entry["time_unit"] = tm[2]

                # Calculate duration and units
                duration = int(tm[1]) - int(tm[0])
                if tm[2] == "s":
                    if duration % 60 == 0:
                        if duration % 3600 == 0:
                            if duration % 86400 == 0:
                                duration = duration / 86400
                                duration_unit = "day"
                            else:
                                duration = duration / 3600
                                duration_unit = "hr"
                        else:
                            duration = duration / 60
                            duration_unit = "min"
                    else:
                        duration_unit = "s"
                elif tm[2] == "min":
                    if duration % 60 == 0:
                        if duration % 1440 == 0:
                            duration = duration / 1440
                            duration_unit = "day"
                        else:
                            duration = duration / 60
                            duration_unit = "hr"
                    else:
                        duration_unit = "min"
                elif tm[2] == "hr":
                    if duration % 24 == 0:
                        duration = duration / 24
                        duration_unit = "day"
                    else:
                        duration_unit = "hr"
                else:
                    duration_unit = "day"

                new_entry["duration"] = int(duration)
                new_entry["duration_unit"] = duration_unit
            else:
                new_entry["start_time"] = ""
                new_entry["end_time"] = ""
                new_entry["time_unit"] = ""
                new_entry["duration"] = ""
                new_entry["duration_unit"] = ""

            cond_parsed.append(new_entry)

    # If no treatment, store empty values
    else:
        new_entry = dict()
        new_entry["sample_name"] = entry["sample_name"]
        new_entry["condition_type"] = "no treatment"
        new_entry["treatment"] = ""
        new_entry["conc_intens"] = ""
        new_entry["start_time"] = ""
        new_entry["end_time"] = ""
        new_entry["time_unit"] = ""
        new_entry["duration"] = ""
        new_entry["duration_unit"] = ""
        cond_parsed.append(new_entry)

# Extract unique conditions and store integer blanks correctly
cond = dbutils.Metatable(meta_path=None, dictlist=cond_parsed)
cond_unique = cond.unique(dbcond_full_keys)
for entry in cond_unique:
    if entry["start_time"] == "":
        entry["start_time"] = None
    if entry["end_time"] == "":
        entry["end_time"] = None
    if entry["duration"] == "":
        entry["duration"] = None

# If not already present, add condition metadata to database
cond_to_add = dbutils.entry_update(dbconnect,
                           "conditionInfo",
                           dbcond_full_keys,
                           cond_unique)
if len(cond_to_add) > 0:
    for entry in cond_to_add:
        if entry["start_time"] == "None":
            entry["start_time"] = None
        if entry["end_time"] == "None":
            entry["end_time"] = None
        if entry["duration"] == "None":
            entry["duration"] = None
    dbconnect.engine.execute(
        dborm.conditionInfo.__table__.insert(), cond_to_add)

# Make condition match table
dbconnect = dbutils.dbnascentConnection(db_url, creds)

# Make new key list for database info dump
dbcond_add_keys = list(dict(config["condition keys"]).keys())
dbcond_add_keys.append("condition_id")

# Grab condition and sample id info from database
dbcond_dump = dbconnect.reflect_table("conditionInfo")
dbcond = dbutils.Metatable(meta_path=None, dictlist=dbcond_dump)
dbcond_data = dbcond.key_grab(dbcond_add_keys)
dbsamp_dump = dbconnect.reflect_table("sampleID")
dbsamp = dbutils.Metatable(meta_path=None, dictlist=dbsamp_dump)
name_id = dbsamp.unique(["sample_name", "sample_id"])

# Add sample ids to parsed condition table
for entry in cond_parsed:
    for eq in name_id:
        if entry["sample_name"] == eq["sample_name"]:
            entry["sample_id"] = eq["sample_id"]
    # Add condition_id to matching condition entries
    dbutils.key_store_compare(entry, dbcond_data,
                              dbcond_full_keys, ["condition_id"])

# If not already present, add sample/condition ids to database
exptcond = dbutils.Metatable(meta_path=None, dictlist=cond_parsed)
exptcond_unique = exptcond.unique(["sample_id", "condition_id"])
exptcond_to_add = dbutils.entry_update(dbconnect,
                               "sampleCondition",
                               ["sample_id", "condition_id"],
                               exptcond_unique)
if len(exptcond_to_add) > 0:
    dbconnect.engine.execute(
        dborm.sampleCondition.insert(), exptcond_to_add)

# Make sampleAccum table
dbconnect = dbutils.dbnascentConnection(db_url, creds)

# Load data location and keys, then replace keys in metadata table
data_path = config["file_locations"]["db_data"]
accum_keys = list(dict(config["metatable accum keys"]).values())
dbaccum_keys = list(dict(config["metatable accum keys"]).keys())
dbaccum_full_keys = list(dict(config["accum keys"]).keys())
samp.key_replace(accum_keys, dbaccum_keys)

# Scrape all QC data for each sample and add to sample dict
for entry in samp.data:
    fastqc_dict = dbutils.scrape_fastqc(entry["paper_id"],
                                        entry["sample_name"],
                                        data_path,
                                        entry)
    pic_dict = dbutils.scrape_picard(entry["paper_id"],
                                     entry["sample_name"],
                                     data_path)
    mapstats_dict = dbutils.scrape_mapstats(entry["paper_id"],
                                            entry["sample_name"],
                                            data_path,
                                            entry)
    rseqc_dict = dbutils.scrape_rseqc(entry["paper_id"],
                                      entry["sample_name"],
                                      data_path)
    preseq_dict = dbutils.scrape_preseq(entry["paper_id"],
                                        entry["sample_name"],
                                        data_path)
    pileup_dict = dbutils.scrape_pileup(entry["paper_id"],
                                        entry["sample_name"],
                                        data_path)
    entry.update(fastqc_dict)
    entry.update(pic_dict)
    entry.update(mapstats_dict)
    entry.update(rseqc_dict)
    entry.update(preseq_dict)
    entry.update(pileup_dict)

    # Calculate qc and data scores and add to sample dict
    score_dict = dbutils.sample_qc_calc(entry)
    entry.update(score_dict)

    # Parse replicate number
    rep_num = re.split(r'(\d+)', entry["replicate"])
    entry["replicate"] = rep_num[1]

# Change datatypes for unique calc
for entry in samp.data:
    for key in entry:
        entry[key] = str(entry[key])

# If not already present, add sample accum info to database
accum_unique = samp.unique(dbaccum_full_keys)
accum_to_add = dbutils.entry_update(dbconnect,
                                    "sampleAccum",
                                    dbaccum_full_keys,
                                    accum_unique)
if len(accum_to_add) > 0:
    for entry in accum_unique:
        for key in ["rcomp", "expt_unusable", "timecourse"]:
            if entry[key] == '1':
                entry[key] = True
            elif entry[key] == '0':
                entry[key] = False
        for key in entry:
            if entry[key] == 'None':
                entry[key] = None
    dbconnect.engine.execute(
        dborm.sampleAccum.__table__.insert(), accum_to_add)

# Calculate and add paper qc/data scores (median of sample qc/data scores)
dbconnect = dbutils.dbnascentConnection(db_url, creds)
paper_scores = dbutils.paper_qc_calc(accum_unique)
dbconnect.engine.execute(
    dborm.exptMetadata.__table__.update().
    where(dborm.exptMetadata.__table__.c.paper_id == paper_id),
    paper_scores)

# Add nascentflow/bidirflow version data
dbconnect = dbutils.dbnascentConnection(db_url, creds)
nf_keys = list(dict(config["nascentflow keys"]).values())
dbnf_keys = list(dict(config["nascentflow keys"]).keys())
bf_keys = list(dict(config["bidirflow keys"]).values())
dbbf_keys = list(dict(config["bidirflow keys"]).keys())
dirpath = config["file_locations"]["db_data"]

# Parse version yamls and set entries as strings for unique calc
nftab = dbutils.add_version_info(dbconnect,
                                 paper_id,
                                 dirpath,
                                 "nascent",
                                 dbnf_keys)
for entry in nftab:
    for key in entry:
        entry[key] = str(entry[key])
bidirtab = dbutils.add_version_info(dbconnect,
                                    paper_id,
                                    dirpath,
                                    "bidir",
                                    dbbf_keys)
for entry in bidirtab:
    for key in entry:
        entry[key] = str(entry[key])

# If not already present, add nascentflow version info to database
nf_table = dbutils.Metatable(meta_path=None, dictlist=nftab)
nf_unique = nf_table.unique(dbnf_keys)
nf_to_add = dbutils.entry_update(dbconnect,
                                 "nascentflowMetadata",
                                 dbnf_keys,
                                 nf_unique)
if len(nf_to_add) > 0:
    # Store null values correctly
    for entry in nf_to_add:
        for key in dbnf_keys:
            if entry[key] == "None":
                entry[key] = None
    dbconnect.engine.execute(
        dborm.nascentflowMetadata.__table__.insert(), nf_to_add)

# If not already present, add bidirflow version info to database
bidir_table = dbutils.Metatable(meta_path=None, dictlist=bidirtab)
bidir_unique = bidir_table.unique(dbbf_keys)
bf_to_add = dbutils.entry_update(dbconnect,
                                 "bidirflowMetadata",
                                 dbbf_keys,
                                 bidir_unique)
if len(bf_to_add) > 0:
    # Store null values correctly
    for entry in bf_to_add:
        for key in dbbf_keys:
            if entry[key] == "None":
                entry[key] = None
    dbconnect.engine.execute(
        dborm.bidirflowMetadata.__table__.insert(), bf_to_add)

# Connect version data to samples
dbconnect = dbutils.dbnascentConnection(db_url, creds)
dbnf_add_keys = list(dict(config["nascentflow keys"]).keys())
dbnf_add_keys.append("nascentflow_id")
dbbf_add_keys = list(dict(config["bidirflow keys"]).keys())
dbbf_add_keys.append("bidirflow_id")

# Extract current version data from database
dbnf_dump = dbconnect.reflect_table("nascentflowMetadata")
dbnf = dbutils.Metatable(meta_path=None, dictlist=dbnf_dump)
dbnf_data = dbnf.key_grab(dbnf_add_keys)
dbbf_dump = dbconnect.reflect_table("bidirflowMetadata")
dbbf = dbutils.Metatable(meta_path=None, dictlist=dbbf_dump)
dbbf_data = dbbf.key_grab(dbbf_add_keys)

# Add ids from version tables to data tables that have sample ids
for entry in nf_table.data:
    dbutils.key_store_compare(entry, dbnf_data,
                              dbnf_keys, ["nascentflow_id"])
for entry in bidir_table.data:
    dbutils.key_store_compare(entry, dbbf_data,
                              dbbf_keys, ["bidirflow_id"])

# If not already present, add sample and nascentflow ids to database
exptnf = dbutils.Metatable(meta_path=None, dictlist=nf_table.data)
exptnf_unique = exptnf.unique(["sample_id", "nascentflow_id"])
exptnf_to_add = dbutils.entry_update(dbconnect,
                                     "sampleNascentflow",
                                     ["sample_id", "nascentflow_id"],
                                     exptnf_unique)
if len(exptnf_to_add) > 0:
    dbconnect.engine.execute(
        dborm.sampleNascentflow.insert(), exptnf_to_add)

# If not already present, add sample and bidirflow ids to database
exptbf = dbutils.Metatable(meta_path=None, dictlist=bidir_table.data)
exptbf_unique = exptbf.unique(["sample_id", "bidirflow_id"])
exptbf_to_add = dbutils.entry_update(dbconnect,
                             "sampleBidirflow",
                             ["sample_id", "bidirflow_id"],
                             exptbf_unique)
if len(exptbf_to_add) > 0:
    dbconnect.engine.execute(
        dborm.sampleBidirflow.insert(), exptbf_to_add)

# db_paper_add_update.py ends here
