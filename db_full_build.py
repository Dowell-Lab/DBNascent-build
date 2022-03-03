#!/usr/bin/env python
# coding: utf-8

# Script for adding and updating DBNascent values

# Import
import dbutils
import dborm
import datetime

# Load config file
config = dbutils.load_config(
    "/scratch/Shares/dowell/dbnascent/DBNascent-build/config.txt")

# Create database connection object and database schema
# This creates tables that do not already exist
# Does not update tables that do exist
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]
backupdir = config["file_locations"]["backup_dir"]

dbconnect = dbutils.dbnascentConnection(db_url, creds)
dbconnect.add_tables()
dbconnect.backup(backupdir, False)

# Add/update organism table
organism_keys = list(dict(config["organism keys"]).values())
dborg_keys = list(dict(config["organism keys"]).keys())
orgtable_path = config["file_locations"]["organism_table"]

# Read in organism table and make sure entries are unique
orgs = dbutils.Metatable(orgtable_path)
orgs.key_replace(organism_keys, dborg_keys)
orgs_unique = orgs.unique(dborg_keys)

# If not already present, add data to database
orgs_to_add = dbutils.entry_update(dbconnect,
                                   "organismInfo",
                                   dborg_keys,
                                   orgs_unique)
if len(orgs_to_add) > 0:
    dbconnect.engine.execute(
        dborm.organismInfo.__table__.insert(), orgs_to_add)

# Add/update search equivalencies table
search_keys = list(dict(config["searcheq keys"]).values())
dbsearch_keys = list(dict(config["searcheq keys"]).keys())
searchtable_path = config["file_locations"]["searcheq_table"]

# Read in search equivalencies table and make sure entries are unique
eqs = dbutils.Metatable(searchtable_path)
eqs.key_replace(search_keys, dbsearch_keys)
eqs_unique = eqs.unique(dbsearch_keys)

# If not already present, add data to database
eqs_to_add = dbutils.entry_update(dbconnect,
                                  "searchEq",
                                  dbsearch_keys,
                                  eqs_unique)
if len(eqs_to_add) > 0:
    dbconnect.engine.execute(
        dborm.searchEq.__table__.insert(), eqs_to_add)
