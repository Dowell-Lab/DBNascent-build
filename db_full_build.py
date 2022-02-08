#!/usr/bin/env python
# coding: utf-8

# Script for adding and updating DBNascent values

# Import
from . import dbutils
from . import dborm
import datetime

# Load config file
config = dbutils.load_config(
    "/home/lsanford/Documents/data/repositories/dbnascent_build/config.txt")

# Create database connection object and database schema
# This creates tables that do not already exist
# Does not update tables that do exist
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]
backupdir = config["file_locations"]["backup_dir"]

dbconnect = dbutils.dbnascentConnection(db_url, creds)
dbconnect.add_tables()
# dbconnect.backup(backupdir, False)

# Add/update organism table
organism_keys = list(dict(config["organism keys"]).values())
dborg_keys = list(dict(config["organism keys"]).keys())
orgtable_path = config["file_locations"]["organism_table"]

# Read in organism table and make sure entries are unique
orgs = dbutils.Metatable(orgtable_path)
orgs_unique = orgs.unique(organism_keys, dborg_keys)

# Add data to database
dbconnect.engine.execute(organismInfo.__table__.insert(), orgs_unique)

# Add/update search equivalencies table
search_keys = list(dict(config["searcheq keys"]).values())
dbsearch_keys = list(dict(config["searcheq keys"]).keys())
searchtable_path = config["file_locations"]["searcheq_table"]

# Read in search equivalencies table and make sure entries are unique
eqs = utils.Metatable(searchtable_path)
eqs_unique = eqs.unique(search_keys, dbsearch_keys)

# Add data to database
dbconnect.engine.execute(searchEq.__table__.insert(), eqs_unique)