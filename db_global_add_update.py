#!/usr/bin/env python
#
# Filename: db_global_add_update.py
# Description: Changes to global DBNascent values
# Authors: Lynn Sanford <lynn.sanford@colorado.edu>
#

# Commentary:
#
# This file contains code for changing global values
# (meaning non-sample- or paper-specific values) in
# the Dowell Lab's Nascent Database.
#

# Code:

# Import
import dbutils
import dborm

# Load config file
config = dbutils.load_config(
    "/scratch/Shares/dowell/dbnascent/DBNascent-build/config_build.txt"
)

# Create database connection object and database schema
#   This creates tables that do not already exist
#   Does NOT update fields in tables that do exist
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]

dbconnect = dbutils.dbnascentConnection(db_url, creds)
dbconnect.add_tables()

# Back up entire database
#backupdir = config["file_locations"]["backup_dir"]
#dbconnect.backup(backupdir, False)

# Load organism table keys and external location
org_keys = list(dict(config["organism keys"]).values())
dborg_keys = list(dict(config["organism keys"]).keys())
orgtable_path = config["file_locations"]["organism_table"]

# Read in organism table and make sure entries are unique
orgs = dbutils.Metatable(orgtable_path)
orgs.key_replace(org_keys, dborg_keys)
orgs_unique = orgs.unique(dborg_keys)

# If not already present, add data to database
orgs_to_add = dbutils.entry_update(
                  dbconnect,
                  "organismInfo",
                  dborg_keys,
                  orgs_unique,
              )
if len(orgs_to_add) > 0:
    dbconnect.engine.execute(
        dborm.organismInfo.__table__.insert(),
        orgs_to_add
    )

# Load sample type table keys and external location
tissuetype_keys = list(dict(config["tissue type keys"]).values())
dbtissuetype_keys = list(dict(config["tissue type keys"]).keys())
tissuetype_path = config["file_locations"]["tissue_table"]

# Read in sample type table and make sure entries are unique
tissuetypes = dbutils.Metatable(tissuetype_path)
tissuetypes.key_replace(tissuetype_keys, dbtissuetype_keys)
tissuetypes_unique = tissuetypes.unique(dbtissuetype_keys)

# If not already present, add data to database
tissuetypes_to_add = dbutils.entry_update(
                       dbconnect,
                       "tissueDetails",
                       dbtissuetype_keys,
                       tissuetypes_unique
                   )
for tissuetype in tissuetypes_to_add:
    for key in ["disease"]:
        if tissuetype[key] == '1':
            tissuetype[key] = True
        elif tissuetype[key] == '0':
            tissuetype[key] = False
if len(tissuetypes_to_add) > 0:
    dbconnect.engine.execute(
        dborm.tissueDetails.__table__.insert(),
        tissuetypes_to_add
    )

# db_global_add_update.py ends here
