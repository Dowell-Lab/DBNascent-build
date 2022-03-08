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
backupdir = config["file_locations"]["backup_dir"]
dbutils.dbnascent_backup(dbconnect, backupdir, False)

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

# Load search equivalencies table keys and external location
search_keys = list(dict(config["searcheq keys"]).values())
dbsearch_keys = list(dict(config["searcheq keys"]).keys())
searchtable_path = config["file_locations"]["searcheq_table"]

# Read in search equivalencies table and make sure entries are unique
searcheqs = dbutils.Metatable(searchtable_path)
searcheqs.key_replace(search_keys, dbsearch_keys)
searcheqs_unique = searcheqs.unique(dbsearch_keys)

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

# db_global_add_update.py ends here
