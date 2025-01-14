#!/usr/bin/env python
#
# Filename: db_global_add_update.py
# Description: Changes to global DBNascent values
# Authors: Lynn Sanford <lynn.sanford@colorado.edu>
#

# Commentary:
#
# This file contains code for changing global values
# (meaning non-sample- or paper-dependent values) in
# the Dowell Lab's Nascent Database.
#

# Code:

# Import
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'global_files'))
import dborm
import dbutils

# Load config file
config = dbutils.load_config(
    "/home/lsanford/DBNascent-build/config/config_build.txt"
)
files = config["file_locations"]

# Create database connection object and database schema
dbconnect = dbutils.dbnascentConnection(files["database"], files["credentials"])
dbconnect.add_tables()

# Back up entire database
#backupdir = config["file_locations"]["backup_dir"]
#dbconnect.backup(backupdir, False)

# Load organism table keys and external location
organisms_keys = dbutils.load_keys(config,"organisms")
orgtable_path = files["organism_table"]

# Read in organism table and make sure entries are unique
orgs = dbutils.Metatable(orgtable_path)
orgs.key_replace(organisms_keys["in"], organisms_keys["match"])
orgs.data = dbutils.format_for_db_add(dbconnect,orgs.data)
orgs_unique = orgs.unique(organisms_keys["db"])

# If not already present, add data to database
orgs_to_add = dbutils.entry_update(
                  dbconnect,
                  "organisms",
                  organisms_keys["db"],
                  orgs_unique,
              )
if len(orgs_to_add) > 0:
    orgs_to_add = dbutils.format_for_db_add(dbconnect,orgs_to_add)
    dbconnect.engine.execute(
        dborm.organisms.__table__.insert(),
        orgs_to_add
    )

# Load sample type table keys and external location
tissues_keys = dbutils.load_keys(config,"tissues")
tissues_path = files["tissue_table"]

# Read in sample type table and make sure entries are unique
tissues = dbutils.Metatable(tissues_path)
tissues.key_replace(tissues_keys["in"], tissues_keys["match"])
tissues.data = dbutils.format_for_db_add(dbconnect,tissues.data)
tissues_unique = tissues.unique(tissues_keys["db"])

# If not already present, add data to database
tissues_to_add = dbutils.entry_update(
                       dbconnect,
                       "tissues",
                       tissues_keys["db"],
                       tissues_unique
                   )
if len(tissues_to_add) > 0:
    tissues_to_add = dbutils.format_for_db_add(dbconnect,tissues_to_add)
    dbconnect.engine.execute(
        dborm.tissues.__table__.insert(),
        tissues_to_add
    )

# Load archived data table keys and external location
archive_keys = dbutils.load_keys(config,"archiveddata")
archive_path = files["archiveddata_table"]

# Read in archived data table and make sure entries are unique
archive = dbutils.Metatable(archive_path)
archive.key_replace(archive_keys["in"], archive_keys["match"])
archive.data = dbutils.format_for_db_add(dbconnect,archive.data)
archive_unique = archive.unique(archive_keys["db"])

# If not already present, add data to database
archive_to_add = dbutils.entry_update(
                       dbconnect,
                       "archive",
                       archive_keys["db"],
                       archive_unique
                   )
if len(archive_to_add) > 0:
    archive_to_add = dbutils.format_for_db_add(dbconnect,archive_to_add)
    dbconnect.engine.execute(
        dborm.archive.__table__.insert(),
        archive_to_add
    )

# db_global_add_update.py ends here
