#!/usr/bin/env python
#
# Filename: db_backup_delete.py
# Description: Backup DBNascent and then delete database by table
# Authors: Lynn Sanford <lynn.sanford@colorado.edu>
#

# Commentary:
#
# This file contains code for backing up DBNascent
# before deleting all tables in order to build from scratch
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

# Back up entire database
backupdir = config["file_locations"]["backup_dir"]
dbconnect.backup(backupdir, False)

# Delete tables
# Can optionally pass a list of tables in the following format:
#    [dborm.<tablename>.__table__,]
dbconnect.delete_tables()

# db_backup_delete.py ends here
