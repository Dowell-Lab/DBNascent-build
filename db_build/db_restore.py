#!/usr/bin/env python
#
# Filename: db_restore.py
# Description: Restore database from backup
# Authors: Lynn Sanford <lynn.sanford@colorado.edu>
#

# Commentary:
#
# This file contains code to restore the Dowell Lab Nascent
# Database from serialized backups
#

# Code:

# Import
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'global_files'))
import dborm
import dbutils

# User input
restore_timestamp = "20220310_134246"
tables = []

# Load config file
config = dbutils.load_config(
    "/Shares/dbnascent/DBNascent-build/config/config_build.txt"
)

# Create database connection object
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]

dbconnect = dbutils.dbnascentConnection(db_url, creds)

# Define restore location
backupdir = config["file_locations"]["backup_dir"]
restoredir = backupdir + "/" + restore_timestamp

# Restore tables
dbconnect.restore(restoredir, tables)

# db_restore.py ends here
