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

config = dbutils.load_config(
    "/Users/lysa8537/pipelines/DBNascent-build/config_build.txt"
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

# Delete tables in opposite order from ORM
#Base.metadata.drop_all(bind=your_engine, tables=[User.__table__])


dbconnect.engine.execute(
    dborm.sampleBidirflow.__table__.drop(),
    dborm.bidirflowMetadata.__table__.drop(),
    dborm.sampleNascentflow.__table__.drop(),
    dborm.nascentflowMetadata.__table__.drop(),
#    dborm.bidirSummary.__table__.drop(),
    dborm.sampleAccum.__table__.drop(),
    dborm.linkIDs.__table__.drop(),
    dborm.conditionInfo.__table__.drop(),
    dborm.geneticInfo.__table__.drop(),
    dborm.sampleID.__table__.drop(),
    dborm.exptMetadata.__table__.drop(),
    dborm.searchEquiv.__table__.drop(),
    dborm.organismInfo.__table__.drop(),
)

# db_backup_delete.py ends here
