#!/usr/bin/env python
#
# Filename: schema_create.py
# Description: Make database schema diagram
# Authors: Lynn Sanford <lynn.sanford@colorado.edu>
#

# Commentary:
#
# This file uses the package SchemaDisplay to create a db schema
# https://github.com/sqlalchemy/sqlalchemy/wiki/SchemaDisplay
#

# Code:

# Import
import sqlalchemy as sql
from sqlalchemy import MetaData
from sqlalchemy_schemadisplay import create_schema_graph
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'global_files'))
import dborm
import dbutils


# Load config file and connect to db
config = dbutils.load_config(
    "/home/lsanford/DBNascent-build/config/config_build.txt"
)
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]
dbconnect = dbutils.dbnascentConnection(db_url, creds)

# create the pydot graph object by autoloading all tables via a bound metadata object
graph = create_schema_graph(
    tables = [
      dborm.searchEquiv.__table__,
      dborm.sampleEquiv.__table__,
      dborm.linkIDs.__table__,
      dborm.samples.__table__,
      dborm.papers.__table__,
      dborm.genetics.__table__,
      dborm.organisms.__table__,
      dborm.conditions.__table__,
      dborm.tissues.__table__,
      dborm.bidirs.__table__,
      dborm.nascentflowRuns.__table__,
      dborm.bidirflowRuns.__table__,
      dborm.conditionLink.__table__,
      dborm.nascentflowLink.__table__,
      dborm.bidirflowLink.__table__,
    ],
    show_datatypes=False, # The image would get nasty big if we'd show the datatypes
    show_indexes=False, # ditto for indexes
    rankdir='LR', # From left to right (instead of top to bottom)
    concentrate=False, # Don't try to join the relation lines together
)
graph.write_png("/home/lsanford/DBNascent-build/dbschema.png")

# schema_create.py ends here
