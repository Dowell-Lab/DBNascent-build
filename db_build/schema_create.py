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
    "/Shares/dbnascent/DBNascent-build/config/config_build.txt"
)
db_url = config["file_locations"]["database"]
creds = config["file_locations"]["credentials"]
dbconnect = dbutils.dbnascentConnection(db_url, creds)

# create the pydot graph object by autoloading all tables via a bound metadata object
graph = create_schema_graph(metadata=MetaData(dbconnect.engine),
    show_datatypes=False, # The image would get nasty big if we'd show the datatypes
    show_indexes=False, # ditto for indexes
    rankdir='LR', # From left to right (instead of top to bottom)
    concentrate=False # Don't try to join the relation lines together
)
graph.write_png("/Shares/dbnascent/DBNascent-build/dbschema.png")

# schema_create.py ends here
