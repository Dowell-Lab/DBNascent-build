# utils.py --- Utilities for simplifying database code
#
# Filename: utils.py
# Description: Miscellaneous utilities for simplifying database code
# Author: Zachary Maas <zama8258@colorado.edu> and Lynn Sanford 
# Maintainer: Lynn Sanford <lynn.sanford@colorado.edu>
# Created: Mon Jul  1 16:04:05 2019 (-0600)
#

# Commentary:
#
# This module contains a few helpful utility functions and classes for
# reducing the total amount of code needed for the database, since
# there are many areas where the same patterns keep popping up.
#

# Code:

import os
import configparser
import sqlalchemy as sql
import csv
from sqlalchemy.orm import sessionmaker


# Database Connection Handler
class NascentDBConnection:
    engine = None
    _Session = None
    session = None

    def __init__(self, db_url):
        self.engine = sql.create_engine(db_url, echo=False)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.commit()
        self.engine.dispose()


# Configuration File Reader
def load_config(filename: str):
    if not os.path.exists(filename):
        raise FileNotFoundError(
            "Configuration file does not exist at the provided path"
        )
    config = configparser.ConfigParser()
    with open(filename) as confFile:
        config.read_string(confFile.read())
    return config

# Add/update (?) tables in database (I'm not actually sure this updates if already existing)

def update_tables(db_url: str) -> None:
    engine = sql.create_engine("sqlite:///" + db_url, echo=False)
    Base.metadata.create_all(engine, checkfirst = True)

# Function for parsing table into list of dicts
def table_parse(table_filepath: str) -> list:
    """Takes the manually curated metadata table as input and 
    turns it into a list of dicts, one entry for each srr with
    key: value pairs for each column in the metadata table
    Output: List of dicts
    """

    # Check that the table file exists
    if not (
        os.path.exists(table_filepath) and os.path.isfile(table_filepath)
    ):
        raise FileNotFoundError(f"{table_filepath} does not exist.")

    # Load in file as a list of dicts
    table_list = []
    with open(table_filepath, newline = '') as tab:                                                                                          
        full_table = csv.DictReader(tab, delimiter="\t")
        for entry in full_table:
            table_list.append(dict(entry))
    
    return table_list

# Function for grabbing specific keys
def key_grab(table_list, key_list) -> list:
    """Takes list of dicts and a list of keys and 
    extracts specific values to a list for inputting into database
    Output: List of values corresponding to input keys for each 
    table entry
    """
    # Load in file as a list of dicts
    value_list = []
    for entry in table_list:
        value_subset = []
        for i in range(len(key_list)):
            value_subset.append(entry[key_list[i]])
        value_list.append(value_subset)
    
    return value_list

def get_unique_table(location_key, column_keys) -> dict:
    filepath = config["file_locations"][location_key]
    full_table_dict = table_parse(filepath)
    
    full_table_list = np.array(key_grab(full_table_dict, column_keys))
    unique_list = np.unique(full_table_list, axis=0)

    unique_table = []
    for i in range(len(unique_list)):
        entry = dict(zip(column_keys, unique_list[i]))
        unique_table.append(entry)
    
    return unique_table

def value_compare(db_row,metatable_row,key_dict) -> bool:
    for key in key_dict:
        if db_row[key] == metatable_row[key_dict[key]]:
            continue
        else:
            return 0
    return 1   
    
def object_as_dict(obj):
    return {c.key: getattr(obj, c.key)
            for c in sql.inspect(obj).mapper.column_attrs}
    
#
# utils.py ends here