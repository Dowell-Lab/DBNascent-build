"""Load functions for building and maintaining DBNascent.

Filename: utils.py
Author: Lynn Sanford <lynn.sanford@colorado.edu>

Commentary:
    This module contains utility functions and classes for
    reducing the total amount of code needed for building and
    updating the database

Classes:

Functions:
    load_config(file) -> object
    add_tables(db_url)
    table_parse(file) -> list of dicts
    key_grab(dict, list) -> list of lists
    get_unique_table(file, list) -> dict
    value_compare(object, dict, dict)
    object_as_dict(object)

Misc variables:
"""

import os
import configparser
import sqlalchemy as sql
import csv
from sqlalchemy.orm import sessionmaker
from . import dborm


# Database Connection Handler
class dbnascentConnection:
    """A class to handle connection to the mysql database.

    Attributes:
        engine (dialect, pool objects) : engine created by sqlalchemy

        session (session object) : ORM session object created by sqlalchemy

    Methods:
        __enter__ :
    """

    engine = None
    _Session = None
    session = None

    def __init__(self, db_url, cred_path):
        """Initialize database connection.

        Parameters:
            db_url (str) : path to database (mandatory)

            cred_path (str) : path to tab-delimited credentials
                one line file with username tab password

        Returns:
            none
        """
        if cred_path:
            with open(cred_path) as f:
                cred = next(f).split("\t")
            self.engine = sql.create_engine("mysql://" + cred[0] + ":"
                                            + cred[1] + db_url, echo=False)
        elif db_url:
            self.engine = sql.create_engine("mysql://" + db_url, echo=False)
        else:
            raise FileNotFoundError(
                "Database url must be provided"
            )
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def add_tables(self) -> None:
        """Add tables into database from ORM.

        Does not update existing tables.

        Parameters:
            none

        Returns:
            none
        """
        Base.metadata.create_all(self.engine)

#    def __enter__(self):
#        return self.session
#
#    def __exit__(self, exc_type, exc_val, exc_tb):
#        self.session.commit()
#        self.engine.dispose()


# Metatable class definition
class Metatable:
    """A class to store metadata.

    Attributes:
        data (list of dicts) :

    Methods:
        load_file :
    """

    def __init__(self, meta_path):
        """Initialize metatable object.

        Parameters:
            meta_path (str) : path to metadata file
                file must be tab-delimited with field names as header
        """
        self.data = []

        if meta_path:
            self.load_file(meta_path)

    def load_file(meta_path):
        """Load metatable object.

        Parameters:
            meta_path (str) : path to metadata file
                file must be tab-delimited with field names as header

        Returns:
            self.data (list of dicts)
        """
        # Check that the metadata file exists
        if not (os.path.exists(meta_path)
                and os.path.isfile(meta_path)):
            raise FileNotFoundError(
                "Metadata file does not exist at the provided path")

        with open(meta_path, newline="") as metatab:
            full_table = csv.DictReader(metatab, delimiter="\t")
            if len(full_table[0]) == 1:
                raise IndexError(
                    "Input must be tab-delimited. Double check input."
                )
            else:
                for entry in full_table:
                    self.data.append(dict(entry))

    def key_grab(self, key_list) -> list:
        """Extract values for specific keys from metatable data.

        Parameters:
            key_list (list) : desired keys from dicts in table_list

        Returns:
            value_list (list of lists) : each entry containing the values
                                         of the given keys
        """
        # Load in file as a list of dicts
        value_list = []

        # Check if keys are valid
        for key in key_list:
            if key not in self.data[0]:
                raise KeyError(
                    "Key(s) not present in metatable object."
                )

        for entry in self.data:
            value_subset = []
            for key in key_list:
                value_subset.append(entry[key])
            value_list.append(value_subset)

        return value_list

    def unique(self, extract_keys) -> dict:
        """Extract values for specific keys from a metatable filepath.

        Parameters:
            extract_keys (list) : list containing desired keys in
                                  metatable data for value extraction

        Returns:
            unique_metatable (list of dicts) : each entry contains the values
                                               of the extract keys; only
                                               returns unique sets of values
        """
        # Check if keys are valid
        for key in extract_keys:
            if key not in self.data[0]:
                raise KeyError(
                    "Key(s) not present in metatable object."
                )

        full_table_list = np.array(self.key_grab(extract_keys))
        unique_list = np.unique(full_table_list, axis=0)

        unique_metatable = []
        for entry in unique_list:
            new_dict = dict(zip(extract_keys, entry))
            unique_metatable.append(new_dict)

        return unique_metatable


# Configuration File Reader
def load_config(filename: str):
    """Load database config file compatible with configparser package.

    Parameters:
        filename (str) : path to config file

    Returns:
        config (configparser object) : parsed config file
    """
    if not os.path.exists(filename):
        raise FileNotFoundError(
            "Configuration file does not exist at the provided path"
        )
    config = configparser.ConfigParser()
    with open(filename) as confFile:
        config.read_string(confFile.read())
    return config


def value_compare(db_row, metatable_row, key_dict) -> bool:
    """Compare values between two dicts.

    Parameters:
        db_row (dict) : dict extracted from one entry in
                        one table of the database

        metatable_row (dict) : dict extracted from a metadata table

        key_dict (dict) : specific keys for comparison

    Returns:
        {0,1} (boolean) : whether the value in the database matches the
                          metadata value; 1 if matching, 0 if not
    """
    for key in key_dict:
        if db_row[key] == metatable_row[key_dict[key]]:
            continue
        else:
            return 0
    return 1


def object_as_dict(obj):
    """Convert queried database entry into dict.

    Parameters:
        obj (str) : single row (entry) of a database query output

    Returns:
        db_dict (dict) : key-value pairs from database entry
    """
    db_dict = {c.key: getattr(obj, c.key) for c
               in sql.inspect(obj).mapper.column_attrs}
    return db_dict

#
# utils.py ends here