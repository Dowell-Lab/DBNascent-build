"""Functions for building and maintaining DBNascent.

Filename: dbutils.py
Authors: Lynn Sanford <lynn.sanford@colorado.edu>

Commentary:
    This module contains utility functions and classes for
    reducing the total amount of code needed for building and
    updating the Dowell Lab Nacent Database

Classes:
    dbnascentConnection
    Metatable

Functions:
    load_config(file) -> object
    load_keys(dict) -> dict
    key_store_compare(dict, list, list, list) -> dict
    object_as_dict(object) -> dict
    entry_update(object, str, list, list -> list
    db_round(float) -> float
    duration_calc(list) -> list
    scrape_fastqc(str, str, str, dict) -> dict
    scrape_picard(str, str, str) -> dict
    scrape_mapstats(str, str, str, dict) -> dict
    scrape_rseqc(str, str, str) -> dict
    scrape_preseq(str, str, str) -> dict
    scrape_pileup(str, str, str) -> dict
    sample_qc_calc(dict) -> int
    paper_qc_calc(list) -> float
    add_version_info(object, str, str, str, list) -> list
"""

import configparser
import csv
import datetime
import numpy as np
import os
import re
import shutil
from statistics import median
import yaml
import zipfile as zp

import dborm
import pymysql
import sqlalchemy as sql
from sqlalchemy.ext.serializer import loads, dumps
from sqlalchemy.orm import sessionmaker


class dbnascentConnection:
    """A class to handle connection to the MySQL database.

    Attributes:
        engine (dialect, pool objects) : 
            engine created by sqlalchemy

        Session (session binding object) :
            ORM session binding object created by sqlalchemy

        session (session object) : 
            ORM session object created by sqlalchemy

    Methods:
        add_tables() :
            Adds tables from ORM to database

        delete_tables() :
            Deletes all tables in ORM from database

        reflect_table(table, filter_crit=None) -> list:
            Pulls table data from database, optionally filtered
            by filter criteria

        backup(out_path, tables=False) :
            Backs up database to an external location, optionally
            limited to specific tables

        restore(in_path, tables=False) :
            Restores database from backups, optionally limited to
            specific tables
    """

    engine = None
    _Session = None
    session = None

    def __init__(self, db_url, cred_path):
        """Initialize database connection.

        Parameters:
            db_url (str) : 
                path to database (mandatory)

            cred_path (str) : 
                path to tab-delimited credentials
                one line file with username tab password

        Returns:
            none
        """
        if cred_path:
            with open(cred_path) as f:
                cred = next(f).split("\t")
            self.engine = sql.create_engine("mysql+pymysql://" + str(cred[0]) + ":"
                                            + str(cred[1].split("\n")[0])
                                            + "@" + db_url, echo=False)
        elif db_url:
            self.engine = sql.create_engine("mysql+pymysql://" + db_url, echo=False)
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
        dborm.Base.metadata.create_all(self.engine)

    def delete_tables(self, table_list=[]) -> None:
        """Delete tables in ORM from database.

        Parameters:
            table_list (list) :
                list of tables to delete, optionally
                each entry in list should look like:
                    <tablename>.__table__

        Returns:
            none
        """
        if len(table_list) > 0:
            dborm.Base.metadata.drop_all(self.engine,
                                         tables=table_list
                                        )
        else:
            dborm.Base.metadata.drop_all(self.engine)

    def reflect_table(self, table, filter_crit=None) -> list:
        """Query all records from a specific table.

        Can optionally add filtering criteria.

        Parameters:
            table (str) : 
                table name from ORM

            filter_crit (dict) : 
                filter criteria for table
                only currently takes "=" filter criteria

        Returns:
            query_results (list of dicts) : 
                all data in table matching filter criteria
        """
        query_results = []

        query_str = "SELECT * FROM " + table
        if filter_crit is not None:
            query_str = query_str + " WHERE "
            i = 0
            for filtkey in filter_crit:
                if i == 0:
                    query_str = (query_str + str(filtkey) +
                                 ' = "' + str(filter_crit[filtkey]) + '"')
                    i = (i + 1)
                else:
                    query_str = (query_str + " AND " + str(filtkey) +
                                 ' = "' + str(filter_crit[filtkey]) + '"')

        sqlquery = self.session.execute(sql.text(query_str)).fetchall()

        for query_res in sqlquery:
            query_results.append(dict(query_res))

        return query_results
    
    def get_coltypes(self) -> dict:
        """Sorts column types for correct formatting.

        Returns:
            coltypes (dict) :
                dict with boolean/string/num columns by table
        """
        insp = sql.inspect(self.engine)
        coltypes = {}
        for table in insp.get_table_names():
            coltypes[table] = {}
            coltypes[table]["boolean"] = []
            coltypes[table]["string"] = []
            coltypes[table]["num"] = []
            for col in insp.get_columns(table):
                if "TINYINT" in str(col["type"]):
                    coltypes[table]["boolean"].append(col["name"])
                elif "CHAR" in str(col["type"]):
                    coltypes[table]["string"].append(col["name"])
                else:
                    coltypes[table]["num"].append(col["name"])

        return coltypes

    def backup(self, out_path, tables=False) -> None:
        """Backup database.

        Can optionally limit to specific tables.

        Parameters:
            out_path (str) : 
                path to backup file directory

            tables (list) : 
                list of specific tables, if whole database 
                backup is not desired

        Returns:
            none
        """
        # Make subdirectory with timestamp
        if not out_path:
            out_path = "/Shares/dbnascent/db_backups"
        now = datetime.datetime.now()
        now_dir = now.strftime("%Y%m%d_%H%M%S")
        backup_dir = out_path + "/" + now_dir
        os.makedirs(backup_dir)

        # Send contents of each table to serialized file
        if not tables:
            dborm.Base.metadata.reflect(bind=self.engine)
            tables = list(dborm.Base.metadata.tables.keys())
        for table in tables:
            outfile = backup_dir + "/" + table + ".dbdump"
            q = self.session.query(eval('dborm.' + table))
            serialized_data = dumps(q.all())
            with open(outfile, 'wb') as out:
                out.write(serialized_data)

    def restore(self, in_path, tables=False) -> None:
        """Restore database.

        Can optionally limit to specific tables.

        Parameters:
            in_path (str) : 
                path to backup file directory

            tables (list) : 
                list of specific tables, if whole database 
                restore is not desired

        Returns:
            none
        """
        # If not specified, identify all tables that have backups
        if not tables:
            files = os.listdir(in_path)
            tables = []
            for file in files:
                tables.append(file.split(".")[0])
        # Grab table order from ORM and reverse for deletion
        dborm.Base.metadata.reflect(bind=self.engine)
        dbtables = list(dborm.Base.metadata.tables.keys())
        dbtables.reverse()
        # Delete data in tables for backup (if not all tables,
        # foreign key dependencies may cause a problem)
        for dbtable in dbtables:
            if dbtable in tables:
                self.engine.execute(
                    eval("dborm." + dbtable + ".__table__.delete()")
                )
        # Restore in normal building order
        dbtables.reverse()
        for dbtable in dbtables:
            if dbtable in tables:
                infile = in_path + "/" + dbtable + ".dbdump"
                with open(infile, 'rb') as table_backup:
                    for dbentry in loads(table_backup.read()):
                        self.session.merge(dbentry)
                self.session.commit()


class Metatable:
    """A class to store metadata.

    Attributes:
        data (list of dicts) :
            metadata, one dict for each sample/paper entry

    Methods:
        load_file(meta_path) :
            load metadata from file into data attribute

        key_replace(file_keys, db_keys) :
            replace metadata file keys with database keys

        value_grab(key_list) -> list :
            extract values for specific keys

        key_grab(key_list) -> list :
            extract dicts with only specific keys

        unique(extract_keys) -> list :
            extract unique set of dicts based on specific keys
    """

    def __init__(self, input_data=False):
        """Initialize metatable object.

        Parameters:
            input_data (str OR list of dicts) : 
                path to metadata file (str)
                OR metadata (list of dicts)
                
                if path str, file must be tab-delimited with 
                field names as header
        """
        self.data = []

        if input_data:
            if type(input_data) == str:
                self.load_file(input_data)
            elif type(input_data) == list:
                if len(input_data) > 0:
                    if type(input_data[0]) == dict:
                        self.data = input_data
                    else:
                        raise TypeError(
                            "Input data must be list of dicts"
                        )

    def load_file(self, meta_path):
        """Load metatable data from file into data attribute.

        Parameters:
            meta_path (str) : path to metadata file
                file must be tab-delimited with field names as header

        Returns:
            none
        """
        # Check if path exists
        if not (os.path.exists(meta_path)
                and os.path.isfile(meta_path)):
            raise FileNotFoundError(
                "Metadata file does not exist at the provided path")

        # Load path and check if tab-delimited
        with open(meta_path, newline="") as metatab:
            full_table = list(csv.DictReader(metatab, delimiter="\t"))
            if len(full_table[0]) == 1:
                raise IndexError(
                    "Input must be tab-delimited. Double check input."
                )
            # Load metadata into data attribute
            else:
                for meta_entry in full_table:
                    self.data.append(dict(meta_entry))

    def key_replace(self, file_keys, db_keys):
        """Replace file keys with database keys.

        Parameters:
            file_keys (list) : list of keys in file

            db_keys (list) : list of keys in database
                Must be equivalent in length to file_keys
                with equivalent indeces

        Returns:
            none
        """
        # Check if keys are valid
        for filekey in file_keys:
            if filekey not in self.data[0]:
                raise KeyError(
                    "Key(s) not present in metatable object."
                )
                return
        # Replace keys
        for meta_entry in self.data:
            for i in range(len(file_keys)):
                meta_entry[db_keys[i]] = meta_entry.pop(file_keys[i])

    def value_grab(self, key_list) -> list:
        """Extract values for specific keys from metatable data.

        Parameters:
            key_list (list) : desired keys from dicts in table_list

        Returns:
            value_list (list of lists) : 
                each entry containing the values of the given keys
        """
        value_list = []

        if len(self.data) == 0:
            return value_list

        # Check if keys are valid
        for key in key_list:
            if key not in self.data[0]:
                raise KeyError(
                    "Key(s) not present in metatable object."
                )
        # Add dict values to list
        for entry in self.data:
            value_subset = []
            for key in key_list:
                value_subset.append(entry[key])
            value_list.append(value_subset)

        return value_list

    def key_grab(self, key_list) -> list:
        """Extract dicts with specific keys from metatable data.

        Parameters:
            key_list (list) : desired keys from dicts in table_list

        Returns:
            dict_list (list of dicts) : 
                each entry containing the dicts with only the 
                given keys
        """
        dict_list = []

        if len(self.data) == 0:
            return dict_list

        # Check if keys are valid
        for key in key_list:
            if key not in self.data[0]:
                raise KeyError(
                    "Key(s) not present in metatable object."
                )
        # Add subsetted dicts to list
        for entry in self.data:
            newentry = dict()
            for key in key_list:
                newentry[key] = entry[key]
            dict_list.append(newentry)

        return dict_list

    def unique(self, extract_keys) -> list:
        """Extract values for specific keys from metatable.

        Parameters:
            extract_keys (list) : 
                list containing db key labels for binding

        Returns:
            unique_metatable (list of dicts) : 
                each entry contains the values of the extract keys;
                only returns unique sets of values
        """
        unique_metatable = []

        if len(self.data) == 0:
            return unique_metatable

        # Check if keys are valid
        for key in extract_keys:
            if key not in self.data[0]:
                raise KeyError(
                    "Key(s) not present in metatable object."
                )
            else:
                for entry in self.data:
                    entry[key] = str(entry[key])

        # Get lists of values and filter for unique values
        full_table_list = np.array(self.value_grab(extract_keys))
        unique_list = np.unique(full_table_list, axis=0)
        # Zip keys and values back into dicts
        for meta_entry in unique_list:
            new_dict = dict(zip(extract_keys, meta_entry))
            unique_metatable.append(new_dict)

        return unique_metatable


# Configuration File Reader
def load_config(config_filename: str) -> dict:
    """Load database config file with configparser package.

    Parameters:
        filename (str) : 
            path to config file

    Returns:
        config (dict) : 
            parsed config file
    """
    if not os.path.exists(config_filename):
        raise FileNotFoundError(
            "Config file does not exist at the provided path"
        )
    config_full = configparser.ConfigParser()
    with open(config_filename) as conffile:
        config_full.read_string(conffile.read())
    config = {s:dict(config_full.items(s)) for s in config_full.sections()}
    return config


def load_keys(config: dict, section: str, dbfields = None) -> dict:
    """Load in keys from config file.

    Parameters:
        config (dict) : 
            config dict output by load_config
        
        section (str) : 
            which key from config file to load
        
        dbfields (list or str) : 
            any extra keys to add to a 'match' entry in dict

    Returns:
        keys (dict) : 
            dict of dicts with input and db keys
    """
    keys = {
        "in": list(config[section].values()),
        "match": list(config[section].keys())
    }
    if dbfields:
        if type(dbfields) == str:
            keys["db"] = list(config[dbfields].keys())
        elif type(dbfields) == list:
            keys["db"] = list(config[section].keys())
            keys["db"].extend(dbfields)
        else:
            raise ValueError(
                "Database fields not input correctly"
            )
    else:
        keys["db"] = list(config[section].keys())
    
    return keys


def key_store_compare(
    comp_dict,
    db_dict,
    comp_keys,
    store_keys,
    addnull = False
) -> dict:
    """Compare a dict to a list of dicts and, if matching, add new key/value.

    Converts all values to strings for comparison purposes

    Parameters:
        comp_dict (dict) : 
            single dict (usually from metatable object)

        db_dict (list of dicts) : 
            list of dicts (usually extracted from db query)

        comp_keys (list) : 
            specific keys for comparison

        store_keys (list) : 
            key(s) for adding to comp_dict

        addnull (boolean) :
            specifies whether to add null values if not present

    Returns:
        comp_dict (dict) : 
            dict with new value added
    """
    for dbentry in db_dict:
        comp = 0
        for key in comp_keys:
            if str(comp_dict[key]) != str(dbentry[key]):
                if str(comp_dict[key]) == "":
                    if dbentry[key] != None:
                        comp = 1
                else:
                    comp = 1
        if comp == 0:
            for storekey in store_keys:
                comp_dict[storekey] = dbentry[storekey]

    if addnull and (store_keys[0] not in comp_dict.keys()):
        for storekey in store_keys:
            comp_dict[storekey] = None

    return comp_dict


def bulk_key_store_compare(
    samples,
    compare,
    comp_keys,
    add_key,
    replace_key = None,
    nulls = False
):
    """Compare two lists of dicts and, if matching, add one new key

    Parameters:
        samples (Metatable object) : 
            sample metatable object

        db_dict (list of dicts) : 
            list of dicts (usually extracted from db query)

        comp_keys (list) : 
            specific keys for comparison

        add_key (list) : 
            key for adding to comp_dict

        replace_key (list) :
            key for replacing afterward if necessary

        nulls (boolean) :
            specifies whether to add null values if not present

    Returns:
        samples (Metatable object) : 
            Metatable object with new field added
    """
    for sample in samples.data:
        key_store_compare(sample, compare, comp_keys, add_key, nulls)

    if replace_key:
        samples.key_replace(add_key, replace_key)

    return samples


def listdict_compare(comp_dict, db_dict, db_keys) -> list:
    """Compare two lists of dicts and return rows not already in db.

    Converts all values to strings for comparison purposes

    Parameters:
        comp_dict (list of dicts) :
            list of dicts (usually from metatable object)

        db_dict (list of dicts) :
            list of dicts (usually extracted from db query)

        db_keys (list) :
            specific keys for comparison

    Returns:
        data_to_add (list of dicts) :
            any dicts in comp_dict not in db_dict
    """
    data_to_add = []

    for comp_entry in comp_dict:
        for dbkey in db_keys:
            comp_entry[dbkey] = str(comp_entry[dbkey])

    for db_entry in db_dict:
        for dbkey in db_keys:
            db_entry[dbkey] = str(db_entry[dbkey])

    for comp_entry in comp_dict:
        if comp_entry not in db_dict:
            data_to_add.append(comp_entry)

    return data_to_add


def object_as_dict(dbobj) -> dict:
    """Convert queried database entry into dict.

    Parameters:
        dbobj (str) : 
            single row (entry) of a database query output

    Returns:
        db_dict (dict) : 
            key-value pairs from database entry
    """
    db_dict = {
        col.key: getattr(dbobj, col.key) for col
        in sql.inspect(dbobj).mapper.column_attrs
    }
    return db_dict


def format_for_db_add(dbconn,samples) -> list:
    """Convert queried database entry into dict.

    Parameters:
        dbconn (db connection object) :
            connection to database

        samples (list of dicts) : 
            sample dicts to format

    Returns:
        samples (list of dicts) : 
            formatted sample dicts
    """
    coltypes = dbconn.get_coltypes()
    for sample in samples:
        for table in list(coltypes.keys()):
            for field in coltypes[table]["string"]:
                if field in list(sample.keys()):
                    if str(sample[field]) == "None":
                        sample[field] = ""
                    elif str(sample[field]) == "NULL":
                        sample[field] = ""
            for field in coltypes[table]["boolean"]:
                if field in list(sample.keys()):
                    if str(sample[field]) == "1":
                        sample[field] = True
                    elif str(sample[field]) == "True":
                        sample[field] = True
                    else:
                        sample[field] = False
            for field in coltypes[table]["num"]:
                if field in list(sample.keys()):
                    if str(sample[field]) == "None":
                        sample[field] = None
                    elif str(sample[field]) == "":
                        sample[field] = None
    
    return samples


def entry_update(dbconn, table, dbkeys, comp_table) -> list:
    """Find and return entries not already in database.

    Parameters:
        dbconn (dbnascentConnection object) : 
            current db connection

        table (str) : 
            which db table to search for entries

        dbkeys (list) : 
            list of keys to use for comparison

        comp_table (list of dicts) : 
            entries to match (or not) to db entries

    Returns:
        to_add (list of dicts) : 
            new entries not in db to add
    """
    db_dump = dbconn.reflect_table(table)
      
    db_dump = format_for_db_add(dbconn,db_dump)
    db_table = Metatable(db_dump)
    dbtable_data = db_table.key_grab(dbkeys)
    
    comp_table = format_for_db_add(dbconn,comp_table)

    entries_to_add = listdict_compare(
                         comp_table,
                         dbtable_data,
                         dbkeys
                     )

    return entries_to_add


def db_round(value_to_round) -> float:
    """Round values appropriately for input into database.

    Parameters:
        value_to_round (float) :
            value in float format

    Returns:
        rounded_value (float) :
            properly rounded value
    """
    if value_to_round > 99999:
        return round(value_to_round, 0)
    elif value_to_round > 9999:
        return round(value_to_round, 1)
    elif value_to_round > 999:
        return round(value_to_round, 2)
    elif value_to_round > 99:
        return round(value_to_round, 3)
    elif value_to_round > 9:
        return round(value_to_round, 4)
    else:
        return round(value_to_round, 5)


def condition_processing(samples) -> list:
    """Parse condition syntax.

    Parameters:
        pre (list of dicts) :
            list of dicts from metatable

    Returns:
        cond_parsed (list of dicts) :
            list of dicts parsed correctly for database
    """
    cond_parsed = []
    for cond in samples:
        if cond["treatment"]:
            cond_types = cond["condition_type"].split(";")
            treatments = cond["treatment"].split(";")
            times = cond["times"].split(";")

            # Check if lengths are equivalent
            if (len(cond_types) != len(treatments)) or (len(cond_types) != len(times)):
                raise SyntaxError("Treatment parsing error: " + cond["paper_name"])

            for i in range(len(cond_types)):
                new_cond = dict()
                new_cond["sample_id"] = cond["sample_id"]
                treatment = treatments[i].split("(")
                time = times[i].split(",")
                new_cond["condition_type"] = cond_types[i]
                new_cond["treatment"] = treatment[0]
                if len(treatment) > 1:
                    new_cond["conc_intens"] = treatment[1].split(")")[0]
                else:
                    new_cond["conc_intens"] = None

                if len(time) > 1:
                    new_cond["start_time"] = int(time[0])
                    new_cond["end_time"] = int(time[1])
                    new_cond["time_unit"] = time[2]

                    # Calculate duration and units
                    duration_list = duration_calc(time)
                    new_cond["duration"] = int(duration_list[0])
                    new_cond["duration_unit"] = duration_list[1]
                else:
                    new_cond["start_time"] = None
                    new_cond["end_time"] = None
                    new_cond["time_unit"] = None
                    new_cond["duration"] = None
                    new_cond["duration_unit"] = None

                cond_parsed.append(new_cond)

        # If no treatment, store empty values
        else:
            new_cond = dict()
            new_cond["sample_id"] = cond["sample_id"]
            new_cond["condition_type"] = "no treatment"
            new_cond["treatment"] = None
            new_cond["conc_intens"] = None
            new_cond["start_time"] = None
            new_cond["end_time"] = None
            new_cond["time_unit"] = None
            new_cond["duration"] = None
            new_cond["duration_unit"] = None
            cond_parsed.append(new_cond)

    return cond_parsed


def duration_calc(time_list) -> list:
    """Calculate duration and duration units from parsed treatment times.

    Parameters:
        time_list (list) :
            list of treatment times parsed from metadata
            time_list[0] is the start time
            time_list[1] is the end time
            time_list[2] is the time unit

    Returns:
        duration_list (list) :
            list containing treatment duration ([0]) and duration units ([1])
    """
    duration = int(time_list[1]) - int(time_list[0])
    if time_list[2] == "s":
        if duration % 60 == 0:
            if duration % 3600 == 0:
                if duration % 86400 == 0:
                    duration = duration / 86400
                    duration_unit = "day"
                else:
                    duration = duration / 3600
                    duration_unit = "hr"
            else:
                duration = duration / 60
                duration_unit = "min"
        else:
            duration_unit = "s"
    elif time_list[2] == "min":
        if duration % 60 == 0:
            if duration % 1440 == 0:
                duration = duration / 1440
                duration_unit = "day"
            else:
                duration = duration / 60
                duration_unit = "hr"
        else:
            duration_unit = "min"
    elif time_list[2] == "hr":
        if duration % 24 == 0:
            duration = duration / 24
            duration_unit = "day"
        else:
            duration_unit = "hr"
    else:
        duration_unit = "day"

    duration_list = [duration, duration_unit]

    return duration_list


def merge_list_accum(merge_file_list) -> list:
    """Scrape master merge lists for paper_id values.

    Parameters:
        merge_file_list (list) :
            list of files to scrape

    Returns:
        merge_ids (list) :
            list of paper_id values incl in master merges
    """
    if len(merge_file_list) == 0:
        raise FileNotFoundError(
            "Merge lists do not exist at the provided paths"
        )

    merge_ids = []
    for mergefile in merge_file_list:
       with open(mergefile) as f:
           for line in f:
               paper_id = tuple(line.strip().split("/"))[3]
               if paper_id not in merge_ids:
                   merge_ids.append(paper_id)

    return merge_ids


def scrape_fastqc(paper_id,
    sample_name,
    data_path,
    db_sample
) -> dict:
    """Scrape read length and depth from fastQC report.

    Parameters:
        paper_id (str) :
            paper identifier

        sample_name (str) :
            sample name

        data_path (str) :
            path to database paper data directory

        db_sample (dict) :
            sample_accum entry dict from db query

    Returns:
        fastqc_dict (dict) :
            scraped fastqc metadata in dict format
    """
    fastqc_dict = {}

    # Determine paths for raw fastQC file to scrape, based on SE/PE
    fqc_path = data_path + paper_id + "/qc/fastqc/zips/"
    if db_sample["single_paired"] == "paired":
        samp_zip = fqc_path + sample_name + "_1_fastqc"
    else:
        samp_zip = fqc_path + sample_name + "_fastqc"

    # If fastQC files don't exist, return null values
    if not (os.path.exists(samp_zip + ".zip")):
        fastqc_dict["raw_read_depth"] = None
        fastqc_dict["raw_read_length"] = None
        fastqc_dict["trim_read_depth"] = None
        return fastqc_dict

    # Unzip fastQC report
    with zp.ZipFile(samp_zip + ".zip", "r") as zp_ref:
        zp_ref.extractall(fqc_path)

    # Extract raw depth and read length
    fdata = open(samp_zip + "/fastqc_data.txt")
    for line in fdata:
        if re.compile("Total Sequences").search(line):
            fastqc_dict["raw_read_depth"] = int(line.split()[2])
        if re.compile("Sequence length").search(line):
            fastqc_dict["raw_read_length"] = int(line.split()[2].split("-")[0])

    # Remove unzipped file
    shutil.rmtree((samp_zip + "/"), ignore_errors=True)

    # Determine paths for trim fastQC file to scrape, based on SE/PE
    # and whether reverse complemented or not
    if (str(db_sample["rcomp"]) == '1') or (str(db_sample["rcomp"]) == 'True'):
        if db_sample["single_paired"] == "paired":
            trim_zip = fqc_path + sample_name + "_1.trim_fastqc"
        else:
            trim_zip = fqc_path + sample_name + ".flip.trim_fastqc"
    else:
        if db_sample["single_paired"] == "paired":
            trim_zip = fqc_path + sample_name + "_1.trim_fastqc"
        else:
            trim_zip = fqc_path + sample_name + ".trim_fastqc"

    # If trimmed fastQC report doesn't exist, return null value for
    # trimmed read depth
    if not (os.path.exists(trim_zip + ".zip")):
        fastqc_dict["trim_read_depth"] = None
        return fastqc_dict

    # Unzip trimmed fastQC report
    with zp.ZipFile(trim_zip + ".zip", "r") as zp_ref:
        zp_ref.extractall(fqc_path)

    # Extract trimmed read depth
    fdata = open(trim_zip + "/fastqc_data.txt")
    for line in fdata:
        if re.compile("Total Sequences").search(line):
            fastqc_dict["trim_read_depth"] = int(line.split()[2])

    # Remove unzipped file
    shutil.rmtree((trim_zip + "/"), ignore_errors=True)

    return fastqc_dict


def scrape_picard(paper_id, sample_name, data_path):
    """Scrape read length and depth from picard duplication report.

    Parameters:
        paper_id (str) :
            paper identifier

        sample_name (str) :
            sample name derived from db query

        data_path (str) :
            path to database storage directory

    Returns:
        picard_dict (dict) :
            scraped picard metadata in dict format
    """
    picard_dict = {}

    dirpath = data_path + paper_id + "/qc/picard/dups/"
    filepath = dirpath + sample_name + ".marked_dup_metrics.txt"

    # If picardtools data doesn't exist, return null value
    if not (os.path.exists(filepath) and os.path.isfile(filepath)):
        picard_dict["duplication_picard"] = None
        return picard_dict

    # Extract duplication data
    fdata = open(filepath)
    for line in fdata:
        if re.compile("Unknown Library").search(line):
            dup = float(line.split("\t")[8])
            picard_dict["duplication_picard"] = round(dup, 5)

    return picard_dict


def scrape_mapstats(paper_id, sample_name, data_path, db_sample):
    """Scrape read length and depth from hisat2 mapstats report.

    Parameters:
        paper_id (str) :
            paper identifier

        sample_name (str) :
            sample name derived from db query

        data_path (str) :
            path to database storage directory

        db_sample (dict) :
            sample_accum entry dict from db query

    Returns:
        mapstats_dict (dict) :
            scraped hisat2 metadata in dict format
    """
    mapstats_dict = {}

    dirpath = data_path + paper_id + "/qc/hisat2_mapstats/"
    filepath = dirpath + sample_name + ".hisat2_mapstats.txt"

    # If hisat mapping data doesn't exist, return null values
    if not (os.path.exists(filepath) and os.path.isfile(filepath)):
        mapstats_dict["single_map"] = None
        mapstats_dict["multi_map"] = None
        mapstats_dict["map_prop"] = None
        return mapstats_dict

    fdata = open(filepath)

    # Sum up and report mapped reads for paired end data
    if db_sample["single_paired"] == "paired":
        for line in fdata:
            if re.compile("concordantly 1 time").search(line):
                reads = int(line.split(": ")[1].split(" (")[0]) * 2
            if re.compile("Aligned 1 time").search(line):
                mapstats_dict["single_map"] = reads + int(
                    line.split(": ")[1].split(" (")[0]
                )
            if re.compile("concordantly >1 times").search(line):
                reads = int(line.split(": ")[1].split(" (")[0]) * 2
            if re.compile("Aligned >1 times").search(line):
                mapstats_dict["multi_map"] = reads + int(
                    line.split(": ")[1].split(" (")[0]
                )
            if re.compile("Overall alignment rate").search(line):
                alrate = float(line.split(": ")[1].split("%")[0]) / 100
                mapstats_dict["map_prop"] = round(alrate, 5)
    # Report mapped reads for single end data
    else:
        for line in fdata:
            if re.compile("Aligned 1 time").search(line):
                mapstats_dict["single_map"] = int(line.split(": ")[1].split(" (")[0])
            if re.compile("Aligned >1 times").search(line):
                mapstats_dict["multi_map"] = int(line.split(": ")[1].split(" (")[0])
            if re.compile("Overall alignment rate").search(line):
                alrate = float(line.split(": ")[1].split("%")[0]) / 100
                mapstats_dict["map_prop"] = round(alrate, 5)

    return mapstats_dict


def scrape_rseqc(paper_id, sample_name, data_path):
    """Scrape read length and depth from RSeQC report.

    Parameters:
        paper_id (str) :
            paper identifier

        sample_name (str) :
            sample name derived from db query

        data_path (str) :
            path to database storage directory

    Returns:
        rseqc_dict (dict) :
            scraped RSeQC metadata in dict format
    """
    rseqc_dict = {}

    dirpath = data_path + paper_id + "/qc/rseqc/read_distribution/"
    filepath = dirpath + sample_name + ".read_distribution.txt"

    # If rseqc read distribution data doesn't exist, return nulls
    if not (os.path.exists(filepath) and os.path.isfile(filepath)):
        rseqc_dict["rseqc_tags"] = None
        rseqc_dict["rseqc_cds"] = None
        rseqc_dict["cds_rpk"] = None
        rseqc_dict["rseqc_five_utr"] = None
        rseqc_dict["rseqc_three_utr"] = None
        rseqc_dict["rseqc_intron"] = None
        rseqc_dict["intron_rpk"] = None
        rseqc_dict["exint_ratio"] = None
        return rseqc_dict

    # Extract RSeQC data
    # MySQL rounds things strangely, so manual rounding required
    # for proper matching to db entries
    fdata = open(filepath)
    for line in fdata:
        if re.compile("Total Assigned Tags").search(line):
            rseqc_dict["rseqc_tags"] = int(line.split()[-1])
        if re.compile("CDS_Exons").search(line):
            rseqc_dict["rseqc_cds"] = int(line.split()[2])
            cds = float(line.split()[-1])
            rseqc_dict["cds_rpk"] = db_round(cds)
        if re.compile("5'UTR_Exons").search(line):
            rseqc_dict["rseqc_five_utr"] = int(line.split()[2])
        if re.compile("3'UTR_Exons").search(line):
            rseqc_dict["rseqc_three_utr"] = int(line.split()[2])
        if re.compile("Introns").search(line):
            rseqc_dict["rseqc_intron"] = int(line.split()[2])
            intron = float(line.split()[-1])
            rseqc_dict["intron_rpk"] = db_round(intron)

    if rseqc_dict["intron_rpk"] > 0:
        exint_ratio = rseqc_dict["cds_rpk"] / rseqc_dict["intron_rpk"]
        rseqc_dict["exint_ratio"] = db_round(exint_ratio)
    else:
        rseqc_dict["exint_ratio"] = None

    return rseqc_dict


def scrape_preseq(paper_id, sample_name, data_path):
    """Scrape read length and depth from preseq complexity report.

    Parameters:
        paper_id (str) :
            paper identifier

        sample_name (str) :
            sample name derived from db query

        data_path (str) :
            path to database storage directory

    Returns:
        preseq_dict (dict) :
            scraped preseq metadata in dict format
    """
    preseq_dict = {}

    dirpath = data_path + paper_id + "/qc/preseq/"
    filepath = dirpath + sample_name + ".lc_extrap.txt"

    # If preseq complexity data doesn't exist, return null value
    if not (os.path.exists(filepath) and os.path.isfile(filepath)):
        preseq_dict["distinct_tenmillion_prop"] = None
        return preseq_dict

    fdata = open(filepath)
    for line in fdata:
        if line.startswith("10000000.0"):
            distinct = float(line.split()[1]) / 10000000

    preseq_dict["distinct_tenmillion_prop"] = round(distinct, 5)

    return preseq_dict


def scrape_pileup(paper_id, sample_name, data_path):
    """Scrape read length and depth from pileup report.

    Parameters:
        paper_id (str) :
            paper identifier

        sample_name (str) :
            sample name derived from db query

        data_path (str) :
            path to database storage directory

    Returns:
        pileup_dict (dict) :
            scraped pileup metadata in dict format
    """
    pileup_dict = {}

    dirpath = data_path + paper_id + "/qc/pileup/"
    filepath = dirpath + sample_name + ".coverage.stats.txt"

    # If pileup complexity data doesn't exist, return null value
    if not (os.path.exists(filepath) and os.path.isfile(filepath)):
        pileup_dict["genome_prop_cov"] = None
        pileup_dict["avg_fold_cov"] = None
        return pileup_dict

    # Add up reads in different categories to calculate coverage
    fdata = open(filepath)
    x = 0
    total = cov = fold = 0
    for line in fdata:
        if x == 0:
            x = x + 1
            continue
        else:
            x = x + 1
            total = total + int(line.split("\t")[2])
            cov = cov + int(line.split("\t")[5])
            fold = fold + (float(line.split("\t")[1])
                           * int(line.split("\t")[2]))

    # MySQL rounds strangely, so manual rounding required
    # for proper matching to database entries
    pileup_dict["genome_prop_cov"] = round((cov / total), 5)
    fold_cov = fold / total
    pileup_dict["avg_fold_cov"] = db_round(fold_cov)

    return pileup_dict


def scrape_all_qc(sampdict, datapath) -> dict:
    """Accumulate all scraped qc stats into one dict.

    Parameters:
        sampdict (dict):
            sample entry dict from metatable

        datapath (str) :
            path to data directory

    Returns:
        qc_all (dict) :
            calculated sample scores in dict format
    """
    ident = sampdict["paper_name"]
    samp = sampdict["sample_name"]
    fastqc_dict = scrape_fastqc(ident, samp, datapath, sampdict)
    picard_dict = scrape_picard(ident, samp, datapath)
    mapstats_dict = scrape_mapstats(ident, samp, datapath, sampdict)
    rseqc_dict = scrape_rseqc(ident, samp, datapath)
    preseq_dict = scrape_preseq(ident, samp, datapath)
    pileup_dict = scrape_pileup(ident, samp, datapath)

    qc_all = {
        **fastqc_dict,
        **picard_dict,
        **mapstats_dict,
        **rseqc_dict,
        **preseq_dict,
        **pileup_dict,
    }

    return qc_all


def sample_qc_calc(sample, thresholds) -> dict:
    """Calculate sample qc and data scores.

    Parameters:
        sample (dict) :
            sample dict from metatable object

        thresholds (dict) :
            dict of thresholds to use for qc and data score calc
            thresholds are given in dict format under keys for scores
            example threshold for qc score is:
                {qc5: [5000000, 0.95, 4000000, 0.05]}

    Returns:
        samp_score (dict) :
            calculated sample scores in dict format
    """
    # Define variables for qc score calc
    scores = dict()
    trimrd = sample["trim_read_depth"]
    dup = sample["duplication_picard"]
    mapped = sample["map_prop"]
    complexity = sample["distinct_tenmillion_prop"]

    # Determine sample QC score
    # All cutoffs based on manual inspection of data
    if (
        trimrd is None
        or dup is None
        or mapped is None
        or complexity is None
    ):
        scores["sample_qc_score"] = 0

    elif (
        trimrd <= thresholds["qc5"][0]
        or dup >= thresholds["qc5"][1]
        or (mapped * trimrd) <= thresholds["qc5"][2]
        or complexity < thresholds["qc5"][3]
    ):
        scores["sample_qc_score"] = 5

    elif (
        trimrd <= thresholds["qc4"][0]
        or dup >= thresholds["qc4"][1]
        or (mapped * trimrd) <= thresholds["qc4"][2]
        or complexity < thresholds["qc4"][3]
    ):
        scores["sample_qc_score"] = 4

    elif (
        trimrd <= thresholds["qc3"][0]
        or dup >= thresholds["qc3"][1]
        or (mapped * trimrd) <= thresholds["qc3"][2]
        or complexity < thresholds["qc3"][3]
    ):
        scores["sample_qc_score"] = 3

    elif (
        trimrd <= thresholds["qc2"][0]
        or dup >= thresholds["qc2"][1]
        or (mapped * trimrd) <= thresholds["qc2"][2]
        or complexity < thresholds["qc2"][3]
    ):
        scores["sample_qc_score"] = 2

    else:
        scores["sample_qc_score"] = 1

    # Find values for NRO score calc

    genome = sample["genome_prop_cov"]
    exint = sample["exint_ratio"]
    tfitgc = False
    if "tfit_bidir_gc_prop" in sample.keys():
        if sample["tfit_bidir_gc_prop"]:
            if sample["tfit_bidir_gc_prop"] != "None":
                tfitgc = float(sample["tfit_bidir_gc_prop"])

    if tfitgc:
        if (
            exint >= thresholds["nro5"][0]
            or tfitgc <= thresholds["nro5"][1]
        ):
            scores["sample_nro_score"] = 5

        elif (
            exint >= thresholds["nro4"][0]
            or tfitgc <= thresholds["nro4"][1]
        ):
            scores["sample_nro_score"] = 4

        elif (
            exint >= thresholds["nro3"][0]
            or tfitgc <= thresholds["nro3"][1]
        ):
            scores["sample_nro_score"] = 3

        elif (
            exint >= thresholds["nro2"][0]
            or tfitgc <= thresholds["nro2"][1]
        ):
            scores["sample_nro_score"] = 2

        else:
            scores["sample_nro_score"] = 1
    elif exint:
        if (exint >= thresholds["nro5"][0]):
            scores["sample_nro_score"] = 5
        elif (exint >= thresholds["nro4"][0]):
            scores["sample_nro_score"] = 4
        elif (exint >= thresholds["nro3"][0]):
            scores["sample_nro_score"] = 3
        elif (exint >= thresholds["nro2"][0]):
            scores["sample_nro_score"] = 2
        else:
            scores["sample_nro_score"] = 1
    else:
        scores["sample_nro_score"] = 0

    return scores


def paper_qc_calc(samples) -> dict:
    """Calculate paper qc and data scores.

    Parameters:
        db_samples (list of dicts) :
            sample data from metatable (incl qc and nro scores)

    Returns:
        paper_scores (dict) : 
            calculated median scores in dict format
    """
    qc_scores = []
    nro_scores = []
    paper_scores = {}

    for entry in samples:
        qc_scores.append(int(entry["sample_qc_score"]))
        nro_scores.append(int(entry["sample_nro_score"]))

    paper_scores["paper_qc_score"] = median(qc_scores)
    paper_scores["paper_nro_score"] = median(nro_scores)

    return paper_scores


def add_version_info(
    samples,
    data_path,
    vertype,
    db_keys
) -> list:
    """Find nascentflow/bidirflow version info for a paper.

    Parameters:
        samples (list of dicts) :
            list of samples from metatable

        data_path (str) :
            path to dbnascent data

        vertype (str) : {"nascent", "bidir"} :
            which nextflow type

        dbver_keys (list) :
            list of keys for version tables

    Returns:
        ver_table (list of dicts) :
            all relevant version info for entry into db
    """
    ver_table = []

    # If vertype is not in list of accepted, raise error
    if vertype not in ["nascent","bidir"]:
        raise ValueError(
            "vertype must be nascent or bidir"
        )

    for sample in samples:
        ver_path = (data_path + sample["paper_name"] + "/software_versions/" +
                    sample["sample_name"] + "_" + vertype + ".yaml")

        if not ((os.path.exists(ver_path) 
                 and os.path.isfile(ver_path))
               ):
            add_entry = dict()
            add_entry["sample_id"] = sample["sample_id"]
            for key in db_keys:
                add_entry.update({key: None})
            ver_table.append(add_entry)
            continue

        with open(ver_path) as f:
            for run in yaml.safe_load_all(f):
                add_entry = dict()
                add_entry["sample_id"] = sample["sample_id"]
                add_entry.update(run)
                for key in db_keys:
                    if not key in add_entry.keys():
                        add_entry.update({key: None})
                ver_table.append(add_entry)

    return ver_table


def add_bidir_info(
    samples,
    summary_path,
    caller,
    merge_ids,
    dbkeys,
) -> Metatable:
    """Find tfit/dreg summary info for a paper.

    Parameters:
        samples (metatable object):
            object to which to add bidir summary data

        summary_path (str) :
            path to paper bidir summary info

        caller (str) :
            which bidir caller ("tfit" or "dreg")

        merge_ids (list) :
            list of paper_id values incl in master merges

        dbkeys (list) :
            db bidir keys

    Returns:
        samples (metatable object) :
            object with bidir data appended
    """
    if os.path.exists(summary_path):
        summary = Metatable(summary_path)
    else:
        summary = Metatable([])

    for sample in samples.data:
        key_store_compare(
            sample,
            summary.data,
            ["sample_name",],
            [
                "num_" + caller +"_bidir",
                "num_" + caller +"_bidir_promoter",
                "num_" + caller +"_bidir_intronic",
                "num_" + caller +"_bidir_intergenic",
                caller + "_bidir_gc_prop",
            ],
            addnull=True,
        )
        # Format proportion correctly
        if sample[caller + "_bidir_gc_prop"]:
            sample[caller + "_bidir_gc_prop"] = str(float(sample[caller + "_bidir_gc_prop"]))
    
        # Add master file info
        if sample["paper_name"] in merge_ids:
            sample[caller + "_master_merge_incl"] = "1"
        else:
            sample[caller + "_master_merge_incl"] = "0"
        for bidirkey in dbkeys:
            if bidirkey in sample.keys():
                sample[bidirkey] = str(sample[bidirkey])

    return samples

# dbutils.py ends here
