"""Functions for building and maintaining DBNascent.

Filename: dbutils.py
Authors: Lynn Sanford <lynn.sanford@colorado.edu> and Zach Maas

Commentary:
    This module contains utility functions and classes for
    reducing the total amount of code needed for building and
    updating the database

Classes:
    dbnascentConnection
    Metatable

Functions:
    load_config(file) -> object
    add_tables(db_url)
    table_parse(file) -> list of dicts
    key_grab(dict, list) -> list of lists
    get_unique_table(file, list) -> dict
    value_compare(object, dict, dict)
    object_as_dict(object)
    scrape_fastqc(object) -> list of dicts

Misc variables:
"""

import configparser
import csv
import numpy as np
import os
import re
import pymysql
import sqlalchemy as sql
from sqlalchemy.ext.serializer import loads, dumps
from sqlalchemy.orm import sessionmaker
import shutil
from statistics import median
import yaml
import zipfile as zp
import dborm


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

    def reflect_table(self, table, filter_crit=None) -> list:
        """Query all records from a specific table.

        Can optionally add filtering criteria.

        Parameters:
            table (str) : string of table name from ORM

            filter_crit (dict) : filter criteria for table

        Returns:
            query_data (list of dicts) : all data in table
                                         matching filter criteria
        """
        query_data = []

        query_str = "SELECT * FROM " + table
        if filter_crit is not None:
            query_str = query_str + " WHERE "
            i = 0
            for key in filter_crit:
                if i == 0:
                    query_str = (query_str + str(key) +
                                 ' = "' + str(filter_crit[key]) + '"')
                    i = (i + 1)
                else:
                    query_str = (query_str + " AND " + str(key) +
                                 ' = "' + str(filter_crit[key]) + '"')

        sqlquery = self.session.execute(sql.text(query_str)).fetchall()

        for entry in sqlquery:
            query_data.append(dict(entry))

        return query_data

    def backup(self, out_path, tables=False):
        """Backup database (whole or specific tables).

        Parameters:
            out_path (str) : path to backup file directory

            tables (list) : list of specific tables, if whole
                            database backup is not desired

        Returns:
            none
        """
        if not tables:
            dborm.Base.metadata.reflect(bind=self.engine)
            tables = list(dborm.Base.metadata.tables.keys())
        for table in tables:
            outfile = out_path + "/" + table + ".dbdump"
            q = self.session.query(table)
            serialized_data = dumps(q.all())
            with open(outfile, 'w') as out:
                out.write(str(serialized_data))

    def restore(self, in_path, tables):
        """Restore database (whole or specific tables).

        Parameters:
            in_path (str) : path to backup file directory

            tables (list) : list of specific tables, if whole
                            database backup is not desired

        Returns:
            none
        """
        if not tables:
            files = os.listdir(in_path)
            tables = []
            for file in files:
                tables.append(file.split(".")[0])
        for table in tables:
            infile = in_path + "/" + table + ".dbdump"
            with open(infile) as f:
                serialized_data = dict(f)
            self.session.merge(serialized_data)

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

    def __init__(self, meta_path, dictlist=None):
        """Initialize metatable object.

        Parameters:
            meta_path (str) : path to metadata file
                file must be tab-delimited with field names as header

            dictlist (list of dicts) : if not path, list of dicts
                this can convert a list of dicts into the self.data of
                a metatable object
        """
        self.data = []

        if meta_path:
            self.load_file(meta_path)
        elif dictlist:
            self.data = dictlist

    def load_file(self, meta_path):
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
            full_table = list(csv.DictReader(metatab, delimiter="\t"))
            if len(full_table[0]) == 1:
                raise IndexError(
                    "Input must be tab-delimited. Double check input."
                )
            else:
                for entry in full_table:
                    self.data.append(dict(entry))

    def key_replace(self, file_keys, db_keys):
        """Replace file keys with database keys.

        Parameters:
            file_keys (list) : list of keys in file

            db_keys (list) : list of keys in database
                Must be equivalent in length to file_keys

        Returns:
            self.data (list of dicts)
        """
        # Check if keys are valid
        for key in file_keys:
            if key not in self.data[0]:
                raise KeyError(
                    "Key(s) not present in metatable object."
                )

        for entry in self.data:
            for i in range(len(file_keys)):
                entry[db_keys[i]] = entry.pop(file_keys[i])

    def value_grab(self, key_list) -> list:
        """Extract values for specific keys from metatable data.

        Parameters:
            key_list (list) : desired keys from dicts in table_list

        Returns:
            value_list (list of lists) : each entry containing the values
                                         of the given keys
        """
        # Load in file as a list of dicts
        value_list = []

        if len(self.data) == 0:
            return value_list

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

    def key_grab(self, key_list) -> list:
        """Extract dicts with specific keys from metatable data.

        Parameters:
            key_list (list) : desired keys from dicts in table_list

        Returns:
            dict_list (list of dicts) : each entry containing the dicts
                                        with only the given keys
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

        for entry in self.data:
            newentry = dict()
            for key in key_list:
                newentry[key] = entry[key]
            dict_list.append(newentry)

        return dict_list

    def unique(self, extract_keys) -> list:
        """Extract values for specific keys from a metatable filepath.

        Parameters:
            extract_keys (list) : list containing db key labels for binding

        Returns:
            unique_metatable (list of dicts) : each entry contains the values
                                               of the extract keys; only
                                               returns unique sets of values
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

        full_table_list = np.array(self.value_grab(extract_keys))
        unique_list = np.unique(full_table_list, axis=0)

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


def listdict_compare(comp_dict, db_dict, db_keys) -> list:
    """Compare two lists of dicts and take any rows not already in db.

    Converts all values to strings for comparison purposes

    Parameters:
        comp_dict (list of dicts) : list of dicts from metatable object

        db_dict (list of dicts) : list of dicts extracted from db query

        db_keys (list) : specific keys for comparison

    Returns:
        data_to_add (list of dicts) : any dicts in comp_dict not in db_dict
    """
    data_to_add = []

    for entry in comp_dict:
        for key in db_keys:
            entry[key] = str(entry[key])

    for entry in db_dict:
        for key in db_keys:
            entry[key] = str(entry[key])

    for comp_entry in comp_dict:
        if comp_entry not in db_dict:
            data_to_add.append(comp_entry)

    return data_to_add


def key_store_compare(comp_dict, db_dict, db_keys, store_keys) -> list:
    """Compare two lists of dicts and take any rows not already in db.

    Converts all values to strings for comparison purposes

    Parameters:
        comp_dict (dict) : single dict from metatable object

        db_dict (list of dicts) : list of dicts extracted from db query

        db_keys (list) : specific keys for comparison

        store_keys (list) : key(s) for adding to dict

    Returns:
        new_dict (dict) : dict with new value added
    """
    for dbentry in db_dict:
        comp = 0
        for key in db_keys:
            if str(comp_dict[key]) != str(dbentry[key]):
                if str(comp_dict[key]) == "":
                    if dbentry[key] != None:
                        comp = 1
                else:
                    comp = 1
        if comp == 0:
            for stkey in store_keys:
                comp_dict[stkey] = dbentry[stkey]

    return comp_dict


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


def entry_update(dbconn, dbtable, dbkeys, comp_table) -> list:
    """Find entries not already in database.

    Parameters:
        dbconn (dbnascentConnection object) : curr db connection

        dbtable (str) : Which db table to search for entries

        dbkeys (list) : list of keys to use for comparison

        comp_table (list of dicts) : Entries to match (or not)
                                     to db entries

    Returns:
        to_add (list of dicts) : New entries not in db to add
    """
    db_dump = dbconn.reflect_table(dbtable)
    dbtab = Metatable(meta_path=None, dictlist=db_dump)
    dbtab_data = dbtab.key_grab(dbkeys)
    to_add = listdict_compare(comp_table, dbtab_data, dbkeys)

    return to_add


def scrape_fastqc(paper_id, sample_name, data_path, db_sample) -> dict:
    """Scrape read length and depth from fastQC report.

    Parameters:
        paper_id (str) : paper identifier

        sample_name (str) : sample name

        data_path (str) : path to database storage directory

        db_sample (dict) : sample_accum entry dict from db query

    Returns:
        fastqc_dict (dict) : scraped fastqc metadata in dict format
    """
    fastqc_dict = {}

    # Determine paths for raw fastQC file to scrape, depending on SE/PE
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

    # Determine paths for trimmed fastQC file to scrape, depending on SE/PE
    # and whether reverse complemented or not
    if str(db_sample["rcomp"]) == '1':
        if db_sample["single_paired"] == "paired":
            samp_zip = fqc_path + sample_name + "_1.flip.trim_fastqc"
        else:
            samp_zip = fqc_path + sample_name + ".flip.trim_fastqc"
    else:
        if db_sample["single_paired"] == "paired":
            samp_zip = fqc_path + sample_name + "_1.trim_fastqc"
        else:
            samp_zip = fqc_path + sample_name + ".trim_fastqc"

    # If trimmed fastQC report doesn't exist, return null value for
    # trimmed read depth
    if not (os.path.exists(samp_zip + ".zip")):
        fastqc_dict["trim_read_depth"] = None
        return fastqc_dict

    # Unzip trimmed fastQC report
    with zp.ZipFile(samp_zip + ".zip", "r") as zp_ref:
        zp_ref.extractall(fqc_path)

    # Extract trimmed read depth
    fdata = open(samp_zip + "/fastqc_data.txt")
    for line in fdata:
        if re.compile("Total Sequences").search(line):
            fastqc_dict["trim_read_depth"] = int(line.split()[2])

    # Remove unzipped file
    shutil.rmtree((samp_zip + "/"), ignore_errors=True)

    return fastqc_dict


def scrape_picard(paper_id, sample_name, data_path):
    """Scrape read length and depth from picard duplication report.

    Parameters:
        paper_id (str) : paper identifier

        sample_name (str) : sample name derived from db query

        data_path (str) : path to database storage directory

    Returns:
        picard_dict (dict) : scraped picard metadata in dict format
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
        paper_id (str) : paper identifier

        sample_name (str) : sample name derived from db query

        data_path (str) : path to database storage directory

        db_sample (dict) : sample_accum entry dict from db query

    Returns:
        mapstats_dict (dict) : scraped hisat2 metadata in dict format
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
        paper_id (str) : paper identifier

        sample_name (str) : sample name derived from db query

        data_path (str) : path to database storage directory

    Returns:
        rseqc_dict (dict) : scraped RSeQC metadata in dict format
    """
    rseqc_dict = {}

    dirpath = data_path + paper_id + "/qc/rseqc/read_distribution/"
    filepath = dirpath + sample_name + ".read_distribution.txt"

    # If rseqc read distribution data doesn't exist, return null values
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
    fdata = open(filepath)
    for line in fdata:
        if re.compile("Total Assigned Tags").search(line):
            rseqc_dict["rseqc_tags"] = int(line.split()[-1])
        if re.compile("CDS_Exons").search(line):
            rseqc_dict["rseqc_cds"] = int(line.split()[2])
            cds = float(line.split()[-1])
            if cds > 99999:
                rseqc_dict["cds_rpk"] = round(cds, 0)
            elif cds > 9999:
                rseqc_dict["cds_rpk"] = round(cds, 1)
            elif cds > 999:
                rseqc_dict["cds_rpk"] = round(cds, 2)
            elif cds > 99:
                rseqc_dict["cds_rpk"] = round(cds, 3)
            elif cds > 9:
                rseqc_dict["cds_rpk"] = round(cds, 4)
            else:
                rseqc_dict["cds_rpk"] = round(cds, 5)
        if re.compile("5'UTR_Exons").search(line):
            rseqc_dict["rseqc_five_utr"] = int(line.split()[2])
        if re.compile("3'UTR_Exons").search(line):
            rseqc_dict["rseqc_three_utr"] = int(line.split()[2])
        if re.compile("Introns").search(line):
            rseqc_dict["rseqc_intron"] = int(line.split()[2])
            intron = float(line.split()[-1])
            if intron > 99999:
                rseqc_dict["intron_rpk"] = round(intron, 0)
            elif intron > 9999:
                rseqc_dict["intron_rpk"] = round(intron, 1)
            elif intron > 999:
                rseqc_dict["intron_rpk"] = round(intron, 2)
            elif intron > 99:
                rseqc_dict["intron_rpk"] = round(intron, 3)
            elif intron > 9:
                rseqc_dict["intron_rpk"] = round(intron, 4)
            else:
                rseqc_dict["intron_rpk"] = round(intron, 5)

    if rseqc_dict["intron_rpk"] > 0:
        exint_ratio = rseqc_dict["cds_rpk"] / rseqc_dict["intron_rpk"]
        if exint_ratio > 99999:
            rseqc_dict["exint_ratio"] = round(exint_ratio, 0)
        elif exint_ratio > 9999:
            rseqc_dict["exint_ratio"] = round(exint_ratio, 1)
        elif exint_ratio > 999:
            rseqc_dict["exint_ratio"] = round(exint_ratio, 2)
        elif exint_ratio > 99:
            rseqc_dict["exint_ratio"] = round(exint_ratio, 3)
        elif exint_ratio > 9:
            rseqc_dict["exint_ratio"] = round(exint_ratio, 4)
        else:
            rseqc_dict["exint_ratio"] = round(exint_ratio, 5)
    else:
        rseqc_dict["exint_ratio"] = None

    return rseqc_dict


def scrape_preseq(paper_id, sample_name, data_path):
    """Scrape read length and depth from preseq complexity report.

    Parameters:
        paper_id (str) : paper identifier

        sample_name (str) : sample name derived from db query

        data_path (str) : path to database storage directory

    Returns:
        preseq_dict (dict) : scraped preseq metadata in dict format
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
        paper_id (str) : paper identifier

        sample_name (str) : sample name derived from db query

        data_path (str) : path to database storage directory

    Returns:
        pileup_dict (dict) : scraped pileup metadata in dict format
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

    pileup_dict["genome_prop_cov"] = round((cov / total), 5)
    fold_cov = fold / total
    if fold_cov > 99999:
        pileup_dict["avg_fold_cov"] = round(fold_cov, 0)
    elif fold_cov > 9999:
        pileup_dict["avg_fold_cov"] = round(fold_cov, 1)
    elif fold_cov > 999:
        pileup_dict["avg_fold_cov"] = round(fold_cov, 2)
    elif fold_cov > 99:
        pileup_dict["avg_fold_cov"] = round(fold_cov, 3)
    elif fold_cov > 9:
        pileup_dict["avg_fold_cov"] = round(fold_cov, 4)
    else:
        pileup_dict["avg_fold_cov"] = round(fold_cov, 5)

    return pileup_dict


def sample_qc_calc(db_sample):
    """Calculate sample qc and data scores.

    Parameters:
        db_sample (dict) : sample_accum entry dict from db query

    Returns:
        samp_score (int) : calculated sample scores in dict format
    """
    samp_score = dict()
    trimrd = db_sample["trim_read_depth"]
    dup = db_sample["duplication_picard"]
    mapped = db_sample["map_prop"]
    complexity = db_sample["distinct_tenmillion_prop"]
    genome = db_sample["genome_prop_cov"]
    exint = db_sample["exint_ratio"]

    # Determine sample QC score
    if (trimrd is None
       or dup is None
       or mapped is None
       or complexity is None):

        samp_score["samp_qc_score"] = 0

    elif (trimrd <= 5000000
          or dup >= 0.95
          or (mapped * trimrd) <= 4000000
          or complexity < 0.05):

        samp_score["samp_qc_score"] = 5

    elif (trimrd <= 10000000
          or dup >= 0.80
          or (mapped * trimrd) <= 8000000
          or complexity < 0.2):

        samp_score["samp_qc_score"] = 4

    elif (trimrd <= 15000000
          or dup >= 0.65
          or (mapped * trimrd) <= 12000000
          or complexity < 0.35):

        samp_score["samp_qc_score"] = 3

    elif (trimrd <= 20000000
          or dup >= 0.5
          or (mapped * trimrd) <= 16000000
          or complexity < 0.5):

        samp_score["samp_qc_score"] = 2

    else:
        samp_score["samp_qc_score"] = 1

    # Determine sample data score
    if (genome is None
       or exint is None):

        samp_score["samp_data_score"] = 0

    elif (genome <= 0.04
          or exint >= 9):

        samp_score["samp_data_score"] = 5

    elif (genome <= 0.08
          or exint >= 7):

        samp_score["samp_data_score"] = 4

    elif (genome <= 0.12
          or exint >= 5):

        samp_score["samp_data_score"] = 3

    elif (genome <= 0.16
          or exint >= 3):
        samp_score["samp_data_score"] = 2

    else:
        samp_score["samp_data_score"] = 1

    return samp_score


def paper_qc_calc(db_samples):
    """Calculate sample qc and data scores.

    Parameters:
        db_samples (list of dicts) : sample_accum entries from db query

    Returns:
        paper_scores (float) : calculated median scores in dict format
    """
    qc_scores = []
    data_scores = []
    paper_scores = {}

    for entry in db_samples:
        qc_scores.append(int(entry["samp_qc_score"]))
        data_scores.append(int(entry["samp_data_score"]))

    paper_scores["paper_qc_score"] = median(qc_scores)
    paper_scores["paper_data_score"] = median(data_scores)

    return paper_scores


def add_version_info(dbconn, paper_id, data_path, vertype, dbver_keys):
    """Find nascentflow/bidirflow version info for a paper.

    Parameters:
        dbconn (dbnascentConnection object) : curr db connection

        paper_id (str) : paper identifier

        data_path (str) : path to dbnascent data

        vertype (str) : {"nascent", "bidir"} : Which nextflow type

        dbver_keys (list) : list of keys for version tables

    Returns:
        ver_table (list of dicts) : all relevant version info for
                                      entry into db
    """
    ver_table = []

    dblink_dump = dbconn.reflect_table("linkIDs", {"paper_id": paper_id})
    for entry in dblink_dump:
        del entry["genetic_id"]
        del entry["expt_id"]
        ver_path = (data_path + paper_id + "/software_versions/" +
                    entry["sample_name"] + "_" + vertype + ".yaml")

        if not (os.path.exists(ver_path) and os.path.isfile(ver_path)):
            for key in dbver_keys:
                entry.update({key: None})
            ver_table.append(entry)
            continue

        with open(ver_path) as f:
            for run in yaml.safe_load_all(f):
                add_entry = dict()
                add_entry.update(entry)
                add_entry.update(run)
                for key in dbver_keys:
                    if not key in add_entry.keys():
                        add_entry.update({key: None})
                ver_table.append(add_entry)

    return ver_table


def dbnascent_backup(db, basedir, tables):
    """Create new database backup.

    Parameters:
        db (dbnascentConnection object) : current database connection

        basedir (str) : path to base backup directory
                        default /home/lsanford/Documents/data/dbnascent_backups

        tables (list) : list of specific tables if whole db backup
                        is not desired

    Returns:
        none
    """
    if not basedir:
        basedir = "/home/lsanford/Documents/data/dbnascent_backups"
    now = datetime.datetime.now()
    nowdir = now.strftime("%Y%m%d_%H%M%S")
    os.makedirs(basedir + "/" + nowdir)

    if tables:
        db.backup((basedir + "/" + nowdir), tables)
    else:
        db.backup((basedir + "/" + nowdir))


def paper_add_update(db, config, identifier, basedir):
    """Add or update paper and associated sample metadata.

    Parameters:
        db (dbnascentConnection object) : current database connection

        config (configParser object) : parsed config file

        identifier (str) : paper identifier, used to locate all (meta)data

        basedir (str) : path to base database data directory
                        default /Shares/dbnascent

    Returns:
        none
    """
    # Add experimental metadata
    expt_keys = list(dict(config["expt keys"]).values())
    if not basedir:
        basedir = "/Shares/dbnascent"
    exptmeta_path = basedir + "/" + identifier + "/"

    # Read in expt metadata and make sure entries are unique
    exptmeta = utils.Metatable(exptmeta_path + "metadata/expt_metadata.txt")
    expt_unique = exptmeta.unique(expt_keys)

    # Add expt metadata to database
    db.engine.execute(exptMetadata.__table__.insert(), expt_unique.data())

    # Add sample ids


#engine.execute(tablename.__table__.insert(),listofdicts)
#
# dbutils.py ends here
