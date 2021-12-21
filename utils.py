"""Functions for building and maintaining DBNascent.

Filename: utils.py
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

import os
import configparser
import sqlalchemy as sql
import csv
import re
import shutil
import zipfile as zp
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


def add_meta_columns(db_row, metatab, comp_keys, fields):
    """Extract db columns to add to metatable.
    
    Parameters:
        db_row (dict) : single row (entry) of a database query output

        metatab (Metatable object) : table to which to add fields

        comp_keys (dict) : specific keys for comparison as derived
                           from config file (db cols as keys and
                           metadata equivalent field names as values)

        fields (list) : list of fields to add from db to table

    Returns:
        metatab (Metatable object) : input table with added field
    """
    db_row_dict = object_as_dict(db_row)
    for entry in metatab:
        if value_compare(db_row_dict, entry, comp_keys):
            for field in fields:
                entry[field] = db_row_dict[field]

    return metatab


def scrape_fastqc(paper_id, sample_name, data_path, db_sample, dbconfig):
    """Scrape read length and depth from fastQC report.

    Parameters:
        paper_id (str) : paper identifier

        sample_name (str) : sample name derived from db query

        data_path (str) : path to database storage directory

        db_sample (dict) : sample_accum entry dict from db query

        dbconfig (configparser object) : config data

    Returns:
        fastqc_dict (dict) : scraped fastqc metadata in dict format
    """
    fastqc_dict = {}
    
    # Determine paths for raw fastQC file to scrape, depending on SE/PE
    fqc_path = data_path + paper_id + "/qc/fastqc/zips/"
    if db_sample[dbconfig["accum keys"]["single_paired"]] == "paired":
        samp_zip = dirpath + sample + "_1_fastqc"
    else:
        samp_zip = dirpath + sample + "_fastqc"

    # If fastQC files don't exist, return null values
    if not (os.path.exists(samp_zip + ".zip")):
        fastqc_dict["raw_read_depth"] = None
        fastqc_dict["raw_read_length"] = None
        fastqc_dict["trim_read_depth"] = None
        return fastqc_dict

    # Unzip fastQC report
    with zp.ZipFile(samp_zip + ".zip", "r") as zp_ref:
        zp_ref.extractall(dirpath)

    # Extract raw depth and read length
    fdata = open(samp_zip + "/fastqc_data.txt")
    for line in fdata:
        if re.compile("Total Sequences").search(line):
            fastqc_dict["raw_read_depth"] = int(line.split()[2])
        if re.compile("Sequence length").search(line):
            fastqc_dict["raw_read_length"] = int(line.split()[2].split("-")[0])

    # Remove unzipped file
    shutil.rmtree(samp_zip)

    # Determine paths for trimmed fastQC file to scrape, depending on SE/PE
    # and whether reverse complemented or not
    if db_sample[dbconfig["accum keys"]["rcomp"]] == 1:
        if db_sample[dbconfig["accum keys"]["single_paired"]] == "paired":
            samp_zip = dirpath + sample + "_1.flip.trim_fastqc"
        else:
            samp_zip = dirpath + sample + ".flip.trim_fastqc"
    else:
        if db_sample[dbconfig["accum keys"]["single_paired"]] == "paired":
            samp_zip = dirpath + sample + "_1.trim_fastqc"
        else:
            samp_zip = dirpath + sample + ".trim_fastqc"

    # If trimmed fastQC report doesn't exist, return null value for 
    # trimmed read depth
    if not (os.path.exists(samp_zip + ".zip")):
        fastqc_dict["trim_read_depth"] = None
        return fastqc_dict

    # Unzip trimmed fastQC report
    with zp.ZipFile(samp_zip + ".zip", "r") as zp_ref:
        zp_ref.extractall(dirpath)

    # Extract trimmed read depth
    fdata = open(samp_zip + "/fastqc_data.txt")
    for line in fdata:
        if re.compile("Total Sequences").search(line):
            fastqc_dict["trim_read_depth"] = int(line.split()[2])

    # Remove unzipped file
    shutil.rmtree(samp_zip)
    
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
            picard_dict["duplication_picard"] = float(line.split("\t")[8])


def scrape_mapstats(paper_id, sample_name, data_path, db_sample, dbconfig):
    """Scrape read length and depth from hisat2 mapstats report.

    Parameters:
        paper_id (str) : paper identifier

        sample_name (str) : sample name derived from db query

        data_path (str) : path to database storage directory

        db_sample (dict) : sample_accum entry dict from db query

        dbconfig (configparser object) : config data

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
    if db_sample[dbconfig["accum keys"]["single_paired"]] == "paired":
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
                mapstats_dict["map_prop"] = (
                    float(line.split(": ")[1].split("%")[0]) / 100
                )
    # Report mapped reads for single end data
    else:
        for line in fdata:
            if re.compile("Aligned 1 time").search(line):
                mapstats_dict["single_map"] = int(line.split(": ")[1].split(" (")[0])
            if re.compile("Aligned >1 times").search(line):
                mapstats_dict["multi_map"] = int(line.split(": ")[1].split(" (")[0])
            if re.compile("Overall alignment rate").search(line):
                mapstats_dict["map_prop"] = (
                    float(line.split(": ")[1].split("%")[0]) / 100
                )


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
            rseqc_dict["cds_rpk"] = float(line.split()[-1])
        if re.compile("5'UTR_Exons").search(line):
            rseqc_dict["rseqc_five_utr"] = int(line.split()[2])
        if re.compile("3'UTR_Exons").search(line):
            rseqc_dict["rseqc_three_utr"] = int(line.split()[2])
        if re.compile("Introns").search(line):
            rseqc_dict["rseqc_intron"] = int(line.split()[2])
            rseqc_dict["intron_rpk"] = float(line.split()[-1])

    if rseqc_dict["intron_rpk"] > 0:
        rseqc_dict["exint_ratio"] = (
            rseqc_dict["cds_rpk"] / rseqc_dict["intron_rpk"]
        )
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
            distinct = float(line.split()[1])

    preseq_dict["distinct_tenmillion_prop"] = distinct / 10000000

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

    pileup_dict["genome_prop_cov"] = cov / total
    pileup_dict["avg_fold_cov"] = fold / total

    return pileup_dict


def sample_qc_calc(db_sample):
    """Calculate sample qc and data scores.

    Parameters:
        db_sample (dict) : sample_accum entry dict from db query

    Returns:
        samp_score (int) : calculated sample scores in dict format
    """
    trimrd = db_sample["trim_read_depth"]
    dup = db_sample["duplication_picard"]
    mapped = db_sample["map_prop"]
    complexity = db_sample["distinct_tenmillion_prop"]
    genome = db_sample["genome_prop_cov"]
    exint = db_sample["exint_ratio"]

    # Determine sample QC score
    if (trimrd == None 
        or dup == None
        or mapped == None 
        or complexity == None):

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
    if (genome == None
        or exint == None):

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
        qc_scores.append(entry["samp_qc_score"])
        data_scores.append(entry["samp_data_score"])

    paper_scores["paper_qc_score"] = median(qc_scores)
    paper_scores["paper_data_score"] = median(data_scores)

    return paper_scores

#engine.execute(tablename.__table__.insert(),listofdicts)
#
# utils.py ends here