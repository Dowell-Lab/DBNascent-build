# orm.py --- ORM for DBNascent
#
# Filename: orm.py
# Description: ORM for DBNascent
# Authors: Lynn Sanford <lynn.sanford@colorado.edu> and Zach Maas
# Created: Mon Jun 10 13:11:55 2019 (-0600)
#

# Commentary:
#
# This file contains code for an ORM to interface with the Dowell
# Lab's Nascent Database.
#

# Code:

import sqlalchemy as sql
from sqlalchemy.ext.declarative import declarative_base

# Base class for our ORM
Base = declarative_base()


# MAIN TABLES

# Defines all organisms in the database
class organismInfo(Base):
    __tablename__ = "organismInfo"
    organism = sql.Column(
        sql.String(length=127),
        primary_key=True,
        index=True,
        unique=True,
    )
    genome_build = sql.Column(sql.String(length=50))
    genome_bases = sql.Column(sql.BigInteger)

# Reference table for unique values in database
class searchEquiv(Base):
    __tablename__ = "searchEquiv"
    search_term = sql.Column(
        sql.String(length=250),
        primary_key=True,
        index=True,
        unique=True,
    )
    db_term = sql.Column(sql.String(length=127))
    term_category = sql.Column(sql.String(length=50))

# Tissue and cancer designations for cell types
class tissueDetails(Base):
    __tablename__ = "tissueDetails"
    tissuedetail_id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
    )
    organism = sql.Column(
        sql.String(length=127),
        sql.ForeignKey("organismInfo.organism"),
    )
    sample_type = sql.Column(sql.String(length=127))
    cell_type = sql.Column(sql.String(length=127))
    tissue = sql.Column(sql.String(length=127))
    cell_origin_type = sql.Column(sql.String(length=127))
    tissue_description = sql.Column(sql.String(length=127))
    disease = sql.Column(sql.Boolean)

# Paper-level metadata common to most samples in paper
class exptMetadata(Base):
    __tablename__ = "exptMetadata"
    expt_id = sql.Column(sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
    )
    srp = sql.Column(sql.String(length=50))
    protocol = sql.Column(sql.String(length=50))
    organism = sql.Column(
        sql.String(length=127),
        sql.ForeignKey("organismInfo.organism"),
    )
    library = sql.Column(sql.String(length=50))
    spikein = sql.Column(sql.String(length=127))
    paper_id = sql.Column(
        sql.String(length=127),
        index=True,
    )
    published = sql.Column(sql.Boolean)
    year = sql.Column(sql.Integer)
    first_author = sql.Column(sql.String(length=127))
    last_author = sql.Column(sql.String(length=127))
    doi = sql.Column(sql.String(length=300))
    curator1 = sql.Column(sql.String(length=50))
    curator2 = sql.Column(sql.String(length=50))
    other_sr_data = sql.Column(sql.Boolean)
    atac_seq = sql.Column(sql.Boolean)
    rna_seq = sql.Column(sql.Boolean)
    chip_seq = sql.Column(sql.Boolean)
    three_dim_seq = sql.Column(sql.Boolean)
    other_seq = sql.Column(sql.Boolean)
    paper_qc_score = sql.Column(sql.Float)
    paper_data_score = sql.Column(sql.Float)

# All SRR numbers, and equivalence to SRZ values and sample IDs
class sampleID(Base):
    __tablename__ = "sampleID"
    srr = sql.Column(sql.String(length=50),
        primary_key=True,
        index=True,
        unique=True,
    )
    sample_name = sql.Column(
        sql.String(length=50),
        index=True,
    )
    sample_id = sql.Column(
        sql.Integer,
        index=True,
    )

# Genetic info, including organism, sample type, cell type, and
# any genetic modifications of note
class geneticInfo(Base):
    __tablename__ = "geneticInfo"
    genetic_id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
    )
    organism = sql.Column(
        sql.String(length=127),
        sql.ForeignKey("organismInfo.organism"),
    )
    sample_type = sql.Column(
        sql.String(length=127),
        sql.ForeignKey("tissueDetails.sample_type"),
    )
    cell_type = sql.Column(
        sql.String(length=127),
        sql.ForeignKey("tissueDetails.cell_type"),
    )
    sample_type = sql.Column(sql.String(length=127))
    cell_type = sql.Column(sql.String(length=127))
    clone_individual = sql.Column(sql.String(length=127))
    strain = sql.Column(sql.String(length=127))
    genotype = sql.Column(sql.String(length=127))
    construct = sql.Column(sql.String(length=127))

# Summary stats for bidirectionals
class bidirSummary(Base):
    __tablename__ = "bidirSummary"
    bidirsummary_id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
    )
    num_tfit_bidir = sql.Column(sql.Integer)
    num_tfit_bidir_promoter = sql.Column(sql.Integer)
    num_tfit_bidir_intronic = sql.Column(sql.Integer)
    num_tfit_bidir_intergenic = sql.Column(sql.Integer)
    num_dreg_bidir = sql.Column(sql.Integer)
    num_dreg_bidir_promoter = sql.Column(sql.Integer)
    num_dreg_bidir_intronic = sql.Column(sql.Integer)
    num_dreg_bidir_intergenic = sql.Column(sql.Integer)
    tfit_bidir_gc_prop = sql.Column(sql.Float)
    dreg_bidir_gc_prop = sql.Column(sql.Float)
    tfit_master_merge_incl = sql.Column(sql.Boolean)
    dreg_master_merge_incl = sql.Column(sql.Boolean)

# Treatment information
class conditionInfo(Base):
    __tablename__ = "conditionInfo"
    condition_id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
    )
    condition_type = sql.Column(sql.String(length=127))
    treatment = sql.Column(sql.String(length=127))
    conc_intens = sql.Column(sql.String(length=50))
    start_time = sql.Column(sql.Integer)
    end_time = sql.Column(sql.Integer)
    time_unit = sql.Column(sql.String(length=50))
    duration = sql.Column(sql.Integer)
    duration_unit = sql.Column(sql.String(length=50))

# Linkage table of each sample to treatment(s)
class sampleCondition(Base):
    __tablename__ = "sampleCondition"
    condition_match_id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
    )
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey(sampleID.sample_id),
    )
    condition_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("conditionInfo.condition_id"),
    )

# Main linkage table between sample, genetic, and expt IDs
class linkIDs(Base):
    __tablename__ = "linkIDs"
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("sampleID.sample_id"),
        primary_key=True,
        index=True,
        unique=True,
    )
    genetic_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("geneticInfo.genetic_id"),
    )
    expt_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("exptMetadata.expt_id"),
    )
    bidirsummary_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("bidirSummary.bidirsummary_id"),
    )
    sample_name = sql.Column(
        sql.String(length=127),
        sql.ForeignKey("sampleID.sample_name"),
    )
    paper_id = sql.Column(
        sql.String(length=127),
        sql.ForeignKey("exptMetadata.paper_id"),
    )

# All sample-specific information, including QC data and notes
class sampleAccum(Base):
    __tablename__ = "sampleAccum"
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("sampleID.sample_id"),
        primary_key=True,
        index=True,
        unique=True,
    )
    replicate = sql.Column(sql.String(length=50))
    single_paired = sql.Column(sql.String(length=50))
    rcomp = sql.Column(sql.Boolean)
    unusable = sql.Column(sql.Boolean)
    timecourse = sql.Column(sql.Boolean)
    control_experimental = sql.Column(sql.String(length=50))
    outlier = sql.Column(sql.Boolean)
    notes = sql.Column(sql.String(length=300))
    processing_notes = sql.Column(sql.String(length=300))
    raw_read_depth = sql.Column(sql.Integer)
    trim_read_depth = sql.Column(sql.Integer)
    raw_read_length = sql.Column(sql.Integer)
    duplication_picard = sql.Column(sql.Float)
    single_map = sql.Column(sql.Integer)
    multi_map = sql.Column(sql.Integer)
    map_prop = sql.Column(sql.Float)
    rseqc_tags = sql.Column(sql.Integer)
    rseqc_cds = sql.Column(sql.Integer)
    rseqc_five_utr = sql.Column(sql.Integer)
    rseqc_three_utr = sql.Column(sql.Integer)
    rseqc_intron = sql.Column(sql.Integer)
    cds_rpk = sql.Column(sql.Float)
    intron_rpk = sql.Column(sql.Float)
    exint_ratio = sql.Column(sql.Float)
    distinct_tenmillion_prop = sql.Column(sql.Float)
    genome_prop_cov = sql.Column(sql.Float)
    avg_fold_cov = sql.Column(sql.Float)
    samp_qc_score = sql.Column(sql.Integer)
    samp_data_score = sql.Column(sql.Integer)

# Version information for nascentflow runs
class nascentflowMetadata(Base):
    __tablename__ = "nascentflowMetadata"
    nascentflow_id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
    )
    nascentflow_version = sql.Column(sql.String(length=127))
    downfile_version = sql.Column(sql.String(length=127))
    pipeline_revision_hash = sql.Column(sql.String(length=127))
    pipeline_hash = sql.Column(sql.String(length=127))
    nascentflow_date = sql.Column(sql.Date)
    nascentflow_redo_date = sql.Column(sql.Date)
    downfile_pipeline_date = sql.Column(sql.Date)
    nextflow_version = sql.Column(sql.String(length=127))
    fastqc_version = sql.Column(sql.String(length=127))
    bbmap_version = sql.Column(sql.String(length=127))
    hisat2_version = sql.Column(sql.String(length=127))
    samtools_version = sql.Column(sql.String(length=127))
    sratools_version = sql.Column(sql.String(length=127))
    preseq_version = sql.Column(sql.String(length=127))
    preseq_date = sql.Column(sql.Date)
    rseqc_version = sql.Column(sql.String(length=127))
    rseqc_date = sql.Column(sql.Date)
    java_version = sql.Column(sql.String(length=127))
    picard_gc_version = sql.Column(sql.String(length=127))
    picard_dups_version = sql.Column(sql.String(length=127))
    picard_date = sql.Column(sql.Date)
    bedtools_version = sql.Column(sql.String(length=127))
    igvtools_version = sql.Column(sql.String(length=127))
    seqkit_version = sql.Column(sql.String(length=127))
    mpich_version = sql.Column(sql.String(length=127))
    gcc_version = sql.Column(sql.String(length=127))
    python_version = sql.Column(sql.String(length=127))
    numpy_version = sql.Column(sql.String(length=127))

# Linkage table of each sample to nascentflow run(s)
class sampleNascentflow(Base):
    __tablename__ = "sampleNascentflow"
    nascentver_match_id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
    )
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey(sampleID.sample_id),
    )
    nascentflow_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("nascentflowMetadata.nascentflow_id"),
    )

# Version information for bidirectionalflow runs
class bidirflowMetadata(Base):
    __tablename__ = "bidirflowMetadata"
    bidirflow_id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
    )
    bidirflow_version = sql.Column(sql.String(length=127))
    pipeline_revision_hash = sql.Column(sql.String(length=127))
    pipeline_hash = sql.Column(sql.String(length=127))
    bidirflow_date = sql.Column(sql.Date)
    nextflow_version = sql.Column(sql.String(length=127))
    samtools_version = sql.Column(sql.String(length=127))
    bedtools_version = sql.Column(sql.String(length=127))
    mpich_version = sql.Column(sql.String(length=127))
    openmpi_version = sql.Column(sql.String(length=127))
    gcc_version = sql.Column(sql.String(length=127))
    r_version = sql.Column(sql.String(length=127))
    rsubread_version = sql.Column(sql.String(length=127))
    boost_version = sql.Column(sql.String(length=127))
    fstitch_version = sql.Column(sql.String(length=127))
    tfit_version = sql.Column(sql.String(length=127))
    dreg_version = sql.Column(sql.String(length=127))
    dreg_date = sql.Column(sql.Date)
    dreg_postprocess_date = sql.Column(sql.Date)
    tfit_date = sql.Column(sql.Date)
    tfit_prelim_date = sql.Column(sql.Date)
    fcgene_date = sql.Column(sql.Date)
    fcbidir_date = sql.Column(sql.Date)

# Linkage table of each sample to bidirectionalflow run(s)
class sampleBidirflow(Base):
    __tablename__ = "sampleBidirflow"
    bidirver_match_id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
    )
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey(sampleID.sample_id),
    )
    bidirflow_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("bidirflowMetadata.bidirflow_id"),
    )

# dborm.py ends here
