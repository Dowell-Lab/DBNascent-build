# orm.py --- ORM for DBNascent
#
# Filename: orm.py
# Description: ORM for DBNascent
# Authors: Zach Maas and Lynn Sanford
# Maintainer: Lynn Sanford <lynn.sanford@colorado.edu>
# Created: Mon Jun 10 13:11:55 2019 (-0600)
# URL:
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
class organismInfo(Base):
    __tablename__ = "organismInfo"
    organism = sql.Column(
        sql.String(length=127), primary_key=True, index=True, unique=True
    )
    genome_build = sql.Column(sql.String(length=50))
    genome_bases = sql.Column(sql.Integer)


class searchEq(Base):
    __tablename__ = "searchEq"
    search_term = sql.Column(
        sql.String(length=250), primary_key=True, index=True, unique=True
    )
    db_term = sql.Column(sql.String(length=127))


class exptMetadata(Base):
    __tablename__ = "exptMetadata"
    expt_id = sql.Column(sql.Integer,
                         primary_key=True,
                         index=True,
                         unique=True)
    srp = sql.Column(sql.String(length=50))
    protocol = sql.Column(sql.String(length=50))
    organism = sql.Column(
        sql.String(length=127), sql.ForeignKey("organismInfo.organism")
    )
    library = sql.Column(sql.String(length=50))
    spikein = sql.Column(sql.String(length=127))
    paper_id = sql.Column(sql.String(length=127))
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


class sampleID(Base):
    __tablename__ = "sampleID"
    srr = sql.Column(sql.String(length=50),
                     primary_key=True,
                     index=True,
                     unique=True)
    sample_name = sql.Column(sql.String(length=50))
    sample_id = sql.Column(sql.Integer)


class geneticInfo(Base):
    __tablename__ = "geneticInfo"
    genetic_id = sql.Column(sql.Integer,
                            primary_key=True,
                            index=True,
                            unique=True)
    organism = sql.Column(
        sql.String(length=127), sql.ForeignKey("organismInfo.organism")
    )
    sample_type = sql.Column(sql.String(length=127))
    cell_type = sql.Column(sql.String(length=127))
    clone_individual = sql.Column(sql.String(length=127))
    strain = sql.Column(sql.String(length=127))
    genotype = sql.Column(sql.String(length=127))
    construct = sql.Column(sql.String(length=127))


class conditionInfo(Base):
    __tablename__ = "conditionInfo"
    condition_id = sql.Column(sql.Integer,
                              primary_key=True,
                              index=True,
                              unique=True)
    condition_type = sql.Column(sql.String(length=127))
    treatment = sql.Column(sql.String(length=127))
    conc_intens = sql.Column(sql.String(length=50))
    start_time = sql.Column(sql.Integer)
    end_time = sql.Column(sql.Integer)
    duration = sql.Column(sql.Integer)
    time_unit = sql.Column(sql.String(length=50))
    duration_unit = sql.Column(sql.String(length=50))


exptCondition = sql.Table(
    "exptCondition",
    Base.metadata,
    sql.Column("sample_id",
               sql.Integer,
               sql.ForeignKey("sampleID.sample_id")),
    sql.Column("condition_id",
               sql.Integer,
               sql.ForeignKey("conditionInfo.condition_id")),
)


class linkIDs(Base):
    __tablename__ = "linkIDs"
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("sampleID.sample_id"),
        primary_key=True,
        index=True,
        unique=True,
    )
    genetic_id = sql.Column(sql.Integer,
                            sql.ForeignKey("geneticInfo.genetic_id"))
    expt_id = sql.Column(sql.Integer,
                         sql.ForeignKey("exptMetadata.expt_id"))


class sampleAccum(Base):
    __tablename__ = "sampleAccum"
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("sampleID.sample_id"),
        primary_key=True,
        index=True,
        unique=True,
    )
    replicate = sql.Column(sql.Integer)
    single_paired = sql.Column(sql.String(length=50))
    rcomp = sql.Column(sql.Boolean)
    expt_unusable = sql.Column(sql.Boolean)
    timecourse = sql.Column(sql.Boolean)
    baseline_control_expt = sql.Column(sql.String(length=50))
    notes = sql.Column(sql.String(length=300))
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


class nascentflowMetadata(Base):
    __tablename__ = "nascentflowMetadata"
    nascentflow_version_id = sql.Column(
        sql.Integer, primary_key=True, index=True, unique=True
    )
    sample_id = sql.Column(sql.Integer, sql.ForeignKey("sampleID.sample_id"))
    nascentflow_version = sql.Column(sql.String(length=127))
    pipeline_revision_hash = sql.Column(sql.String(length=127))
    pipeline_hash = sql.Column(sql.String(length=127))
    nascentflow_date = sql.Column(sql.Date)
    nascentflow_redo_date = sql.Column(sql.Date)
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


class bidirflowMetadata(Base):
    __tablename__ = "bidirflowMetadata"
    bidirflow_version_id = sql.Column(
        sql.Integer, primary_key=True, index=True, unique=True
    )
    sample_id = sql.Column(sql.Integer, sql.ForeignKey("sampleID.sample_id"))
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
    tfit_date = sql.Column(sql.Date)
    fcgene_date = sql.Column(sql.Date)


# The following were created by Zach and we may or may not use...

# class tf(Base):
#    __tablename__ = "tf"
#    tf_id = sql.Column(sql.String(length=127), primary_key=True)
#    tf_alias = sql.Column(sql.String(length=127))


# class pipeline_status(Base):
#    __tablename__ = "pipeline_status"
#    srr_id = sql.Column(
#        sql.String(length=127),
#        sql.ForeignKey("srr_metadata.srr_id"),
#        primary_key=True,
#    )
#    fastqc_complete = sql.Column(sql.Boolean)
#    bbduk_complete = sql.Column(sql.Boolean)
#    hisat2_complete = sql.Column(sql.Boolean)
#    samtools_complete = sql.Column(sql.Boolean)
#    fastq_dump_complete = sql.Column(sql.Boolean)
#    pileup_complete = sql.Column(sql.String(length=127))
#    preseq_complete = sql.Column(sql.Boolean)
#    rseqc_complete = sql.Column(sql.String(length=127))
#    bedtools_complete = sql.Column(sql.Boolean)
#    igv_tools_complete = sql.Column(sql.Boolean)
#    fstitch_complete = sql.Column(sql.Boolean)
#    tfit_complete = sql.Column(sql.Boolean)


# class md_score(Base):
#    __tablename__ = "md_score"
#    srr_id = sql.Column(
#        sql.String(length=127),
#        sql.ForeignKey("srr_metadata.srr_id"),
#        primary_key=True,
#    )
#    tf_id = sql.Column(sql.String, sql.ForeignKey("tf.tf_id"))
#    erna_type = sql.Column(sql.String(length=127))
#    md_score_expected = sql.Column(sql.Integer)
#    md_score_std = sql.Column(sql.Integer)


# orm.py ends here