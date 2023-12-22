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
class organisms(Base):
    __tablename__ = "organisms"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    organism = sql.Column(sql.String(length=127))
    genome_build = sql.Column(sql.String(length=50))
    genome_bases = sql.Column(sql.BigInteger, nullable=True)

# Reference table for unique values in database
class searchEquiv(Base):
    __tablename__ = "searchEquiv"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    search_term = sql.Column(sql.String(length=250))
    db_term = sql.Column(sql.String(length=127))
    search_field = sql.Column(sql.String(length=50))

# Tissue and cancer designations for cell types
class tissues(Base):
    __tablename__ = "tissues"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    tissue = sql.Column(sql.String(length=127))
    cell_origin_type = sql.Column(sql.String(length=127), nullable=True)
    tissue_description = sql.Column(sql.String(length=127), nullable=True)
    disease = sql.Column(sql.Boolean)

# Paper-level metadata common to most samples in paper
class papers(Base):
    __tablename__ = "papers"
    id = sql.Column(sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    srp = sql.Column(sql.String(length=50))
    protocol = sql.Column(sql.String(length=50))
    organism_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("organisms.id"),
    )
    library = sql.Column(sql.String(length=50))
    spikein = sql.Column(sql.String(length=127), nullable=True)
    paper_name = sql.Column(sql.String(length=127))
    published = sql.Column(sql.Boolean)
    year = sql.Column(sql.Integer)
    first_author = sql.Column(sql.String(length=127))
    last_author = sql.Column(sql.String(length=127))
    doi = sql.Column(sql.String(length=300), nullable=True)
    curator1 = sql.Column(sql.String(length=50))
    curator2 = sql.Column(sql.String(length=50))
    other_sr_data = sql.Column(sql.Boolean)
    atac_seq = sql.Column(sql.Boolean)
    rna_seq = sql.Column(sql.Boolean)
    chip_seq = sql.Column(sql.Boolean)
    three_dim_seq = sql.Column(sql.Boolean)
    other_seq = sql.Column(sql.Boolean)
    paper_qc_score = sql.Column(sql.Float)
    paper_nro_score = sql.Column(sql.Float)

# All sample-specific information, including QC data and notes
class samples(Base):
    __tablename__ = "samples"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    sample_name = sql.Column(sql.String(length=50))
    replicate = sql.Column(sql.String(length=50))
    single_paired = sql.Column(sql.String(length=50))
    rcomp = sql.Column(sql.Boolean)
    unusable = sql.Column(sql.Boolean)
    timecourse = sql.Column(sql.Boolean)
    control_experimental = sql.Column(sql.String(length=50))
    outlier = sql.Column(sql.Boolean)
    notes = sql.Column(sql.String(length=300), nullable=True)
    processing_notes = sql.Column(sql.String(length=300), nullable=True)
    raw_read_depth = sql.Column(sql.Integer, nullable=True)
    trim_read_depth = sql.Column(sql.Integer, nullable=True)
    raw_read_length = sql.Column(sql.Integer, nullable=True)
    duplication_picard = sql.Column(sql.Float, nullable=True)
    single_map = sql.Column(sql.Integer, nullable=True)
    multi_map = sql.Column(sql.Integer, nullable=True)
    map_prop = sql.Column(sql.Float, nullable=True)
    rseqc_tags = sql.Column(sql.Integer, nullable=True)
    rseqc_cds = sql.Column(sql.Integer, nullable=True)
    rseqc_five_utr = sql.Column(sql.Integer, nullable=True)
    rseqc_three_utr = sql.Column(sql.Integer, nullable=True)
    rseqc_intron = sql.Column(sql.Integer, nullable=True)
    cds_rpk = sql.Column(sql.Float, nullable=True)
    intron_rpk = sql.Column(sql.Float, nullable=True)
    exint_ratio = sql.Column(sql.Float, nullable=True)
    distinct_tenmillion_prop = sql.Column(sql.Float, nullable=True)
    genome_prop_cov = sql.Column(sql.Float, nullable=True)
    avg_fold_cov = sql.Column(sql.Float, nullable=True)
    sample_qc_score = sql.Column(sql.Integer)
    sample_nro_score = sql.Column(sql.Integer)

# All SRR numbers, and equivalence to SRZ values and sample IDs
class sampleEquiv(Base):
    __tablename__ = "sampleEquiv"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("samples.id"),
    )
    srr = sql.Column(
        sql.String(length=50),
    )

# Genetic info, including organism, sample type, cell type, and
# any genetic modifications of note
class genetics(Base):
    __tablename__ = "genetics"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    organism_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("organisms.id"),
    )
    sample_type = sql.Column(sql.String(length=127))
    cell_type = sql.Column(sql.String(length=127))
    tissue_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("tissues.id"),
    )
    clone_individual = sql.Column(sql.String(length=127), nullable=True)
    strain = sql.Column(sql.String(length=127), nullable=True)
    genotype = sql.Column(sql.String(length=127), nullable=True)
    construct = sql.Column(sql.String(length=127), nullable=True)

# Summary stats for bidirectionals
class bidirs(Base):
    __tablename__ = "bidirs"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    num_tfit_bidir = sql.Column(sql.Integer, nullable=True)
    num_tfit_bidir_promoter = sql.Column(sql.Integer, nullable=True)
    num_tfit_bidir_exonic = sql.Column(sql.Integer, nullable=True)
    num_tfit_bidir_intronic = sql.Column(sql.Integer, nullable=True)
    num_tfit_bidir_intergenic = sql.Column(sql.Integer, nullable=True)
    num_dreg_bidir = sql.Column(sql.Integer, nullable=True)
    num_dreg_bidir_promoter = sql.Column(sql.Integer, nullable=True)
    num_dreg_bidir_exonic = sql.Column(sql.Integer, nullable=True)
    num_dreg_bidir_intronic = sql.Column(sql.Integer, nullable=True)
    num_dreg_bidir_intergenic = sql.Column(sql.Integer, nullable=True)
    tfit_bidir_gc = sql.Column(sql.Float, nullable=True)
    dreg_bidir_gc = sql.Column(sql.Float, nullable=True)
    tfit_master_merge_incl = sql.Column(sql.Boolean)
    dreg_master_merge_incl = sql.Column(sql.Boolean)

# Treatment information
class conditions(Base):
    __tablename__ = "conditions"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    condition_type = sql.Column(sql.String(length=127))
    treatment = sql.Column(sql.String(length=127))
    conc_intens = sql.Column(sql.String(length=50), nullable=True)
    start_time = sql.Column(sql.Integer, nullable=True)
    end_time = sql.Column(sql.Integer, nullable=True)
    time_unit = sql.Column(sql.String(length=50), nullable=True)
    duration = sql.Column(sql.Integer, nullable=True)
    duration_unit = sql.Column(sql.String(length=50), nullable=True)

# Linkage table of each sample to treatment(s)
class conditionLink(Base):
    __tablename__ = "conditionLink"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("samples.id"),
    )
    condition_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("conditions.id"),
    )



# Version information for bidirectionalflow runs
class bidirflowRuns(Base):
    __tablename__ = "bidirflowRuns"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    bidirflow_version = sql.Column(sql.String(length=127), nullable=True)
    pipeline_revision_hash = sql.Column(sql.String(length=127), nullable=True)
    pipeline_hash = sql.Column(sql.String(length=127), nullable=True)
    bidirflow_date = sql.Column(sql.Date, nullable=True)
    nextflow_version = sql.Column(sql.String(length=127), nullable=True)
    samtools_version = sql.Column(sql.String(length=127), nullable=True)
    bedtools_version = sql.Column(sql.String(length=127), nullable=True)
    mpich_version = sql.Column(sql.String(length=127), nullable=True)
    openmpi_version = sql.Column(sql.String(length=127), nullable=True)
    gcc_version = sql.Column(sql.String(length=127), nullable=True)
    r_version = sql.Column(sql.String(length=127), nullable=True)
    rsubread_version = sql.Column(sql.String(length=127), nullable=True)
    boost_version = sql.Column(sql.String(length=127), nullable=True)
    fstitch_version = sql.Column(sql.String(length=127), nullable=True)
    tfit_version = sql.Column(sql.String(length=127), nullable=True)
    dreg_version = sql.Column(sql.String(length=127), nullable=True)
    dreg_date = sql.Column(sql.Date, nullable=True)
    dreg_postprocess_date = sql.Column(sql.Date, nullable=True)
    tfit_date = sql.Column(sql.Date, nullable=True)
    tfit_prelim_date = sql.Column(sql.Date, nullable=True)
    fcgene_date = sql.Column(sql.Date, nullable=True)
    fcbidir_date = sql.Column(sql.Date, nullable=True)

# Version information for nascentflow runs
class nascentflowRuns(Base):
    __tablename__ = "nascentflowRuns"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    nascentflow_version = sql.Column(sql.String(length=127), nullable=True)
    downfile_version = sql.Column(sql.String(length=127), nullable=True)
    pipeline_revision_hash = sql.Column(sql.String(length=127), nullable=True)
    pipeline_hash = sql.Column(sql.String(length=127), nullable=True)
    nascentflow_date = sql.Column(sql.Date, nullable=True)
    nascentflow_redo_date = sql.Column(sql.Date, nullable=True)
    downfile_pipeline_date = sql.Column(sql.Date, nullable=True)
    nextflow_version = sql.Column(sql.String(length=127), nullable=True)
    fastqc_version = sql.Column(sql.String(length=127), nullable=True)
    bbmap_version = sql.Column(sql.String(length=127), nullable=True)
    hisat2_version = sql.Column(sql.String(length=127), nullable=True)
    samtools_version = sql.Column(sql.String(length=127), nullable=True)
    sratools_version = sql.Column(sql.String(length=127), nullable=True)
    preseq_version = sql.Column(sql.String(length=127), nullable=True)
    preseq_date = sql.Column(sql.Date, nullable=True)
    rseqc_version = sql.Column(sql.String(length=127), nullable=True)
    rseqc_date = sql.Column(sql.Date, nullable=True)
    java_version = sql.Column(sql.String(length=127), nullable=True)
    picard_gc_version = sql.Column(sql.String(length=127), nullable=True)
    picard_dups_version = sql.Column(sql.String(length=127), nullable=True)
    picard_date = sql.Column(sql.Date, nullable=True)
    bedtools_version = sql.Column(sql.String(length=127), nullable=True)
    igvtools_version = sql.Column(sql.String(length=127), nullable=True)
    seqkit_version = sql.Column(sql.String(length=127), nullable=True)
    mpich_version = sql.Column(sql.String(length=127), nullable=True)
    gcc_version = sql.Column(sql.String(length=127), nullable=True)
    python_version = sql.Column(sql.String(length=127), nullable=True)
    numpy_version = sql.Column(sql.String(length=127), nullable=True)

# Linkage table of each sample to bidirectionalflow run(s)
class bidirflowLink(Base):
    __tablename__ = "bidirflowLink"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("samples.id"),
    )
    bidirflow_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("bidirflowRuns.id"),
    )

# Linkage table of each sample to nascentflow run(s)
class nascentflowLink(Base):
    __tablename__ = "nascentflowLink"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("samples.id"),
    )
    nascentflow_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("nascentflowRuns.id"),
    )

# Main linkage table between sample, genetic, and expt IDs
class linkIDs(Base):
    __tablename__ = "linkIDs"
    id = sql.Column(
        sql.Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
    )
    sample_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("samples.id"),
    )
    genetic_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("genetics.id"),
    )
    paper_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("papers.id"),
    )
    bidir_id = sql.Column(
        sql.Integer,
        sql.ForeignKey("bidirs.id"),
    )

# dborm.py ends here
