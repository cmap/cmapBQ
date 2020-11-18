import re
import os
import sys
import gzip
import shutil
from datetime import datetime

from math import ceil
import multiprocessing as mp

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage

import cmapBQ.config as cfg
from .utils import long_to_gctx, parse_condition
from cmapPy.pandasGEXpress.concat import hstack

def list_tables():
    config = cfg.get_default_config()
    print(config.tables)
    return

def cmap_genetic_perts(client,
                       pert_id=None,
                       cmap_name=None,
                       gene_id=None,
                       gene_title=None,
                       ensemble_id=None,
                       table=None,
                       verbose=False):
    """
    Query genetic_pertinfo table

    :param client: Bigquery Client
    :param pert_id: List of pert_ids
    :param cmap_name: List of cmap_names
    :param gene_id: List of type INTEGER corresponding to gene_ids
    :param gene_title: List of gene_titles
    :param ensemble_id: List of ensumble_ids
    :param table: table to query. This by default points to the siginfo table and normally should not be changed.
    :param verbose: Print query and table address.
    :return:
    """
    if table is None:
        config = cfg.get_default_config()
        table = config.tables.genetic_pertinfo

    SELECT = "SELECT *"
    FROM = "FROM {}".format(table)

    CONDITIONS = []
    if pert_id:
        pert_id = parse_condition(pert_id)
        CONDITIONS.append("pert_id in UNNEST({})".format(list(pert_id)))
    if cmap_name:
        cmap_name = parse_condition(cmap_name)
        CONDITIONS.append("cmap_name in UNNEST({})".format(list(cmap_name)))
    if gene_id:
        gene_id = parse_condition(gene_id)
        CONDITIONS.append("gene_id in UNNEST({})".format(list(gene_id)))
    if gene_title:
        gene_title = parse_condition(gene_title)
        CONDITIONS.append("gene_title in UNNEST({})".format(list(gene_title)))
    if ensemble_id:
        ensemble_id = parse_condition(ensemble_id)
        CONDITIONS.append("ensemble_id in UNNEST({})".format(list(ensemble_id)))

    if CONDITIONS:
        WHERE = "WHERE " + " AND ".join(CONDITIONS)
    else:
        WHERE = ""

    query = " ".join([SELECT, FROM, WHERE])

    if verbose:
        print("Table: \n {}".format(table))
        print("Query:\n {}".format(query))

    return run_query(query, client).result().to_dataframe()


def cmap_cell(client,
              cell_iname=None,
              cell_alias = None,
              ccle_name=None,
              primary_disease=None,
              cell_lineage=None,
              cell_type=None,
              table=None,
              verbose=False):
    """
    Query cellinfo table

    :param client: Bigquery Client
    :param cell_iname: List of cell_inames
    :param cell_alias: List of cell aliases
    :param ccle_name: List of ccle_names
    :param primary_disease: List of primary_diseases
    :param cell_lineage: List of cell_lineages
    :param cell_type: List of cell_types
    :param table: table to query. This by default points to the siginfo table and normally should not be changed.
    :param verbose: Print query and table address.
    :return: Pandas DataFrame
    """
    if table is None:
       config = cfg.get_default_config()
       table = config.tables.cellinfo

    SELECT = "SELECT *"
    FROM = "FROM {}".format(table)

    CONDITIONS = []
    if cell_iname:
        cell_iname = parse_condition(cell_iname)
        CONDITIONS.append("cell_iname in UNNEST({})".format(list(cell_iname)))
    if cell_alias:
        cell_alias = parse_condition(cell_alias)
        CONDITIONS.append("cell_alias in UNNEST({})".format(list(cell_alias)))
    if ccle_name:
        ccle_name = parse_condition(ccle_name)
        CONDITIONS.append("ccle_name in UNNEST({})".format(list(ccle_name)))
    if primary_disease:
        primary_disease = parse_condition(primary_disease)
        CONDITIONS.append("primary_disease in UNNEST({})".format(list(primary_disease)))
    if cell_lineage:
        cell_lineage = parse_condition(cell_lineage)
        CONDITIONS.append("cell_lineage in UNNEST({})".format(list(cell_lineage)))
    if cell_type:
        cell_type = parse_condition(cell_type)
        CONDITIONS.append("cell_type in UNNEST({})".format(list(cell_type)))

    if CONDITIONS:
       WHERE = "WHERE " + " AND ".join(CONDITIONS)
    else:
       WHERE = ""

    query = " ".join([SELECT, FROM, WHERE])

    if verbose:
       print("Table: \n {}".format(table))
       print("Query:\n {}".format(query))

    return run_query(query, client).result().to_dataframe()

def cmap_genes(client,
               gene_id=None,
               gene_symbol=None,
               ensembl_id=None,
               gene_title = None,
               gene_type=None,
               src=None, table=None,
               verbose=False):
    """
    Query geneinfo table. geneinfo contains information about

    :param client: Bigquery Client
    :param gene_id: list of gene_ids
    :param gene_symbol: list of gene_symbols
    :param ensembl_id:  list of ensembl_ids
    :param gene_title: list of gene_titles
    :param gene_type: list of gene_types
    :param src: list of gene sources
    :param table: table to query. This by default points to the siginfo table and normally should not be changed.
    :param verbose: Print query and table address.
    :return: Pandas DataFrame
    """

    if table is None:
        config = cfg.get_default_config()
        table = config.tables.geneinfo

    SELECT = "SELECT *"
    FROM = "FROM {}".format(table)

    CONDITIONS = []
    if gene_id:
        gene_id = parse_condition(gene_id)
        CONDITIONS.append("gene_id in UNNEST({})".format(list(gene_id)))
    if gene_symbol:
        gene_symbol = parse_condition(gene_symbol)
        CONDITIONS.append("gene_symbol in UNNEST({})".format(list(gene_symbol)))
    if ensembl_id:
        ensembl_id = parse_condition(ensembl_id)
        CONDITIONS.append("ensembl_id in UNNEST({})".format(list(ensembl_id)))
    if gene_title:
        gene_title = parse_condition(gene_title)
        CONDITIONS.append("gene_title in UNNEST({})".format(list(gene_title)))
    if gene_type:
        gene_type = parse_condition(gene_type)
        CONDITIONS.append("gene_type in UNNEST({})".format(list(gene_type)))

    if CONDITIONS:
        WHERE = "WHERE " + " AND ".join(CONDITIONS)
    else:
        WHERE = ""

    query = " ".join([SELECT, FROM, WHERE])

    if verbose:
        print("Table: \n {}".format(table))
        print("Query:\n {}".format(query))

    return run_query(query, client).result().to_dataframe()

def cmap_sig(
    client,
    sig_id=None,
    pert_id=None,
    cmap_name=None,
    cell_iname=None,
    build_name=None,
    return_fields='priority',
    limit=None,
    table=None,
    verbose=False,
):
    """
    Query level 5 metadata table

    :param client: Bigquery Client
    :param sig_id: list of sig_ids
    :param pert_id: list of pert_ids
    :param cmap_name: list of cmap_name, formerly pert_iname
    :param cell_iname: list of cell names
    :param build_name: list of builds
    :param return_fields: ['priority', 'all']
    :param limit: Maximum number of rows to return
    :param table: table to query. This by default points to the level 5 siginfo table and normally should not be changed.
    :param verbose: Print query and table address.
    :return: Pandas Dataframe
    """

    priority_fields = ['sig_id', 'pert_id',
              'cmap_name', 'pert_type', 'cell_iname',
              'pert_idose', 'nsample', 'build_name', 'project_code',
              'ss_ngene', 'cc_q75',
              'tas']

    if return_fields == 'priority':
        SELECT = "SELECT " + ",".join(priority_fields)
    elif return_fields == 'all':
        SELECT = "SELECT *"
    else:
        print("return_fields only takes ['priority', 'all']")
        sys.exit(1)

    if table is None:
        config = cfg.get_default_config()
        table = config.tables.siginfo

    FROM = "FROM {}".format(table)

    CONDITIONS = []
    if pert_id:
        pert_id = parse_condition(pert_id)
        CONDITIONS.append("pert_id in UNNEST({})".format(list(pert_id)))
    if sig_id:
        sig_id = parse_condition(sig_id)
        CONDITIONS.append("sig_id in UNNEST({})".format(list(sig_id)))
    if cell_iname:
        cell_iname = parse_condition(cell_iname)
        CONDITIONS.append("cell_iname in UNNEST({})".format(list(cell_iname)))
    if cmap_name:
        cmap_name = parse_condition(cmap_name)
        CONDITIONS.append("cmap_name in UNNEST({})".format(list(cmap_name)))
    if build_name:
        build_name = parse_condition(build_name)
        CONDITIONS.append("build_name in UNNEST({})".format(list(build_name)))

    if CONDITIONS:
        WHERE = "WHERE " + " AND ".join(CONDITIONS)
    else:
        WHERE = ""

    if limit:
        assert isinstance(limit, int), "Limit argument must be an integer"
        WHERE = WHERE + " LIMIT {}".format(limit)
    query = " ".join([SELECT, FROM, WHERE])

    if verbose:
        print("Table: \n {}".format(table))
        print("Query:\n {}".format(query))

    return run_query(query, client).result().to_dataframe()


def cmap_profiles(
    client,
    sample_id=None,
    pert_id=None,
    cmap_name=None,
    cell_iname=None,
    build_name=None,
    return_fields='priority',
    limit=None,
    table=None,
    verbose=False,
):
    """
    Query per sample metadata, corresponds to level 3 and level 4 data, AND operator used for multiple
    conditions.

    :param client: Bigquery client
    :param sample_id: list of sample_ids
    :param pert_id: list of pert_ids
    :param cmap_name: list of cmap_name
    :param build_name: list of builds
    :param return_fields: ['priority', 'all']
    :param limit: Maximum number of rows to return
    :param table: table to query. This by default points to the siginfo table and normally should not be changed.
    :param verbose: Print query and table address.
    :return: Pandas Dataframe
    """
    if table is None:
        config = cfg.get_default_config()
        table = config.tables.instinfo

    priority_fields = ['sample_id', 'det_plate', 'pert_id',
                  'cmap_name', 'pert_type', 'cell_iname',
                  'pert_idose', 'build_name', 'project_code']

    if return_fields == 'priority':
        SELECT = "SELECT " + ",".join(priority_fields)
    elif return_fields == 'all':
        SELECT = "SELECT *"
    else:
        print("return_fields only takes ['priority', 'all']")
        sys.exit(1)


    FROM = "FROM {}".format(table)

    CONDITIONS = []
    if pert_id:
        pert_id = parse_condition(pert_id)
        CONDITIONS.append("pert_id in UNNEST({})".format(list(pert_id)))
    if sample_id:
        sample_id = parse_condition(sample_id)
        CONDITIONS.append("sample_id in UNNEST({})".format(list(sample_id)))
    if cell_iname:
        cell_iname = parse_condition(cell_iname)
        CONDITIONS.append("cell_iname in UNNEST({})".format(list(cell_iname)))
    if cmap_name:
        cmap_name = parse_condition(cmap_name)
        CONDITIONS.append("cmap_name in UNNEST({})".format(list(cmap_name)))
    if build_name:
        build_name = parse_condition(build_name)
        CONDITIONS.append("build_name in UNNEST({})".format(list(build_name)))

    if CONDITIONS:
        WHERE = "WHERE " + " AND ".join(CONDITIONS)
    else:
        WHERE = ""

    if limit:
        assert isinstance(limit, int), "Limit argument must be an integer"
        WHERE = WHERE + " LIMIT {}".format(limit)
    query = " ".join([SELECT, FROM, WHERE])

    assert (
        len(query) < 1024 * 10 ** 3
    ), "Query length exceeds maximum allowed by BQ, keep under 1M characters"

    if verbose:
        print("Table: \n {}".format(table))
        print("Query:\n {}".format(query))

    return run_query(query, client).result().to_dataframe()


def cmap_compounds(
    client,
    pert_id=None,
    cmap_name=None,
    moa=None,
    target=None,
    compound_aliases=None,
    limit=None,
    verbose=False,
):
    """
    Query compoundinfo table for various field by providing lists of compounds, moa, targets, etc.
    'AND' operator used for multiple conditions.

    :param client: BigQuery Client
    :param pert_id: List of pert_ids
    :param cmap_name: List of cmap_names
    :param target: List of targets
    :param moa: List of MoAs
    :param compound_aliases: List of compound aliases
    :param limit: Maximum number of rows to return
    :param verbose: Print query and table address.
    :return: Pandas Dataframe matching queries
    """
    config = cfg.get_default_config()
    compoundinfo_table = config.tables.compoundinfo

    SELECT = "SELECT *"
    FROM = "FROM {}".format(compoundinfo_table)

    CONDITIONS = []
    if pert_id:
        pert_id = parse_condition(pert_id)
        CONDITIONS.append("pert_id in UNNEST({})".format(list(pert_id)))
    if cmap_name:
        cmap_name = parse_condition(cmap_name)
        CONDITIONS.append("cmap_name in UNNEST({})".format(list(cmap_name)))
    if target:
        target = parse_condition(target)
        CONDITIONS.append("target in UNNEST({})".format(list(target)))
    if moa:
        moa = parse_condition(moa)
        CONDITIONS.append("moa in UNNEST({})".format(list(moa)))
    if compound_aliases:
        compound_aliases = parse_condition(compound_aliases)
        CONDITIONS.append(
            "compound_aliases in UNNEST({})".format(list(compound_aliases))
        )

    if CONDITIONS:
        WHERE = "WHERE " + " AND ".join(CONDITIONS)
    else:
        WHERE = ""

    if limit:
        assert isinstance(limit, int), "Limit argument must be an integer"
        WHERE = WHERE + " LIMIT {}".format(limit)

    query = " ".join([SELECT, FROM, WHERE])

    if verbose:
        print("Table: \n {}".format(compoundinfo_table))
        print("Query:\n {}".format(query))

    return run_query(query, client).result().to_dataframe()


def cmap_matrix(
    client,
    data_level="level5",
    rid=None,
    cid=None,
    verbose=False,
    chunk_size=1000,
    table=None,
    limit=1000,
):
    """
    Query for numerical data for signature-gene level data.

    :param client: Bigquery Client
    :param data_level: Data level requested. IDs from siginfo file correspond to 'level5'. Ids from instinfo are available
     in 'level3' and 'level4'. Choices are ['level5', 'level4', 'level3']
    :param rid: Row ids
    :param cid: Column ids
    :param verbose: Run in verbose mode
    :param chunk_size: Runs queries in stages to avoid query character limit. Default 1,000
    :param table: Table address to query. Overrides 'data_level' parameter. Generally should not be used.
    :param verbose: Print query and table address.
    :return: GCToo object
    """

    config = cfg.get_default_config()

    if table is not None:
        table_id = table
    else:
        if data_level == "level3":
            table_id = config.tables.level3
        elif data_level == "level4":
            table_id = config.tables.level4
        elif data_level == "level5":
            table_id = config.tables.level5
        else:
            print(
                "Unsupported data_level. select from ['level3', 'level4', level5'].\n Default is 'level5'. "
            )
            sys.exit(1)

    if cid:
        cid = parse_condition(cid)
        assert len(cid) <= limit, "List of cids can not exceed limit of {}".format(
            limit
        )
        cur = 0
        nparts = ceil(len(cid) / chunk_size)
        result_dfs = []
        while cur < nparts:
            start = cur * chunk_size
            end = (
                cur * chunk_size + chunk_size
            )  # No need to check for end, index only returns present values
            cur = cur + 1
            print("Running query ... ({}/{})".format(cur, nparts))
            result_dfs.append(
                _build_and_launch_query(
                    client, table_id, cid=cid[start:end], rid=rid, verbose=verbose
                )
            )

        try:
            pool = mp.Pool(mp.cpu_count())
            print("Pivoting Dataframes to GCT objects")
            result_gctoos = pool.map(_pivot_result, result_dfs)
            pool.close()
        except:
            if nparts > 1:
                print("Multiprocessing unavailable, pivoting chunks in series...")
            cur = 0
            result_gctoos = []
            for df in result_dfs:
                cur = cur + 1
                print("Pivoting... ({}/{})".format(cur, nparts))
                result_gctoos.append(_pivot_result(df))
        print("Complete")
        return hstack(result_gctoos)
    else:
        print("Provide column ids to extract using the cid= keyword argument")
        sys.exit(1)


def get_table_info(client, table_id):
    """
    Query a table address within client's permissions for schema.

    :param client: Bigquery Client
    :param table_id: table address as {dataset}.{table_id}
    :return: Pandas Dataframe of column names. Note: Not all column names are query-able but all will be returned from a given metadata table
    """
    tok = table_id.split(".")

    if len(tok) > 1:
        dataset_name = ".".join(tok[0:-1])
        table_name = tok[-1]
    else:
        return NotImplementedError("table_id should be in {dataset}.{table_id} format")

    QUERY = "SELECT column_name, data_type FROM `{}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name='{}'".format(
        dataset_name, table_name
    )
    table_desc = run_query(QUERY, client).result().to_dataframe()
    return table_desc


def _build_query(table_id, rid=None, cid=None):
    """
    Crafts and retrieves query from rid and cid conditions. Uses pandas GBQ read_gbq
    to download records from BigQuery as a dataframe object.

    :param table_id: Matrix table
    :param rid: list of row ids (gene space)
    :param cid: list of column ids (samples/sig_ids)
    :return: Long-form DataFrame object
    """
    SELECT = "SELECT cid, rid, value"
    FROM = "FROM `{}`".format(table_id)

    CONDITIONS = []
    if rid:
        rids = parse_condition(rid)
        CONDITIONS.append("rid in UNNEST({})".format(list(rids)))
    if cid:
        cids = parse_condition(cid)
        CONDITIONS.append("cid in UNNEST({})".format(cids))

    if CONDITIONS:
        WHERE = "WHERE " + " AND ".join(CONDITIONS)
    else:
        WHERE = ""

    QUERY = " ".join([SELECT, FROM, WHERE])

    return QUERY


def _build_and_launch_query(client, table_id, rid=None, cid=None, verbose=False):
    """
    Crafts and retrieves query from rid and cid conditions. Uses pandas GBQ read_gbq
    to download records from BigQuery as a dataframe object.

    :param table_id: Matrix table
    :param rid: list of row ids (gene space)
    :param cid: list of column ids (samples/sig_ids)
    :param verbose: Shows extra information for debugging
    :return: Long-form DataFrame object
    """
    SELECT = "SELECT cid, rid, value"
    FROM = "FROM `{}`".format(table_id)

    CONDITIONS = []
    if rid:
        rids = parse_condition(rid)
        CONDITIONS.append("rid in UNNEST({})".format(list(rids)))
    if cid:
        cids = parse_condition(cid)
        CONDITIONS.append("cid in UNNEST({})".format(cids))

    if CONDITIONS:
        WHERE = "WHERE " + " AND ".join(CONDITIONS)
    else:
        WHERE = ""

    QUERY = " ".join([SELECT, FROM, WHERE])

    if verbose:
        print(QUERY)

    try:
        return pd.read_gbq(QUERY, dialect="standard")
    except:
        print(
            "Pandas read_gbq may not be installed... Running query but no progress indicator available."
        )
        return run_query(QUERY, client).result().to_dataframe()


def _pivot_result(df_long):
    """
    Converts long-form DataFrame to GCToo object

    :param df_long: long-form DataFrame
    :return: GCToo Object
    """
    gctoo = long_to_gctx(df_long)
    return gctoo


def list_cmap_moas(client):
    """
    List available MoAs

    :param client: BigQuery Client
    :return: Single column Dataframe of MoAs
    """
    config = cfg.get_default_config()
    compoundinfo_table = config.tables.compoundinfo

    QUERY = "SELECT DISTINCT moa from {}".format(compoundinfo_table)
    return run_query(QUERY, client).result().to_dataframe()


def list_cmap_targets(client):
    """
    List available targets

    :param client: BigQuery Client
    :return: Pandas DataFrame
    """
    config = cfg.get_default_config()
    compoundinfo_table = config.tables.compoundinfo

    QUERY = "SELECT DISTINCT target from {}".format(compoundinfo_table)
    return run_query(QUERY, client).result().to_dataframe()


def list_cmap_compounds(client):
    """
    List available compounds

    :param client: BigQuery Client
    :return: Single column Dataframe of compounds
    """
    config = cfg.get_default_config()
    compoundinfo_table = config.tables.compoundinfo
    QUERY = "SELECT DISTINCT cmap_name from {}".format(compoundinfo_table)
    return run_query(QUERY, client).result().to_dataframe()


def extract_matrix_GCS(query, destination_table=None, storage_uri=None, out_path=None):
    """

    Run a BigQuery query using Google Cloud Storage to export table as CSV. Downloads exported CSVs to out_path/csv/.
    This function was meant as a step in a deprecated matrix download process.

    :param query: Query String
    :param destination_table: Store as BQ table
    :param storage_uri: GCS Location
    :param out_path: outpath on file system
    :return: list of csvs
    """
    bigquery_client = cfg.get_bq_client()

    # run query
    query_job = run_query(query, bigquery_client, destination_table)
    # extract table to GCS
    extract_job = export_table(query_job, bigquery_client, storage_uri=storage_uri)
    # download from GCS
    csv_path = os.path.join(out_path, "csv")
    cnt = 0
    while os.path.exists(csv_path):
        csv_path = os.path.join(out_path, "csv{}".format(cnt))
        cnt += 1
    os.mkdir(csv_path)

    file_list = download_from_extract_job(extract_job, csv_path)
    file_list = gunzip_csv(file_list, csv_path)

    return file_list


def _run_query_create_log(query, client, destination_table=None):
    """
    Runs BigQuery queryjob

    :param query: Query to run as a string
    :param client: BigQuery client object
    :return: QueryJob object
    """

    # Job config
    job_config = bigquery.QueryJobConfig()
    if destination_table is not None:
        job_config.destination = destination_table
    else:
        timestamp_name = datetime.now().strftime("query_%Y%m%d%H%M%S")
        project = "cmap-big-table"
        dataset = "cmap_query"
        dest_tbl = ".".join([project, dataset, timestamp_name])
        job_config.destination = dest_tbl

    job_config.create_disposition = "CREATE_IF_NEEDED"
    return client.query(query, job_config=job_config)

def run_query(query, client):
    """
    Runs BigQuery queryjob

    :param query: Query to run as a string
    :param client: BigQuery client object
    :return: QueryJob object
    """
    return client.query(query)

def export_table(query_job, client, storage_uri=None):
    """
    Extract result of a QueryJob object to location in GCS.

    :param query_job: QueryJob object from which to extract results
    :param client: BigQuery Client Object
    :param storage_uri: location in GCS to extract table
    :return: ExtractJob object
    """
    result_bucket = "clue_queries"
    res = query_job.result()
    # print(res)
    if storage_uri is not None:
        storage_uri = storage_uri
    else:
        timestamp_name = datetime.now().strftime("query_%Y%m%d%H%M%S")
        filename = "result-*.csv"
        storage_uri = "gs://{}/{}/{}".format(result_bucket, timestamp_name, filename)
        storage_uri = storage_uri

    exjob_config = bigquery.job.ExtractJobConfig(compression="GZIP")

    table_ref = query_job.destination
    extract_job = client.extract_table(table_ref, storage_uri, job_config=exjob_config)

    extract_job.result()
    return extract_job


def download_from_extract_job(extract_job, destination_path):
    """
    Downloads a blob from the ExtractJob

    :param extract_job: Extract Job object
    :param destination_path: Output path
    :return: List of files
    """
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket("clue_queries")
    location = extract_job.destination_uris[0]
    blob_prefix = re.findall("query_[0-9]+/", location)
    blobs = [_ for _ in bucket.list_blobs(prefix=blob_prefix)]

    filelist = []
    for blob in blobs:
        fn = os.path.basename(blob.name) + ".gz"
        blob.download_to_filename(os.path.join(destination_path, fn))
        filelist.append(os.path.join(destination_path, fn))

    return filelist

def gunzip_csv(filepaths, destination_path):
    """
    Unzip .gz files

    :param filepaths: Path of files with '.gz' extensions
    :param destination_path: folder to place unzipped files
    :return: List of outfile paths
    """
    out_paths = []
    for filename in filepaths:
        assert filename.endswith(".gz"), "Can't unzip extension"
        with gzip.open(filename, "rb") as f_in:
            if destination_path is not None:
                no_ext = os.path.splitext(filename)[0]
                outname = os.path.basename(no_ext)  # Just file name w/o ext
                outname = os.path.join(destination_path, outname)  # path
            else:
                outname = os.path.splitext(filename)[0]  # remove .gz extension

            out_paths.append(outname)
            with open(outname, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        if os.path.exists(outname):  # If unzipped version exists, delete .gz file
            os.remove(filename)

    return out_paths
