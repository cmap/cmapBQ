import re
import os, argparse, sys, traceback
from datetime import datetime
import gzip, shutil
from math import ceil

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.auth import exceptions

from .utils import write_args, write_status, mk_out_dir, long_to_gctx, parse_condition
from cmapPy.set_io.grp import read as parse_grp
from cmapPy.pandasGEXpress.concat import hstack

def cmap_compounds(client, pert_id=None, cmap_name=None, moa=None, target=None,
                   compound_aliases=None, limit=None):
    """
    Query compound info table for various field by providing lists of compounds, moa, targets, etc.
    'OR' operator used for multiple conditions to be maximally inclusive.

    :param client: BigQuery Client
    :param pert_id: List of pert_ids
    :param cmap_name: List of cmap_names
    :param target: List of targets
    :param moa: List of MoAs
    :param compound_aliases: List of compound aliases
    :return: Pandas Dataframe matching queries
    """
    SELECT = "SELECT *"
    FROM = "FROM broad_cmap_lincs_data.compoundinfo"
    WHERE = ""

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
        CONDITIONS.append("compound_aliases in UNNEST({})".format(list(compound_aliases)))

    if CONDITIONS:
        WHERE = "WHERE" +  " OR ".join(CONDITIONS)
    else:
        WHERE = ""

    if limit:
        assert isinstance(limit, int), "Limit argument must be an integer"
        WHERE = WHERE + " LIMIT {}".format(limit)
    query = " ".join([SELECT, FROM, WHERE])

    return run_query(query, client).result().to_dataframe()

def cmap_sig(client, args):
    ...
    pass

def cmap_profiles(client, args):
    ...
    pass

def cmap_cell(client, args):
    ...
    pass


def cmap_matrix(client, table, rid=None, cid=None, project=None, dataset=None, verbose=False, chunk_size=10000):
    """

    :param client: Bigquery Client
    :param table: Table containing numerical data
    :param rid: Row ids
    :param cid: Column ids
    :param project:
    :param dataset:
    :param verbose: Run in verbose mode
    :param chunk_size: Runs queries in stages to avoid query character limit. Default 10,000
    :return: GCToo object
    """
    if (project is not None) and (dataset is not None):
        table_id = '.'.join([project, dataset, table])
    else:
        table_id = table

    SELECT = "SELECT cid, rid, value"
    #make table address
    FROM = "FROM `{}`".format(table_id)
    WHERE = ""

    if cid:
        cids = parse_condition(cid)
        cur = 0
        gctoos = []
        nparts = ceil(len(cids)/chunk_size)
        qjobs = []
        while cur < nparts:
            start = cur*chunk_size
            end = cur*chunk_size + chunk_size #No need to check for end, index only returns present values
            cur = cur + 1
            WHERE = ""

            CONDITIONS = []
            if rid:
                rids = parse_condition(rid)
                CONDITIONS.append("rid in UNNEST({})".format(list(rids)))
            if cid:
                cids = parse_condition(cid)
                CONDITIONS.append("cid in UNNEST({})".format(cids[start:end]))

            if CONDITIONS:
                WHERE = "WHERE " +  " AND ".join(CONDITIONS)
            else:
                WHERE = ""

            QUERY = " ".join([SELECT, FROM, WHERE])

            if verbose:
                print(QUERY)

            qjobs.append(run_query(QUERY, client))
            print("Running query... ({}/{})".format(cur, nparts))

        cur = 0
        for qjob in qjobs:
            cur = cur + 1
            print("Waiting for result...")
            df_long = qjob.result().to_dataframe()
            print("Converting to GCToo object... ({}/{})".format(cur, nparts))
            gctoos.append(long_to_gctx(df_long))

        return hstack(gctoos)
    else:
        CONDITIONS = []
        if rid:
            rids = parse_condition(rid)
            CONDITIONS.append("rid in UNNEST({})".format(list(rids)))
        if cid:
            cids = parse_condition(cid)
            CONDITIONS.append("cid in UNNEST({})".format(list(cids)))

        if CONDITIONS:
            WHERE = "WHERE " +  " AND ".join(CONDITIONS)
        else:
            WHERE = ""

        QUERY = " ".join([SELECT, FROM, WHERE])

        if verbose:
            print(QUERY)

        qjob = run_query(QUERY, client)

        print("Running query...")
        df_long = qjob.result().to_dataframe()

        print("Done.")
        print("Converting to GCToo object...")
        gctoo = long_to_gctx(df_long)
        return gctoo

def list_cmap_moas(client):
    """
    List available MoAs
    :param client: BigQuery Client
    :return: Single column Dataframe of MoAs
    """
    QUERY = "SELECT DISTINCT moa from cmap-big-table.broad_cmap_lincs_data.compoundinfo"
    return run_query(QUERY, client).result().to_dataframe()

def list_cmap_targets(client):
    """
    List available targets
    :param client: BigQuery Client
    :return:
    """
    QUERY = "SELECT DISTINCT target from cmap-big-table.broad_cmap_lincs_data.compoundinfo"
    return run_query(QUERY, client).result().to_dataframe()

def list_cmap_compounds(client):
    """
    List available compounds
    :param client: BigQuery Client
    :return: Single column Dataframe of compounds
    """
    QUERY = "SELECT DISTINCT cmap_name from cmap-big-table.broad_cmap_lincs_data.compoundinfo"
    return run_query(QUERY, client).result().to_dataframe()

def run_query(query, client, destination_table=None):
    """
    Runs BigQuery queryjob
    :param query: Query to run as a string
    :param client: BigQuery client object
    :param args: additional args
    :return: QueryJob object
    """

    #Job config
    job_config = bigquery.QueryJobConfig()
    if destination_table is not None:
        job_config.destination = destination_table
    else:
        timestamp_name = datetime.now().strftime('query_%Y%m%d%H%M%S')
        project = "cmap-big-table"
        dataset = "cmap_query"
        dest_tbl = ".".join([project, dataset, timestamp_name])
        job_config.destination = dest_tbl

    job_config.create_disposition = 'CREATE_IF_NEEDED'
    return client.query(query, job_config=job_config)

def export_table(query_job, client, args):
    """

    :param query_job: QueryJob object from which to extract results
    :param client: BigQuery Client Object
    :param args: Additional Args. Noteworthy is storage_uri which is the location in GCS to extract table
    :return: ExtractJob object
    """
    result_bucket = 'clue_queries'
    res = query_job.result()
    #print(res)
    if args.storage_uri is not None:
        storage_uri = args.storage_uri
    else:
        timestamp_name = datetime.now().strftime('query_%Y%m%d%H%M%S')
        filename = ('result-*.csv')
        storage_uri = "gs://{}/{}/{}".format(result_bucket, timestamp_name, filename)
        args.storage_uri = storage_uri

    exjob_config = bigquery.job.ExtractJobConfig(compression='GZIP')

    table_ref = query_job.destination
    extract_job = client.extract_table(
        table_ref,
        storage_uri,
        job_config=exjob_config)

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

    bucket = storage_client.bucket('clue_queries')
    location = extract_job.destination_uris[0]
    blob_prefix = re.findall("query_[0-9]+/", location)
    blobs = [_ for _ in bucket.list_blobs(prefix=blob_prefix)]

    filelist = []
    for blob in blobs:
        fn = os.path.basename(blob.name) + '.gz'
        blob.download_to_filename(os.path.join(destination_path, fn))
        filelist.append(os.path.join(destination_path, fn))

    return filelist

def gunzip_csv(filepaths, destination_path):
    """

    :param filepaths: Path of files with '.gz' extensions
    :param destination_path: folder to place unzipped files
    :return: List of outfile paths
    """
    out_paths = []
    for filename in filepaths:
        assert filename.endswith('.gz'), "Can't unzip extension"
        with gzip.open(filename, 'rb') as f_in:
            if destination_path is not None:
                no_ext = os.path.splitext(filename)[0]
                outname = os.path.basename(no_ext) #Just file name w/o ext
                outname = os.path.join(destination_path, outname) #path
            else:
                outname = os.path.splitext(filename)[0] #remove .gz extension

            out_paths.append(outname)
            with open(outname, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    return out_paths
