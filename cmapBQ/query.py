import re
import os, argparse, sys, traceback
from datetime import datetime
import gzip, shutil

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.auth import exceptions
from .utils import write_args, write_status, mk_out_dir

def parse_condition(arg):
    """
    Parse argument for pathname, string or list. If file path exists reads GRP or TXT file.
    Returns list

    :param arg: Takes in pathname, string, or list.
    :return: list
    """
    if isinstance(arg, str):
        if os.path.isfile(arg):
            arg = parse_grp(arg)
        else:
            arg = [arg]
    return list(arg)

def cmap_compounds(client, pert_id=None, cmap_name=None, moa=None, target=None,
                   compound_aliases=None):
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
    SELECT = 'SELECT *'
    FROM = 'FROM broad_cmap_lincs_data.compoundinfo'
    WHERE = 'WHERE '

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

    WHERE = WHERE +  " OR ".join(CONDITIONS)
    query = " ".join([SELECT, FROM, WHERE])

    return run_query(query, client).result().to_dataframe()

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
