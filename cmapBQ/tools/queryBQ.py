import re
import os, argparse, sys, traceback
from datetime import datetime
import gzip, shutil

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.auth import exceptions
from ..utils import write_args, write_status, mk_out_dir

from cmapPy.pandasGEXpress.GCToo import GCToo
from cmapPy.pandasGEXpress.write_gctx import write as write_gctx
from cmapPy.pandasGEXpress.write_gct import write as write_gct

from ..utils.file import  csv_to_gctx

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Runs Query in BQ, downloads the results and convert results from CSV to GCT")
    parser.add_argument('-q', '--query', help="Source Table, rows that exist in both will this table's value")
    parser.add_argument('-d', '--destination_table', help="Destination Table in Bigquery to save results, if empty a temporary table will be used.",
                        default=None)
    parser.add_argument('-s', '--storage_uri', help="GCS location for output. Example gs://clue/folder/output.csv. "
                        "Default is to write to timestamped directory", default=None)
    parser.add_argument('-k', '--key', help="Path to service account key. \n Alternatively, set GOOGLE_APPLICATION_CREDENTIALS", default=None)
    parser.add_argument('-o', '--out', help="Output folder", default=os.getcwd())
    parser.add_argument('-c', '--create_subdir', help="Create Subdirectory", type=str2bool, default=True)
    parser.add_argument('-g', '--use_gctx', help="Use GCTX format, default is true", default=True)

    if argv is None:
        parser.print_help()
        sys.exit(1)
    else:
        args = parser.parse_args(argv)
        return args

def run_query(query, client, args):
    """
    Runs BigQuery queryjob
    :param query: Query to run as a string
    :param client: BigQuery client object
    :param args: additional args
    :return: QueryJob object
    """

    #Job config
    job_config = bigquery.QueryJobConfig()
    if args.destination_table is not None:
        job_config.destination = args.destination_table
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
    out_paths = []
    for filename in filepaths:
        assert filename.endswith('.gz'), "Can't unzip extension"
        with gzip.open(filename, 'rb') as f_in:
            outname = os.path.splitext(filename)[0] #remove .gz extension
            out_paths.append(outname)
            with open(outname, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    return out_paths

def main(argv=None):
    args = parse_args(argv)
    out_path = mk_out_dir(args.out, "queryBQ", create_subdir=args.create_subdir)
    write_args(args, out_path)

    if args.key is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key

    bigquery_client = bigquery.Client()
    storage_client = storage.Client()

    try:
        #run query
        query_job = run_query(args.query, bigquery_client, args)
        #extract table to GCS
        extract_job = export_table(query_job, bigquery_client, args)
        #download from GCS
        csv_path = os.path.join(out_path, 'csv')
        os.mkdir(csv_path)
        file_list = download_from_extract_job(extract_job, csv_path)
        file_list = gunzip_csv(file_list, csv_path)
        csv_to_gctx(file_list, out_path, args)

        print ("result_destination: {}".format(extract_job.destination_uris[0]))

        write_status(True, out_path)
    except exceptions.DefaultCredentialsError as cred_error:
        print('Could not automatically determine credentials. Please set GOOGLE_APPLICATION_CREDENTIALS or'
              ' specify path to key using --key')
        write_status(False, out_path, exception=cred_error)
        exit(1)
    except Exception as e:
        write_status(False, out_path, exception=e)
        exit(1)


if __name__ == '__main__':
    main(sys.argv[1:])
