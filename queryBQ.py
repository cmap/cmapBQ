import glob, re, pytz, time
import os, argparse, sys, traceback
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from google.cloud import bigquery
from google.cloud import storage
from google.api_core import exceptions as api_exception
from google.auth import exceptions
from cmapPy.set_io import grp


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--query', help="Source Table, rows that exist in both will this table's value")
    parser.add_argument('-d', '--destination_table', help="Destination Table in Bigquery to save results, if empty a temporary table will be used.",
                        default=None)
    parser.add_argument('-s', '--storage_uri', help="GCS location for output. Example gs://clue/folder/output.csv. "
                        "Default is to write to timestamped directory", default=None)
    parser.add_argument('-k', '--key', help="Path to service account key. \n Alternatively, set GOOGLE_APPLICATION_CREDENTIALS", default=None)
    parser.add_argument('-o', '--out', help="Output folder", default=os.getcwd())
    parser.add_argument('-c', '--create_subdir', help="Create Subdirectory", type=str2bool, default=True)
    args = parser.parse_args(argv)
    return args

def write_args(args, out_path):
    options = vars(args)
    with open(os.path.join(out_path, 'config.txt'), 'w+') as f:
        for option in options:
            f.write("{}: {}\n".format(option, options[option]))
            print("{}: {}".format(option, options[option]))


def write_status(success, out, exception=""):
    if success:
        print("Successfully writted output to {}".format(out))
        with open(os.path.join(out, 'SUCCESS.txt'), 'w') as file:
            file.write("Finished on {}\n".format(datetime.now().strftime('%c')))
    else:
        print(traceback.format_exc())
        print("FAILURE: Output and stack traced saved to {}".format(out))
        with open(os.path.join(out, 'FAILURE.txt'), 'w') as file:
            file.write(str(exception))
            file.write(traceback.format_exc())

def mk_out_dir(path, toolname, create_subdir=True):
    path = os.path.abspath(path)
    if not os.path.exists(path):
        os.makedirs(path)

    if create_subdir:
        timestamp = datetime.now().strftime('_%Y%m%d%H%M%S')
        out_name = ''.join([toolname, timestamp])
        out_path = os.path.join(path, out_name)
        os.makedirs(out_path)
        return out_path
    else:
        return path

def run_query(query, client, args):
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
    result_bucket = 'clue_queries'

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


    return extract_job

def download_from_extract_job(extract_job, destination_path):
    """Downloads a blob from the bucket."""
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
        fn = os.path.basename(blob.name)
        blob.download_to_filename(os.path.join(destination_path, fn))
        filelist.append(fn)

    return filelist

def csv_gz_to_gctx(filepaths, args):
    pass

def csv_to_gctx(filepaths, args):
    pass


if __name__ == '__main__':
    args = parse_args(sys.argv[1:])

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
        file_list = download_from_extract_job(extract_job, out_path)

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



