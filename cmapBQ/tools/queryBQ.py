import re
import os, argparse, sys, traceback
from datetime import datetime
import gzip, shutil

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.auth import exceptions

from cmapBQ.utils import write_args, write_status, mk_out_dir
from cmapBQ.query import run_query, export_table, download_from_extract_job
from cmapBQ.utils.file import csv_to_gctx


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Runs Query in BQ, downloads the results and convert results from CSV to GCT"
    )
    parser.add_argument(
        "-q",
        "--query",
        help="Source Table, rows that exist in both will this table's value",
    )
    parser.add_argument(
        "-d",
        "--destination_table",
        help="Destination Table in Bigquery to save results, if empty a temporary table will be used.",
        default=None,
    )
    parser.add_argument(
        "-s",
        "--storage_uri",
        help="GCS location for output. Example gs://clue/folder/output.csv. "
        "Default is to write to timestamped directory",
        default=None,
    )
    parser.add_argument(
        "-k",
        "--key",
        help="Path to service account key. \n Alternatively, set GOOGLE_APPLICATION_CREDENTIALS",
        default=None,
    )
    parser.add_argument("-o", "--out", help="Output folder", default=os.getcwd())
    parser.add_argument(
        "-c", "--create_subdir", help="Create Subdirectory", type=str2bool, default=True
    )
    parser.add_argument(
        "-g", "--use_gctx", help="Use GCTX format, default is true", default=True
    )

    if argv is None:
        parser.print_help()
        sys.exit(1)
    else:
        args = parser.parse_args(argv)
        return args


def gunzip_csv(filepaths, destination_path=None):
    """
    Unzips list of CSVs
    :param filepaths: Path of files with '.gz' extensions
    :param destination_path: folder to place unzipped files. If None, places in input directory.
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
    return out_paths


def main(argv=None):
    args = parse_args(argv)

    out_path = mk_out_dir(args.out, "queryBQ", create_subdir=args.create_subdir)
    write_args(args, out_path)

    if args.key is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key

    try:
        bigquery_client = bigquery.Client()

        # run query
        query_job = run_query(args.query, bigquery_client, args.destination_table)
        # extract table to GCS
        extract_job = export_table(query_job, bigquery_client)
        # download from GCS
        csv_path = os.path.join(out_path, "csv")
        os.mkdir(csv_path)
        file_list = download_from_extract_job(extract_job, csv_path)
        file_list = gunzip_csv(file_list, csv_path)
        csv_to_gctx(file_list, out_path, use_gctx=args.use_gctx)

        print("result_destination: {}".format(extract_job.destination_uris[0]))

        write_status(True, out_path)
    except exceptions.DefaultCredentialsError as cred_error:
        print(
            "Could not automatically determine credentials. Please set GOOGLE_APPLICATION_CREDENTIALS or"
            " specify path to key using --key"
        )
        write_status(False, out_path, exception=cred_error)
        exit(1)
    except Exception as e:
        write_status(False, out_path, exception=e)
        exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
