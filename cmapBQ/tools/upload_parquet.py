from google.cloud import bigquery
from google.api_core import exceptions as api_exception
from google.auth import exceptions
from cmapPy.set_io import grp
import glob, re, pytz
import os, argparse, sys, traceback
from datetime import datetime
from datetime import timedelta
import time

''' 
Function to convert strings to boolean
'''
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
    parser.add_argument('-d', '--dir',  help="Directory of parquet files",default=None)
    parser.add_argument('-l', '--grp', help="List of filenames as grp to upload", default=None)
    parser.add_argument('-t', '--table', help="Name of bigquery table")
    parser.add_argument('-f', '--source_format', help="Name of bigquery table",
                        default="PARQUET", choices=["PARQUET","CSV", "JSON"])
    parser.add_argument("--replace", type=str2bool, nargs='?',
                        const=True, default=False,
                        help="If true, existing data is erased when new data is loaded. Default is false")
    parser.add_argument("--expires", type=int, nargs='?',
                            const=5, default=None,
                            help="Number of days after which to delete the table")
    parser.add_argument("--append", type=str2bool, nargs='?',
                        const=True, default=False,
                        help="If true, new data is appended to existing. Default is false")
    parser.add_argument('-dlm', '--delimiter', help="The separator for fields in a CSV file.",
                        default=',')
    parser.add_argument('-k', '--key', help="Path to service account key. \n Alternatively, set GOOGLE_APPLICATION_CREDENTIALS", default=None)
    parser.add_argument('-o', '--out', help="Output folder", default=os.getcwd())
    parser.add_argument('-c', '--create_subdir', help="Create Subdirectory", type=str2bool, default=True)
    args = parser.parse_args(argv)
    return args

"""
Get BQ type from String input
"""
def get_bq_type(type_str):
    if type_str == "PARQUET":
        return  bigquery.SourceFormat.PARQUET
    elif type_str == "CSV":
        return bigquery.SourceFormat.CSV
    elif type_str == "JSON":
        return bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    else:
        raise TypeError("--source_format: {} not supported".format(type_str))


def build_job_config(args):
    if args.key is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key
    job_config = bigquery.LoadJobConfig()
    SRC_TYPE = get_bq_type(args.source_format)
    job_config.source_format = SRC_TYPE
    if SRC_TYPE == bigquery.SourceFormat.CSV:       #delimiter arg only allowed on CSV Format
        job_config.field_delimiter = args.delimiter
    job_config.autodetect = False
    job_config.schema = [
        bigquery.SchemaField("rid", "STRING"),
        bigquery.SchemaField("cid", "STRING"),
        bigquery.SchemaField("value", "FLOAT")
    ]       #define schema for gctx data tables
    job_config.create_disposition = 'CREATE_IF_NEEDED'
    job_config.write_disposition = 'WRITE_APPEND'
    return job_config

def table_exist(client, table_id, args):
    try:
        client.get_table(table_id)  # Make an API request.
        return True
    except api_exception.NotFound:
        print("Table {} is not found.".format(table_id))
        return False

def sumbit_job_load(file_list, table_id, args):
    job_config = build_job_config(args)
    client = bigquery.Client()
    table_ref = bigquery.Table(table_id)

    file_list = [f for f in file_list if re.match(r'.*\.parquet', f)]

    print("Files to upload")
    print(file_list)
    failed = []

    if args.replace is True:
        print("Deleting Table `{}` if found ".format(table_id))
        client.delete_table(table_ref, not_found_ok=True)
        print("Creating new table `{}`".format(table_id))
        client.create_table(table_ref)
        print("Done")
    elif args.append is False:      #if replace is true, append is ignored
        if table_exist(client, table_id, args):
            raise api_exception.Conflict("CONFLICT: Table `{}` exists. To append use --append flag".format(table_id))

    for filename in file_list:
        with open(filename, "rb") as source_file:
            try:
                print("Attempting to load table {} with file {}".format(table_id, filename))
                t1 = time.time()
                query_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

                query_job.result()
                t2 = time.time()
                elapsed = t2-t1
                print("Job finished in {0:.3f}".format(elapsed))

                #job.result()  # Waits for table load to complete.
                #t2 = time.time()
                #elapsed = t2-t1
                #print("Loaded {} rows from {} into {} in {} seconds.".format(job.output_rows, filename, table_id, elapsed))
            except:
                failed.append(filename)
                raise

    if args.expires is not None:
        expiration = datetime.now(pytz.utc) + timedelta(days=args.expires)
        table = client.get_table(table_ref)
        table.expires = expiration
        table = client.update_table(table, ["expires"])

    print("Failed at:")
    print(failed)

## To be moved to a class in the future

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
        print("Output and stack traced saved to {}".format(out))
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

def main(argv=None):
    t_start = time.time()
    args = parse_args(argv)

    out_path = mk_out_dir(args.out, "upload_parquet", create_subdir=args.create_subdir)
    write_args(args, out_path)

    if args.grp is not None:
        file_list = grp.read(args.grp)
    elif args.dir is not None:
        if str.endswith(args.dir, ".parquet"):
            file_list = glob.glob(os.path.join(args.dir))
        else:
            file_list = glob.glob(os.path.join(args.dir, '*'))
    else:
        raise FileNotFoundError("Directory or input list grp is required")

    try:
        file_list.sort()
        #Run load job
        sumbit_job_load(file_list, args.table, args)
        write_status(True, out_path)
    except exceptions.DefaultCredentialsError as cred_error:
        print('Could not automatically determine credentials. Please set GOOGLE_APPLICATION_CREDENTIALS or'
              ' specify path to key using --key')
        write_status(False, out_path, exception=cred_error)
        exit(1)
    except Exception as e:
        write_status(False, out_path, exception=e)
        exit(1)

    t_end = time.time()

    print("-------------------")
    print("-------{}-------".format(t_end - t_start))
    print("-------------------")


if __name__ == '__main__':
    main(sys.argv[1:])
