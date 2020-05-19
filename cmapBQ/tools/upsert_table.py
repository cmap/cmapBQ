from google.cloud import bigquery
from google.auth import exceptions
from datetime import datetime
import pandas as pd
import os, argparse, sys, traceback

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
    parser.add_argument('-a', '--from_table', help="Source Table, rows that exist in both will this table's value")
    parser.add_argument('-b', '--to_table', help="Destination Table, matching rows will have value overwritten")
    parser.add_argument('-k', '--key', help="Path to service account key. \n Alternatively, set GOOGLE_APPLICATION_CREDENTIALS", default=None)
    parser.add_argument("--in_place", type=str2bool, nargs='?',
                        const=True, default=False,
                        help="If false, to_table will be copied before performing merge. Default is False")
    parser.add_argument('-o', '--out', help="Output folder", default=os.getcwd())
    parser.add_argument('-c', '--create_subdir', help="Create Subdirectory", type=str2bool, default=True)
    args = parser.parse_args(argv)
    return args


'''
Perform upsert
Query uses Google's Data Manipulation Language (DML) to insert rows into from_table that
are not present in the to_table, and updates them where they match. Matched on (rid AND cid)
'''
def upsert_table(from_table, to_table, args):
    client = bigquery.Client()

    if args.in_place is False:
        if False:
            timestamp = datetime.now().strftime('_%Y%m%d%H%M%S')
            dest = to_table + timestamp

        else:
            tokens = to_table.split('.')
            dataset_id = ".".join(tokens[:-1])
            table_name = tokens[-1]   # 'id' is used for full reference. 'name' is used for short table name

            ds_tables = client.list_tables(dataset_id)
            table_names = [table.table_id for table in ds_tables]

            if "{}_copy".format(table_name) not in table_names:
                cp_name = "{}_copy".format(table_name)
            else:
                i = 2
                while "{}_copy{}".format(table_name, i) in table_names:
                    i += 1
                cp_name = "{}_copy{}".format(table_name, i)

            dest = ".".join([dataset_id, cp_name])

        print("Copying table {} to table {}".format(to_table, dest))
        copy_job = client.copy_table(sources=to_table, destination=dest)
        copy_job.result()
        to_table = dest


    COUNT_TO = 'SELECT count(*) as nrows FROM `{}`'.format(to_table)
    COUNT_FROM = 'SELECT count(*) as nrows FROM `{}`'.format(from_table)

    count_to = client.query(COUNT_TO)
    count_from = client.query(COUNT_FROM)

    result = count_to.result()       #returns single row RowIterator with count
    n_to_pre = [n.nrows for n in result]
    result = count_from.result()
    n_from_pre = [n.nrows for n in result]

    print("Table {} found with {} rows".format(from_table, n_from_pre))
    print("Table {} found with {} rows".format(to_table, n_to_pre))


    MERGE_QUERY = ('MERGE `{}` A '
                   'USING `{}` B '
                   'ON A.rid = B.rid AND A.cid = B.cid '
                   'WHEN MATCHED THEN UPDATE SET value = B.value '
                   'WHEN NOT MATCHED THEN INSERT (rid, cid, value) VALUES(rid, cid, value)')

    MERGE_QUERY = MERGE_QUERY.format(to_table, from_table)


    print("Merging table {} into table {}".format(from_table, to_table))
    query_job = client.query(MERGE_QUERY)  # API request'

    result = query_job.result()  # Waits for query to finish
    print("Merge complete")

    count_post = client.query(COUNT_TO)
    result = count_post.result()
    n_to_post = [n.nrows for n in result]

    num_insert = n_to_post[0] - n_to_pre[0]
    num_update = n_from_pre[0] - num_insert

    rpt = {'num_inserted': [num_insert], 'num_updated': [num_update]}
    print("{} rows added to {}".format(num_insert, to_table))
    print("{} rows matched and updated to {}".format(num_update, to_table))
    rpt = pd.DataFrame(data=rpt, columns=['num_inserted', 'num_updated'])
    return rpt

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


def main(argv = None):
    args = parse_args(argv)
    if args.key is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key

    out_path = mk_out_dir(args.out, "upsert_table", create_subdir=args.create_subdir)
    write_args(args, out_path)

    try:
        rpt = upsert_table(args.from_table, args.to_table, args)
        rpt.to_csv(os.path.join(out_path, 'report.txt'), sep='\t', index=False)
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
