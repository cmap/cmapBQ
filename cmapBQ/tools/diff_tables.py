from google.cloud import bigquery
from datetime import datetime
import os, argparse, sys

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
    parser.add_argument('table1', help="Input Table")
    parser.add_argument('table2', help="Reference Table")
    parser.add_argument('--join', choices=["FULL", "INNER", "LEFT", "RIGHT"], help="Join Type, joins on rid AND cid")
    parser.add_argument('-d', '--destination', help="Table address to place results, appends with timestamp if table present", default=None)
    parser.add_argument('-k', '--key', help="Path to service account key. \n Alternatively, set GOOGLE_APPLICATION_CREDENTIALS", default=None)
    parser.add_argument("--replace", type=str2bool, nargs='?',
                        const=True, default=False,
                        help="If True, destination table deleted before placing results")

    args = parser.parse_args(argv)
    return args

def diff_table(table_a, table_b, args):
    join_type = 'FULL'
    timezone = "America/New_York"

    QUERY = (
        'SELECT '
        '  FORMAT_DATETIME("%Y-%m-%d %T", CURRENT_DATETIME("{}") ) AS time, '
        '  IFNULL(table1.rid, table2.rid) AS rid,'
        '  IFNULL(table1.cid, table2.cid) AS cid,'
        '  table1.value value1,'
        '  table2.value value2'
        ' FROM `{}` AS table1'
        ' {} JOIN `{}` AS table2'
        ' ON table1.rid = table2.rid AND table1.cid = table2.cid'
        ' WHERE TO_JSON_STRING(table1) != TO_JSON_STRING(table2)'.format(timezone, table_a, join_type, table_b))

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    #set job settings
    if args.destination is not None:
        job_config.destination = args.destination
        if args.replace is True:
            print ("--replace True: deleting table `{}` ".format(args.destination))
            client.delete_table(args.destination, not_found_ok=True)
            print ("Done")
    job_config.create_disposition = 'CREATE_IF_NEEDED'
    job_config.write_disposition = "WRITE_APPEND"

    query_job = client.query(QUERY, job_config=job_config)  # API request

    return query_job.result()


def write_success(success):
    if success:
        with open('SUCCESS.txt', 'w+') as file:
            file.write()

def main(argv=None):
    args = parse_args(argv)
    if args.key is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key
    result = diff_table(args.table1, args.table2, args)

if __name__ == '__main__':
    main(sys.argv[1:])

