import boto3,json
from pg import DB
import sys
from awsglue.utils import getResolvedOptions
import datetime

now = datetime.datetime.now()
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'username', 'password', 'host', 'dbname', 'from_path'])
username = args['username']
password = args['password']
host = args['host']
dbname = args['dbname']
from_path = args['from_path']

print("load data incrementally from: ", from_path, now)


db = DB(dbname=dbname,host=host,port=5439,user=username,passwd=password)

table = str(from_path).split('/')[5]
print(table)
stage_table = "stage" + table 


create_staging_table = "create temp table {0} (like {1});".format(stage_table, table)
db.query(create_staging_table)
print("Execute create staging table", now)

copy_query = """
            copy {0} from {1}
            credentials 'aws_access_key_id={0};aws_secret_access_key={1}'.format(args['access_key'], args['secret_access_key'])
			CSV;
            """.format(stage_table, "'" + str(from_path) + "'")
data = db.query(copy_query)
print("Execute copy query", now)

merge_qry = """
            begin transaction;
            
            delete from {0}
            using {1}
            where {0}.salesid = {1}.salesid;

            insert into {0}
            select * from {1};
            
            end transaction;
            """.format(table, stage_table)
result = db.query(merge_qry)
print("Execute merge query", now)

drop_staging_table = """ 
                    drop table {0};
                    """.format(stage_table)
db.query(drop_staging_table)             
print("Execute drop staging table", now)
