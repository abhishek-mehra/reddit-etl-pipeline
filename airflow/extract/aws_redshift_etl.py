import configparser
import pathlib
import sys
import redshift_connector
from validation import validate_input

# Parse our configuration file
script_path = pathlib.Path(__file__).parent.resolve()
parser = configparser.ConfigParser()
parser.read(f"{script_path}/configuration.conf")

# Store our configuration variables
USERNAME = parser.get("aws_config", "redshift_username")
PASSWORD = parser.get("aws_config", "redshift_password")
HOST = parser.get("aws_config", "redshift_hostname")
PORT = parser.get("aws_config", "redshift_port")
REDSHIFT_ROLE = parser.get("aws_config", "redshift_role")
DATABASE = parser.get("aws_config", "redshift_database")
BUCKET_NAME = parser.get("aws_config", "bucket_name")
ACCOUNT_ID = parser.get("aws_config", "account_id")
TABLE_NAME = "reddit"

# Check command line argument passed
if len(sys.argv) != 2:
    print("Usage: python script.py <output_name>")
    sys.exit(1)

output_name = sys.argv[1]

# Our S3 file & role_string
file_path = f"s3://{BUCKET_NAME}/{output_name}.csv"
role_string = f"arn:aws:iam::{ACCOUNT_ID}:role/{REDSHIFT_ROLE}"

# Create Redshift table if it doesn't exist
sql_create_table = f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                            id varchar PRIMARY KEY,
                            title varchar(max),
                            num_comments int,
                            score int,
                            author varchar(max),
                            created_utc timestamp,
                            url varchar(max),
                            upvote_ratio float,
                            over_18 bool,
                            edited bool,
                            spoiler bool,
                            stickied bool
                        );"""


# If ID already exists in the table, we remove it and add a new ID record during load.
create_temp_table = f"CREATE TEMP TABLE our_staging_table AS SELECT * FROM {TABLE_NAME} WHERE 1=0;"
sql_copy_to_temp = f"COPY our_staging_table FROM '{file_path}' iam_role '{role_string}' IGNOREHEADER 1 DELIMITER ',' CSV;"
delete_from_table = f"DELETE FROM {TABLE_NAME} USING our_staging_table WHERE {TABLE_NAME}.id = our_staging_table.id;"
insert_into_table = f"INSERT INTO {TABLE_NAME} SELECT * FROM our_staging_table;"
drop_temp_table = "DROP TABLE our_staging_table;"


def main():
    """Upload file from S3 to Redshift Table"""
    validate_input(output_name)
    rs_conn = connect_to_redshift()
    load_data_into_redshift(rs_conn)

def connect_to_redshift():
    """Connect to Redshift instance"""
    try:
        rs_conn = redshift_connector.connect(
            host=HOST,
            database=DATABASE,
            port=int(PORT),
            user=USERNAME,
            password=PASSWORD
        )
        return rs_conn
    except Exception as e:
        print(f"Unable to connect to Redshift. Error {e}")
        sys.exit(1)

def load_data_into_redshift(rs_conn):
    """Load data from S3 into Redshift"""
    with rs_conn:
        cur = rs_conn.cursor()

        # Execute SQL queries
        cur.execute(sql_create_table)
        cur.execute(create_temp_table)
        cur.execute(sql_copy_to_temp)
        cur.execute(delete_from_table)
        cur.execute(insert_into_table)
        cur.execute(drop_temp_table)

        # Commit only at the end, so we won't end up with a temp table and deleted main table if something fails
        rs_conn.commit()

if __name__ == "__main__":
    main()
