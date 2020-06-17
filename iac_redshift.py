import boto3
import configparser
import json
import os
import pandas as pd
import psycopg2
from time import sleep

"""
Infrastructure As Code (IaC):  Provision an AWS Redshift cluster based upon 
parameters defined in dwh.cfg.

# ---------------------------------
# Example dwh.cfg file
# ---------------------------------
[AWS]
# Define IAM credentials
KEY=[REDACTED]
SECRET=[REDACTED]

[DWH] 
# define Redshift cluster attributes
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large

DWH_IAM_ROLE_NAME=dwhRole
DWH_CLUSTER_IDENTIFIER=dwhCluster
DWH_DB=dev
DWH_DB_USER=awsuser
DWH_DB_PASSWORD=Passw0rd

REGION=us-west-2

"""

# Read configuration parameters from file
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

REGION = config.get("DWH", "REGION")


iam = boto3.client('iam',
                   region_name=REGION,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   )

redshift = boto3.client('redshift',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

iam = boto3.client('iam',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name=REGION
                   )

redshift = boto3.client('redshift',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )


def create_role():
    """
    Create IAM role with s3 read access.
    """
    try:
        print("1.1 Creating a new IAM Role")
        _dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']


def create_cluster(ROLE_ARN):
    """
    Create a Redshift cluster, e.g. 4 nodes / dc2.large type, and the specified IAM role.
    """
    try:
        _response = redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[ROLE_ARN]
        )
    except Exception as e:
        print(e)


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def check_status(status):
    """
    Checks whether the cluster has the specified status.
    """
    try:
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        while redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus'] != status:
            prettyRedshiftProps(myClusterProps)
            sleep(20)
        prettyRedshiftProps(myClusterProps)
    except:
        print('cluster is deleted')


def test_connection(ENDPOINT, PORT):
    """
    Checks if a connection can be made to the Redshift cluster that was created.
    """
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, PORT))
        cur = conn.cursor()
        cur.execute("SELECT version();")
        for row in cur:
            print('row = %r' % (row,))

        print(config.get("PARSE", "VALUE"))
        conn.close()
        print('connection to cluster successful')
    except:
        print('something went wrong, cannot connect to cluster')


def teardown():
    """
    Deletes Redshift cluster.
    """
    try:
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                                SkipFinalClusterSnapshot=True)
    except:
        print('problem deleting cluster')


def main(action):
    df = pd.DataFrame({"Param":
                       ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER",
                           "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_IAM_ROLE_NAME", "REGION"],
                       "Value":
                       [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER,
                           DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_IAM_ROLE_NAME, REGION]
                       })
    print(df)

    if action.upper() == 'D':
        teardown()
        check_status('deleted')
    else:
        create_role()
        ROLE_ARN = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        print(f'ARN: {ROLE_ARN}')

        create_cluster(ROLE_ARN)
        check_status('available')

        ENDPOINT = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
            'Clusters'][0]['Endpoint']['Address']
        PORT = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
            'Clusters'][0]['Endpoint']['Port']

        test_connection(ENDPOINT, PORT)

        print("===== Airflow Initialization =====")
        print(f"""Airflow AWS Connection
            Conn id:  aws_credentials
            Login:  {KEY}
            Password: {SECRET}
        """)

        print(f"""Airflow Redshift Connection
            Conn id:  redshift
            Conn Type:  Postgres
            Host:   {ENDPOINT}
            Schema: {DWH_DB}
            Login:  {DWH_DB_USER}
            Password: {DWH_DB_PASSWORD}
            Port: {PORT}
        """)

        print("""Airflow Variable
            Key:  s3_bucket
            Value:  udacity-dend
        """)


if __name__ == "__main__":
    action = input(
        '[C]reate or [D]elete Redshift cluster? Enter C to create or D to delete: ')
    main(action)
