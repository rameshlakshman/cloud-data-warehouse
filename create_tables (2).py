import configparser
import boto3
import psycopg2
import json
import time
import pandas as pd
from sql_queries import create_table_queries, drop_table_queries

def create_cluster_role(config):
    """This function calls below two other functions
           create_role    --> to create a new IAM role and attach required policies
           create_cluster --> to create a new REDSHIFT cluster
    """    
    iam = boto3.client('iam',aws_access_key_id=config.get('AWS','KEY'),
                     aws_secret_access_key=config.get('AWS','SECRET'),
                     region_name='us-west-2'
                  )

    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=config.get('AWS','KEY'),
                       aws_secret_access_key=config.get('AWS','SECRET')
                       )

    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=config.get('AWS','KEY'),
                       aws_secret_access_key=config.get('AWS','SECRET')
                    )
    
    roleArn = create_role(config, iam)
    
    arnClstr, hostClstr = create_cluster(config, redshift, ec2, roleArn)
    
    return arnClstr, hostClstr

def create_role(config, iam):
    """Funciton to create a new role for Redshift cluster that will be created
    in next step. Name of role is mentioned in dwh.cfg file.
       Also, attach policies S3readonly & RedshiftFullAccess upon creation.
       If role already exists, proceed without failing.
    """
    #1.1 Create the role, 
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=config.get("IAM_ROLE", "IAM_ROLE_NAME"),
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=config.get("IAM_ROLE", "IAM_ROLE_NAME"),
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    iam.attach_role_policy(RoleName=config.get("IAM_ROLE", "IAM_ROLE_NAME"),
                       PolicyArn="arn:aws:iam::aws:policy/AmazonRedshiftFullAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
    roleArn = iam.get_role(RoleName=config.get("IAM_ROLE", "IAM_ROLE_NAME"))['Role']['Arn']
    return roleArn

 
def create_cluster(config, redshift, ec2, roleArn):  
    """  Function to create new Redshift cluster.
       Program must wait till the cluster is created before proceeding. This is 
       accomplished with 'time' module with 'while' loop & describe_clusters method.
         Once cluster ready, authorize security group to accept TCP connections inbound
       and attach same security-group-id to the cluster created.
    """
    try:
        print("Creating Cluster")
        response = redshift.create_cluster(        
            #HW
            ClusterType=config.get("CLUSTER","CLS_CLUSTER_TYPE"),
            NodeType=config.get("CLUSTER","CLS_NODE_TYPE"),
            NumberOfNodes=int(config.get("CLUSTER","CLS_NUM_NODES")),

            #Identifiers & Credentials
            DBName=config.get("DWH","DB_NAME"),
            ClusterIdentifier=config.get("CLUSTER","CLS_IDENTIFIER"),
            MasterUsername=config.get("DWH","DB_USER"),
            MasterUserPassword=config.get("DWH","DB_PASSWORD"),
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
        print("Cluster Ready")
    except Exception as e:
        print(e)    

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("CLUSTER","CLS_IDENTIFIER"))['Clusters'][0]
    
    while myClusterProps["ClusterStatus"] != 'available':
        print("Cluster status is : ",myClusterProps["ClusterStatus"])
        print("wait for 60 seconds to check again ")
        time.sleep(60)
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("CLUSTER","CLS_IDENTIFIER"))['Clusters'][0]

    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg.group_name, defaultSg.group_name)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config.get("DWH","DB_PORT")),
            ToPort=int(config.get("DWH","DB_PORT"))
        )
    except Exception as e:
        print(e)    

    try:
        response = redshift.modify_cluster(
            ClusterIdentifier=config.get("CLUSTER","CLS_IDENTIFIER"),
            VpcSecurityGroupIds=[ defaultSg.group_id, ]
        )
    except Exception as e:
        print(e)         
        
    return myClusterProps['IamRoles'][0]['IamRoleArn'], myClusterProps['Endpoint']['Address']

   
def drop_tables(cur, conn):
    """This function performs DROP of 2 staging tables (event_data & song_data)
       and 5 target tables (songs, users, artists, time, songplays)
    """    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Drop tables complete ")


def create_tables(cur, conn):
    """This function performs CREATE of 2 staging tables (event_data & song_data)
       and 5 target tables (songs, users, artists, time, songplays)
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Create tables complete")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')    
    
    #Below call is to 
    #           create a new REDSHIFT cluster
    #           create a new IAM role
    #           return ARN & ENDPOINT
    aws_arn, aws_clstr = create_cluster_role(config)
    
    #Create connection to perform DROP and CREATE tables
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(aws_clstr, 
                                                                                   config.get("DWH","DB_NAME"),
                                                                                   config.get("DWH","DB_USER"),
                                                                                   config.get("DWH","DB_PASSWORD"),
                                                                                   config.get("DWH","DB_PORT")
                                                                                  )
                           )
    
    print("psycopg2 connection established")
    
    #Create cursor for connection
    cur = conn.cursor()
    
    #drop and create table calls
    drop_tables(cur, conn)
    create_tables(cur, conn)    

    conn.close()
    print("create_tables.py completed successfully !!")

if __name__ == "__main__":
    main()