    import configparser
    import psycopg2
    import boto3    
    from sql_queries import copy_table_queries, insert_table_queries, process_table, song_table, user_table, artist_table, time_table, songplay_table


    def load_staging_tables(cur, conn):
        """This function execute the COPY statements to load JSON file data
        into the staging tables STG_EVENTS & STG_SONGS
        """
        for query in copy_table_queries:
            cur.execute(query)
            conn.commit()
        cur.execute("select count(*) from stg_events")
        print(cur.fetchone()[0])
        cur.execute("select count(*) from stg_songs")
        print(cur.fetchone()[0])

    def insert_tables(cur, conn):
        """This function performs the necessary transformations & load targets.
        Source - stg_events & stg_songs
        Target - songs, artists, users, time, songplays
        Load type - designed to do merge, that is load incremental data.
                For 'users' table alone update of attribute 'level' happens
        """
        for tgt in process_table:
            for query in tgt:
                cur.execute(query)
            conn.commit()


    def main():
        config = configparser.ConfigParser()
        config.read('dwh.cfg')
        redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=config.get('AWS','KEY'),
                       aws_secret_access_key=config.get('AWS','SECRET')
                       )

        #This step is to extract the ARN & ENDPOINT information to be used in further steps.
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("CLUSTER","CLS_IDENTIFIER"))['Clusters'][0]        
        aws_arn, aws_clstr = myClusterProps['IamRoles'][0]['IamRoleArn'], myClusterProps['Endpoint']['Address']
        
        print(aws_arn)
        print(type(aws_arn))
        
        #Create a connection for the REDSHIFT cluster
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(aws_clstr, 
                                                                                   config.get("DWH","DB_NAME"),
                                                                                   config.get("DWH","DB_USER"),
                                                                                   config.get("DWH","DB_PASSWORD"),
                                                                                   config.get("DWH","DB_PORT"))
                               )
        
        #Create a cursor for connection
        cur = conn.cursor()
        
        #load JSON data into staging tables in redshift
        load_staging_tables(cur, conn)

        #process staging table data and perform necesary transformations and load targets.
        insert_tables(cur, conn)

        conn.close()
        print("etl.py process completed successfully ! Well done !!")

    if __name__ == "__main__":
        main()