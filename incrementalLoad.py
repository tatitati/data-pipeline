import requests
from functional import seq
import json
import boto3
import configparser
import time
import snowflake.connector

def getS3Client():
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    access_key = parser.get("aws_boto_credentials", "access_key")
    secret_key = parser.get("aws_boto_credentials", "secret_key")

    return boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

def executeQuery(schema, query):
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    username = parser.get("snowflake_creds", "username")
    password = parser.get("snowflake_creds", "password")
    account_name = parser.get("snowflake_creds", "account_name")
    database = parser.get("snowflake_creds", "database")

    snow_conn = snowflake.connector.connect(user=username, password=password, account=account_name, database=database,schema=schema)
    cur = snow_conn.cursor()
    cur.execute(query)
    cur.close()

def extractJsonFromRestApi():
    urlSearch = "https://bikeindex.org:443/api/v3/search?page=1&per_page=15&location=IP&distance=10&stolenness=all"
    return seq(requests.get(urlSearch).json()['bikes'])

def writeJsonFile(dataJson):
    amountRecords = 0
    with open('bikes.json', 'w') as file:
        for entry in dataJson.sequence:
            file.write(json.dumps(entry))
            file.write("\n")
            amountRecords += 1
    file.close()
    return amountRecords

def uploadJsonToDatalakeS3():
    s3 = getS3Client()
    filenameInS3 = "bikes_" + time.strftime("%Y-%m-%dT%H:%M:%S")+".json"
    s3.upload_file("bikes.json", "b-i-k-e-s", filenameInS3)
    return filenameInS3

def loadJsonToDatawarehouseSnowflake(filenameInS3, amountOfRecords):
    sql = f"""insert into epam.ingestion.stage(raw, filename, copied_at) 
            select 
                *, 
                '{filenameInS3}', 
                CURRENT_TIMESTAMP 
            FROM '@bikes/{filenameInS3}'"""

    executeQuery("ingestion", sql)

def transformStagedDate():
    dimBikesSql = """        
        MERGE INTO target_table USING source_table 
            ON target_table.id = source_table.id
        WHEN MATCHED THEN 
            UPDATE SET target_table.description = source_table.description
        WHEN NOT MATCHED THEN 
            INSERT (ID, description) VALUES (source_table.id, source_table.description);
        """
    executeQuery("ingestion", dimBikesSql)


def populateFactlessBikeStolen():
        factlesStolenBike = """            
            insert into datamodel.factless_bikes_stolen(bikeid, date, fact, location)
            select
                raw:id,
                date(to_timestamp(raw:date_stolen)),
                case 
                    when raw:stolen = 'true' then 'stolen'
                    else 'found'
                end as fact,
                case 
                    when raw:stolen = 'true' then raw:stolen_location
                    else raw:location_found
                end as location             
            from ingestion.stage
            where integrated_at is null;
            """

        executeQuery("ingestion", factlesStolenBike)

if __name__ == '__main__':
    # extract
    bikesJson = extractJsonFromRestApi()

    # load to s3
    # writeJsonFile(bikesJson)
    # filenameInS3 = uploadJsonToDatalakeS3()
    # # load to DW
    # loadJsonToDatawarehouseSnowflake(filenameInS3)
    # # Transform
    # transformStagedDate()

