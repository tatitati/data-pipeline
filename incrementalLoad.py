import requests
from functional import seq
import json
import boto3
import configparser
import time
import snowflake.connector

url = "https://bikeindex.org:443/api/v3/search?page=1&per_page=15&location=IP&distance=10&stolenness=all"

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
    return seq(requests.get(url).json()['bikes'])

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

def loadJsonToDatawarehouseSnowflake(filenameInS3, url):
    sql = f"""insert into epam.ingestion.stage(raw, filename, copied_at, url) 
            select 
                *, 
                '{filenameInS3}', 
                CURRENT_TIMESTAMP,
                '{url}'
            FROM '@bikes/{filenameInS3}'"""

    executeQuery("ingestion", sql)

def populateDimBike():
    inactiveBike = """        
        MERGE INTO epam.datamodel.dim_bike as dim_bike USING (select * from ingestion.stage where ingestion.stage.integrated_at is null) as stage ON stage.raw:id = dim_bike.id
        WHEN MATCHED and valid=true THEN 
            update set valid_to = CURRENT_TIMESTAMP(), valid = false;        
        """
    executeQuery("ingestion", inactiveBike)

    insertNewOrUpdated = """
            insert into datamodel.dim_bike(id, description, frame_model, manufacturer_name, serial, valid_from, valid_to, valid)
            select 
                raw:id,
                raw:description,
                raw:frame_model,
                raw:manufacturer_name,
                raw:serial,
                CURRENT_TIMESTAMP,
                '9999-02-20 00:00:00.000' as datetime,
                true
            from ingestion.stagev;
        """
    executeQuery("ingestion", insertNewOrUpdated)

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

def markStageIntegrationCompleted():
    factlesStolenBike = """            
        update epam.ingestion.stage
        set integrated_at =  CURRENT_TIMESTAMP()
        where integrated_at is null
        """

    executeQuery("ingestion", factlesStolenBike)        

if __name__ == '__main__':
    # extract
    bikesJson = extractJsonFromRestApi()

    # load to s3
    writeJsonFile(bikesJson)
    filenameInS3 = uploadJsonToDatalakeS3()
    # # load to DW
    loadJsonToDatawarehouseSnowflake(filenameInS3, url)
    # Transform: Update bike dim
        # populateDimBike()
    # Transform: Add to factless table
        # populateFactlessBikeStolen()

    # mark staged integration completed
        # markStageIntegrationCompleted()