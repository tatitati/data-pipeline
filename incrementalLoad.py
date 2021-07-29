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
    markAsInactiveOutOfDate = """        
        update dim_bike set 
            isActive = false,
            valid_to = current_timestamp()
        where id in (
            SELECT dim_bike.id
            from ingestion.stage stage
            left join datamodel.dim_bike dim_bike  on dim_bike.id = stage.raw:id
            where   
                md5(to_varchar(array_construct(
                    stage.raw:id, 
                    stage.raw:description, 
                    stage.raw:frame_model, 
                    stage.raw:manufacturer_name,
                    stage.raw:serial
                ))) <> dim_bike.entryHash
        );
        """
    executeQuery("ingestion", markAsInactiveOutOfDate)

    insertNewOrUpdated = """        
        insert into user_dim(userId, name, postcode, valid_from, valid_to, isActive, userHash)
        SELECT 
            user_ingestion.id, 
            user_ingestion.name,
            user_ingestion.postcode,
            current_timestamp(),
            '9999-02-20 00:00:00.000' as datetime,
            true,
            md5(to_varchar(array_construct(user_ingestion.id, user_ingestion.name, user_ingestion.postcode)))
        from user_ingestion user_ingestion
        left join user_dim  on user_dim.userId = user_ingestion.id
        where   md5(to_varchar(array_construct(user_ingestion.id, user_ingestion.name, user_ingestion.postcode))) <> userHash;
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