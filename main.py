import requests
from functional import seq
import json
import boto3
import configparser
import time
import snowflake.connector

def extractJsonFromRestApi():
    # Example of bike info returned
    # {
    #     "date_stolen": 1627241794,
    #     "description": "editor's note - serial entered as \"0106\" but this is incomplete",
    #     "frame_colors":
    #         [
    #             "Black"
    #         ],
    #     "frame_model": "Turbo X",
    #     "id": 1109965,
    #     "is_stock_img": false,
    #     "large_img": null,
    #     "location_found": null,
    #     "manufacturer_name": "Specialized",
    #     "external_id": null,
    #     "registry_name": null,
    #     "registry_url": null,
    #     "serial": "Unknown",
    #     "status": null,
    #     "stolen": true,
    #     "stolen_location": "Langley, WA - US",
    #     "thumb": null,
    #     "title": "2016 Specialized Turbo X",
    #     "url": "https://bikeindex.org/bikes/1109965",
    #     "year": 2016
    # }

    urlSearch = "https://bikeindex.org:443/api/v3/search?page=1&per_page=25&location=IP&distance=10&stolenness=all"
    return seq(requests.get(urlSearch).json()['bikes'])

def writeJsonFile(dataJson):
    with open('bikes.json', 'w') as file:
        for entry in dataJson.sequence:
            file.write(json.dumps(entry))
            file.write("\n")
    file.close()

def uploadJsonToDatalakeS3():
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    access_key = parser.get("aws_boto_credentials", "access_key")
    secret_key = parser.get("aws_boto_credentials", "secret_key")
    bucket_name = parser.get("aws_boto_credentials", "bucket_name")

    client = boto3.client("s3", aws_access_key_id=access_key,aws_secret_access_key=secret_key)
    filenameInS3 = "bikes_" + time.strftime("%Y-%m-%dT%H:%M:%S")+".json"
    client.upload_file("bikes.json", bucket_name, filenameInS3)
    return filenameInS3

def loadJsonToDatawarehouseSnowflake(filenameInS3):
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    username = parser.get("snowflake_creds", "username")
    password = parser.get("snowflake_creds", "password")
    account_name = parser.get("snowflake_creds", "account_name")
    database = parser.get("snowflake_creds", "database")

    snow_conn = snowflake.connector.connect(user=username, password=password, account=account_name, database=database, schema="ingestion")
    cur = snow_conn.cursor()
    sql = "insert into epam.ingestion.stage(raw, filename, copied_at) select *, '" + filenameInS3 + "', CURRENT_TIMESTAMP FROM '@bikes/'"
    cur.execute(sql)
    cur.close()

def transformCopiedData(filename):
    return

if __name__ == '__main__':
    # extract
    bikesJson = extractJsonFromRestApi()
    # load
    writeJsonFile(bikesJson)
    filenameInS3 = uploadJsonToDatalakeS3()
    # instead of running this daily, this can be simplified using Snowpipes, so when a new file pop up in S3, the copy process is executed
    loadJsonToDatawarehouseSnowflake(filenameInS3)
    # transform (our events are about stolen bikes, so we will use factless tables, there is nothing special to meassure about this events)
    transformCopiedData(filenameInS3)
     
