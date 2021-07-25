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
    #     "date_stolen": 1627232851,
    #     "description": null,
    #     "frame_colors": [
    #         "Teal"
    #     ],
    #     "frame_model": "Lux XE",
    #     "id": 1109834,
    #     "is_stock_img": false,
    #     "large_img": null,
    #     "location_found": null,
    #     "manufacturer_name": "Diamondback",
    #     "external_id": null,
    #     "registry_name": null,
    #     "registry_url": null,
    #     "serial": "DAF15F015598",
    #     "status": null,
    #     "stolen": true,
    #     "stolen_location": "York, PA - US",
    #     "thumb": null,
    #     "title": "2016 Diamondback Lux XE",
    #     "url": "https://bikeindex.org/bikes/1109834",
    #     "year": 2016
    # },

    urlSearch = "https://bikeindex.org:443/api/v3/search?page=1&per_page=25&location=IP&distance=10&stolenness=all"
    urlBikeDetail = "https://bikeindex.org:443/api/v3/bikes/"
    bikes = seq(requests.get(urlSearch).json()['bikes'])
    return bikes.map(lambda bike: requests.get(urlBikeDetail + str(bike['id'])).json())

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
    schema = parser.get("snowflake_creds", "schema")

    snow_conn = snowflake.connector.connect(user=username, password=password, account=account_name, database=database, schema=schema)
    cur = snow_conn.cursor()
    sql = "insert into epam.myschema.stage(raw, filename, copied_at) select *, '" + filenameInS3 + "', CURRENT_TIMESTAMP FROM '@bikes/'"
    cur.execute(sql)
    cur.close()

def transformCopiedData(filename):


if __name__ == '__main__':
    # extract
    bikesJson = extractJsonFromRestApi()
    # load
    writeJsonFile(bikesJson)
    filenameInS3 = uploadJsonToDatalakeS3()
    loadJsonToDatawarehouseSnowflake(filenameInS3)
    # transform
    transformCopiedData(filenameInS3)
     
