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

    client = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    filenameInS3 = "bikes_" + time.strftime("%Y-%m-%dT%H:%M:%S") + ".json"
    client.upload_file("bikes.json", bucket_name, filenameInS3)
    return filenameInS3


def loadJsonToDatawarehouseSnowflake(filenameInS3):
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    username = parser.get("snowflake_creds", "username")
    password = parser.get("snowflake_creds", "password")
    account_name = parser.get("snowflake_creds", "account_name")
    database = parser.get("snowflake_creds", "database")

    snow_conn = snowflake.connector.connect(user=username, password=password, account=account_name, database=database,
                                            schema="ingestion")
    cur = snow_conn.cursor()
    sql = "insert into epam.ingestion.stage(raw, filename, copied_at) select *, '" + filenameInS3 + "', CURRENT_TIMESTAMP FROM '@bikes/" + filenameInS3 + "'"
    cur.execute(sql)
    cur.close()


def ingestNewData():
    # TODO
    # Basically I want to use SCD 2 for changes in dimesion tables to keep the history of these dimensions while I keep appending to fact tables stolen bikes
    # dim_date is not going to change too much (in case we want to update it we can run a simple script that add more dates)
    # For dim_bike we have to join new ingested data (stage table) with dim_bike and see if we already have a bike with that id. In case we receive a new bike
    # we have to add it to the dim_bike.

if __name__ == '__main__':
    bikesJson = extractJsonFromRestApi()
    writeJsonFile(bikesJson)
    filenameInS3 = uploadJsonToDatalakeS3()
    loadJsonToDatawarehouseSnowflake(filenameInS3)
    ingestNewData()

    # TODO: read below
    # ================
    # This would allow to answer a few questions:
    # - When people steal bikes?   join of factles and dim_table
    # - what brands are stealing?: join of dim_bike, factles
    # - what colors are the favourite to steal?: join of dim_bike and factless
    # - how many bikes were stolen in between two dates?: join of factles and dim_date


