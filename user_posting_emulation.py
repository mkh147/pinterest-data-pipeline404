import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
#from decouple import config


random.seed(100)



HOST="pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
USER="project_user"
PASSWORD=":t%;yCY3Yjg"
DATABASE='pinterest_data'
PORT=3306


class AWSDBConnector:

    def __init__(self):

        self.HOST = HOST
        self.USER = USER
        self.PASSWORD = PASSWORD
        self.DATABASE = DATABASE
        self.PORT = PORT
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    def send_data_to_topic(api_url, topic, data):
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        payload = json.dumps({
            "records": [
                {
                    "value": data
                }
            ]
        }, default=str)
        response = requests.request("POST", api_url, headers=headers, data=data)
        if response.status_code != 200:
            print(f"Failed to send data to {topic}. Status code: {response.status_code}")
        else:
            print(f"Successfully sent data to {topic}")
        return response

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)
            pin_res = json.dumps({
                "records": [
                    {
                    "value": pin_result
                    }
                ]
            }, default=str)
            geo_res = json.dumps({
                "records": [
                    {
                    "value": geo_result
                    }
                ]
            }, default=str)
            user_res = json.dumps({
                "records": [
                    {
                    "value": user_result
                    }
                ]
            }, default=str)
            
            topics = ["user_id.pin", "replace.geo", "replace.user"]

            url = 'https://j0itaq5qpg.execute-api.us-east-1.amazonaws.com/dev/topics/0e1a30bcc1ff.pin'
            url1 = 'https://j0itaq5qpg.execute-api.us-east-1.amazonaws.com/dev/topics/0e1a30bcc1ff.geo'
            url2 = 'https://j0itaq5qpg.execute-api.us-east-1.amazonaws.com/dev/topics/0e1a30bcc1ff.user'

            ps = send_data_to_topic(url, topics[0], pin_res)
            ps1 = send_data_to_topic(url1, topics[1], geo_res)
            ps2 = send_data_to_topic(url2, topics[2], user_res)
            print(ps.status_code)
            print(ps1.status_code)
            print(ps2.status_code)







if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
