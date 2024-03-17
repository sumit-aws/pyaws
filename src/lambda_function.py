import json
import boto3
import random
import uuid
from datetime import datetime,timedelta

sqs_client = boto3.client('sqs')
QUEUE_URL = 'https://sqs.ap-south-1.amazonaws.com/471112572097/airbnb-booking-queue'

def produceairbnbbookingdata():
    city = ['pune', 'mumbai', 'hyderabad', 'delhi', 'chennai']
    days = [1,2,1,3,1,4,1,5,6,1]
    bookingID = str(uuid.uuid4())
    userID = "U"+str(random.randint(10000, 99999))
    propertyID = "P"+str(random.randint(1000, 9999))
    location = random.choice(city)
    start_date =  datetime.strptime("2024-01-" + str(random.randint(10, 31)), "%Y-%m-%d")
    end_date =   start_date + timedelta(days=random.choice(days))
    num_of_days = (end_date - start_date).days
    price = num_of_days * 20
    
    data = {"bookingID": bookingID,
            "userID": userID,
            "propertyID": propertyID,
            "location": location,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "num_of_days": num_of_days,
            "price": price}

    return data

def lambda_handler(event, context):
    i=0
    while(i<2):
        booking_data = produceairbnbbookingdata()
        print(booking_data)
        sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(booking_data)
        )
        i += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps('Sales order data published to SQS!')
    }
