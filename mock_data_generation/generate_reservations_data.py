from datetime import datetime
import random
import boto3

sqs_client = boto3.client('sqs')


def generate_mock_reservation_data(num_records):

    records = []
    for i in range(num_records):
        record = {
            "bookingId": f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{i}",
            "userId": random.randint(1, 1000),
            "propertyId": f"property-{i%200}", 
            "location": f"City-{i%5}, Country-{i%3}",  
            "startDate": f"2025-01-{(i%28)+1:02d}",
            "endDate": f"2027-12-{(i%28)+2:02d}",
            "price": round(50 + (i * 3.75) % 450, 2)  
            }
        records.append(record)

    return records

def lambda_handler(event, context):

    try:
        num_records = random.randint(30,50)
        records = generate_mock_reservation_data(num_records=num_records)
        print(f"Generated {num_records} mock reservation records.")
        
        # Send each record as a message to SQS
        for record in records:
            sqs_client.send_message(
                QueueUrl='https://sqs.us-east-1.amazonaws.com/652734276321/AirbnbBookingQueue',
                MessageBody=str(record)
            )
        print(f"{num_records} mock reservation records sent to SQS.")

        return {'statusCode': 200, 'body': f'{num_records} mock reservation records sent to SQS.'}

    except Exception as e:
        print(f"Error sending messages to SQS: {str(e)}")
        
