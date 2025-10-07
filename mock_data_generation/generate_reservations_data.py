from datetime import datetime, timedelta
import random
import boto3

sqs_client = boto3.client('sqs')


def generate_mock_reservation_data(num_records):

    records = []
    for i in range(num_records):
        
        start_date_min = datetime(2025, 1, 1)
        start_date_max = datetime(2027, 12, 1)
        # Generate a random start date
        days_between = (start_date_max - start_date_min).days
        random_start_date = start_date_min + timedelta(days=random.randint(0, days_between))

        # Generate a random duration between 1 and 30 days
        duration_days = random.randint(1, 30)
        random_end_date = random_start_date + timedelta(days=duration_days)

        # Converto dates to string format YYYY-MM-DD
        start_date_str = random_start_date.strftime("%Y-%m-%d")
        end_date_str = random_end_date.strftime("%Y-%m-%d")

        record = {
            "bookingId": f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{i}",
            "userId": random.randint(1, 1000),
            "propertyId": f"property-{i%200}", 
            "location": f"City-{i%5}, Country-{i%3}",  
            "startDate": start_date_str,
            "endDate": end_date_str,
            "reservationDurationDays": duration_days,
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
        
