import json
import boto3
from datetime import datetime

#S3 Config
s3 = boto3.client('s3')
destination_bucket_name = 'airbnbn-long-term-reservations'
#SQS Config
sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/652734276321/AirbnbBookingQueue'

def lambda_handler(event, context):

    mini_batch = []

    # Receive messages from the SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,  
        WaitTimeSeconds=5       
    )

    # msg_number_respose = sqs.get_queue_attributes(
    # QueueUrl=queue_url,
    # AttributeNames=['ApproximateNumberOfMessages']
    # )

    # num_messages = int(msg_number_respose['Attributes']['ApproximateNumberOfMessages'])
    # print("Number of messages in the queue:", num_messages)

    messages = response.get('Messages', [])

    print("Total messages received in the batch : ",len(messages))

    for message in messages:

        try: 
            reservation = json.loads(message['Body'])

            # Append to mini_batch
            if reservation.get('reservationDurationDays', 0) == 1:
                print("Skipping short-term reservation of 1 day")
                pass
            else:
                mini_batch.append(reservation)
            # Delete message from the queue
            receipt_handle = message['ReceiptHandle']

            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            print("Message deleted from the queue")

        except Exception as e:
            print(f"Error processing message: {str(e)}")
            continue
        
    if mini_batch:

        try:
            # Serialize the data to JSON string
            json_data = json.dumps(mini_batch, indent=2)
            # Create timestamped key for S3
            now = datetime.now()
            s3_key = (
                f'long-term-reservations/year={now.year}/month={now.month:02}/'
                f'day={now.day:02}/hour={now.hour:02}/'
                f'reservation_data_{now.minute:02}_{now.second:02}.json'
            )
        
            # Upload the JSON to S3
            s3.put_object(
                Bucket=destination_bucket_name,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json'
            )

            print(f'Uploaded {len(mini_batch)} records to {s3_key}')

            return {
                'statusCode': 200,
                'body': json.dumps(f'Uploaded {len(mini_batch)} records to {s3_key}')
            }
        
        except Exception as e:
            print(f"Error uploading to S3: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error uploading to S3: {str(e)}')
            }
    else:
        print("No long-term reservations to process.")
        return {
            'statusCode': 200,
            'body': json.dumps('No long-term reservations to process.')
        }

# if __name__ == "__main__":
#     lambda_handler(None, None)
