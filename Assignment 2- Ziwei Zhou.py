# setup-the resource

import boto3
import time
from boto3 import dynamodb

bucket_name = f"testbucket-{int(time.time())}"  # This adds a timestamp to make it unique

s3 = boto3.client('s3')

def create_s3_bucket():
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"S3 bucket '{bucket_name}' created.")
    except Exception as e:
        print(f"Error creating bucket: {e}")

create_s3_bucket()

table_name = 'S3-object-size-history01'

def create_dynamodb_table():
    try:
        existing_tables = dynamodb.tables.all()
        if table_name not in [table.name for table in existing_tables]:
            table = dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'BucketName', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'Timestamp', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'BucketName', 'AttributeType': 'S'},
                    {'AttributeName': 'Timestamp', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            table.wait_until_exists()
            print(f"DynamoDB table '{table_name}' created.")
        else:
            print(f"Table '{table_name}' already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")


# Lambda function: size-tracking
import boto3
import time

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    bucket_name = 'testbucket-1729236050'
    table_name = 'S3-object-size-history'
    table = dynamodb.Table(table_name)

    # List objects in the S3 bucket
    response = s3.list_objects_v2(Bucket=bucket_name)
    total_size = 0
    object_count = 0

    if 'Contents' in response:
        object_count = len(response['Contents'])
        total_size = sum(obj['Size'] for obj in response['Contents'])

    # Store the result in DynamoDB
    timestamp = str(time.time())
    table.put_item(
        Item={
            'BucketName': bucket_name,
            'Timestamp': timestamp,
            'Size': total_size,
            'ObjectCount': object_count
        }
    )

    return {
        'statusCode': 200,
        'body': f"Updated size info for bucket {bucket_name}."
    }


# Lambda function: plotting
import boto3
import matplotlib.pyplot as plt
import time

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    bucket_name = 'testbucket-1729236050t'
    table = dynamodb.Table('S3-object-size-history')

    # Query DynamoDB for the last 10 seconds
    current_time = time.time()
    start_time = str(current_time - 10)

    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('BucketName').eq(bucket_name) &
                               boto3.dynamodb.conditions.Key('Timestamp').between(start_time, str(current_time))
    )

    items = response['Items']
    timestamps = [float(item['Timestamp']) for item in items]
    sizes = [item['Size'] for item in items]

    # Log the returned items from DynamoDB
    print(f"Items retrieved from DynamoDB: {items}")

    # Check if there is any data to plot
    if len(sizes) == 0 or len(timestamps) == 0:
        print("No data found for plotting.")
        return {
            'statusCode': 400,
            'body': 'No data found for plotting.'
        }

    # Generate the plot
    plt.plot(timestamps, sizes, label='Bucket Size')
    plt.axhline(max(sizes), color='red', linestyle='--', label='Max Size')
    plt.xlabel('Time (s)')
    plt.ylabel('Size (bytes)')
    plt.legend()
    plt.savefig('/tmp/plot.png')

    # Upload the plot to S3
    with open('/tmp/plot.png', 'rb') as file:
        s3.put_object(Bucket=bucket_name, Key='plot', Body=file)

    return {
        'statusCode': 200,
        'body': 'Plot generated and uploaded to S3.'
    }


# Lambda function: driver
import boto3
import time
import urllib3

# Initialize AWS clients
s3 = boto3.client('s3')
http = urllib3.PoolManager()


def lambda_handler(event, context):
    bucket_name = 'testbucket-1729236050'

    # Step 1: Create a new object in the S3 bucket
    s3.put_object(Bucket=bucket_name, Key='assignment1.txt', Body='Empty Assignment 1')
    print("Created assignment1.txt in S3 bucket.")
    time.sleep(5)  # Wait for 5 seconds to simulate time between operations

    # Step 2: Update the object with new content
    s3.put_object(Bucket=bucket_name, Key='assignment1.txt', Body='Empty Assignment 2222222222')
    print("Updated assignment1.txt in S3 bucket.")
    time.sleep(5)  # Wait for 5 seconds

    # Step 3: Delete the object from the bucket
    s3.delete_object(Bucket=bucket_name, Key='assignment1.txt')
    print("Deleted assignment1.txt from S3 bucket.")
    time.sleep(5)  # Wait for 5 seconds

    # Step 4: Create another object in the bucket
    s3.put_object(Bucket=bucket_name, Key='assignment2.txt', Body='33')
    print("Created assignment2.txt in S3 bucket.")
    time.sleep(5)  # Wait for 5 seconds

    # Step 5: Call the Plotting Lambda via API Gateway
    plotting_lambda_api_url = 'https://mbrls81nt8.execute-api.us-east-1.amazonaws.com/default/plotting'
    response = http.request('GET', plotting_lambda_api_url)

    # Log the response from the Plotting Lambda
    print(f"Plotting Lambda API response status: {response.status}")
    print(f"Plotting Lambda API response body: {response.data.decode('utf-8')}")

    # Return success message
    return {
        'statusCode': response.status,
        'body': response.data.decode('utf-8')
    }





