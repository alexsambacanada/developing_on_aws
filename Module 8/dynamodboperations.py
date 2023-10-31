import boto3

dynamodb = boto3.resource('dynamodb')

table_name = 'myawesometable'


# create a table
def create_dynamodb_table(table_name):
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'},  # Partition key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'N'},
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5,
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create table '{table_name}': {str(e)}")

def put_items_in_table(table_name):
    try:
        table = dynamodb.Table(table_name)
        for i in range(500):
            table.put_item(
                Item={
                    'id': i,
                    'data': f'data_{i}'
                }
            )
        print(f"Put 500 items into '{table_name}' successfully.")
    except Exception as e:
        print(f"Failed to put items into '{table_name}': {str(e)}")
        

def update_item_in_table(table_name, item_id, new_data):
    try:
        table = dynamodb.Table(table_name)
        response = table.update_item(
            Key={'id': item_id},
            UpdateExpression='set #d = :d',
            ExpressionAttributeNames={'#d': 'data'},
            ExpressionAttributeValues={':d': new_data},
            ConditionExpression='attribute_exists(id)'
        )
        print(f"Item '{item_id}' updated successfully.")
    except Exception as e:
        print(f"Failed to update item '{item_id}': {str(e)}")

def delete_item_from_table(table_name, item_id):
    try:
        table = dynamodb.Table(table_name)
        table.delete_item(Key={'id': item_id})
        print(f"Item '{item_id}' deleted successfully.")
    except Exception as e:
        print(f"Failed to delete item '{item_id}': {str(e)}")

def delete_dynamodb_table(table_name):
    try:
        table = dynamodb.Table(table_name)
        table.delete()
        table.meta.client.get_waiter('table_not_exists').wait(TableName=table_name)
        print(f"Table '{table_name}' deleted successfully.")
    except Exception as e:
        print(f"Failed to delete table '{table_name}': {str(e)}")

# Create the DynamoDB table
create_dynamodb_table(table_name)

# Put 500 items into the table
put_items_in_table(table_name)

# Update an item in the table
update_item_in_table(table_name, 42, 'new_data')

# Delete an item from the table
delete_item_from_table(table_name, 10)

# Delete the DynamoDB table
# delete_dynamodb_table(table_name)




