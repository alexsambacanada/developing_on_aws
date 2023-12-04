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
        print(f"Creating table '{table_name}'...")
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create table '{table_name}': {str(e)}")

def put_items_in_table(table_name):
    print(f"Putting some items into '{table_name}'...")
    try:
        table = dynamodb.Table(table_name)
        for i in range(500):
            print(f'Putting item {i} in table.')
            table.put_item(
                Item={
                    'id': i,
                    'data': f'data_{i}'
                }
            )
        print(f"Put 500 items into '{table_name}' successfully.")
    except Exception as e:
        print(f"Failed to put items into '{table_name}': {str(e)}")
        
def get_item(table_name, item_id):
    table = dynamodb.Table(table_name)
    
    try:
        print(f"Getting item {item_id}.")
        response = table.get_item(Key=item_id)
    
        # Check if the item was found
        if 'Item' in response:
            item = response['Item']
            print("Item found:")
            print(item)
        else:
            print("Item not found.")
    except Exception as e:
        print(f"Failed to retrieve the item from DynamoDB: {str(e)}")
        

def update_item_in_table(table_name, item_id, new_data):
    
    # item_id = 42
    # new_data = "this is some awesome new data!!"
    
    try:
        table = dynamodb.Table(table_name)
        response = table.update_item(
            Key={'id': item_id},
            ExpressionAttributeNames={'#d': 'data'},
            ExpressionAttributeValues={':d': new_data},
            UpdateExpression='set #d = :d',
            ConditionExpression='attribute_exists(id)',
            ReturnValues='ALL_NEW'
        )
        print(f"Item '{item_id}' updated successfully.")
        print(response['Attributes'])
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
        print(f"Emptying and deleting '{table_name}'...")
        table = dynamodb.Table(table_name)
        table.delete()
        table.meta.client.get_waiter('table_not_exists').wait(TableName=table_name)
        print(f"Table '{table_name}' deleted successfully.")
    except Exception as e:
        print(f"Failed to delete table '{table_name}': {str(e)}")

# Create the DynamoDB table
create_dynamodb_table(table_name)
input("Press Enter to continue...")

# Put 500 items into the table
put_items_in_table(table_name)
input("Press Enter to continue...")

# Delete an item from the table
get_item(table_name, {'id': 42})
input("Press Enter to continue...")

# Update an item in the table
update_item_in_table(table_name, 42, 'this is some awesome new data!!!')
input("Press Enter to continue...")

delete_item_from_table(table_name, 42)
input("Press Enter to continue...")

# Delete the DynamoDB table
delete_dynamodb_table(table_name)
input("Press Enter to continue...")




