import argparse
import boto3
import json
import requests


def create_parser():
    parser = argparse.ArgumentParser(description='Create/Insert data into DynamoDB')
    parser.add_argument('-operation', required=True)
    parser.add_argument('-region', required=False)
    return parser


def connect_to_dynamo(region):
    return boto3.client('dynamodb', region_name=region)



def create_dynamo_db_tables(region):
    dynamodb_conn = connect_to_dynamo(region)

    # Create ProductCatalog DynamoDB Table
    response = dynamodb_conn.create_table(AttributeDefinitions=[
                                          {
                                            'AttributeName': 'Id',
                                            'AttributeType': 'N'
                                          },
                                          ],
                                          TableName='ProductCatalog',
                                          KeySchema=[
                                              {
                                                  'AttributeName': 'Id',
                                                  'KeyType': 'HASH'
                                              },
                                          ],
                                          ProvisionedThroughput = {
                                                  'ReadCapacityUnits': 5,
                                                  'WriteCapacityUnits': 5
                                                })

    # Create Forum DynamoDB Table
    response = dynamodb_conn.create_table(AttributeDefinitions=[
                                            {
                                                'AttributeName': 'Name',
                                                'AttributeType': 'S'
                                            },
                                            ],
                                          TableName='Forum',
                                          KeySchema=[
                                            {
                                                'AttributeName': 'Name',
                                                'KeyType': 'HASH'
                                            },
                                            ],
                                          ProvisionedThroughput={
                                            'ReadCapacityUnits': 5,
                                            'WriteCapacityUnits': 5
                                            })


    # Create Thread DynamoDB Table
    response = dynamodb_conn.create_table(AttributeDefinitions=[
                                            {
                                            'AttributeName': 'ForumName',
                                            'AttributeType': 'S'
                                            },
                                            {
                                            'AttributeName': 'Subject',
                                            'AttributeType': 'S'
                                            }
                                            ],
                                         TableName='Thread',
                                         KeySchema=[
                                            {
                                            'AttributeName': 'ForumName',
                                            'KeyType': 'HASH'
                                            },
                                            {
                                            'AttributeName' : 'Subject',
                                            'KeyType': 'RANGE'
                                            }
                                            ],
                                        ProvisionedThroughput={
                                        'ReadCapacityUnits': 5,
                                        'WriteCapacityUnits': 5
                                            })

    # Create Reply DynamoDB Table
    response = dynamodb_conn.create_table(AttributeDefinitions=[
                                          {
                                            'AttributeName': 'Id',
                                            'AttributeType': 'S'
                                          },
                                          {
                                            'AttributeName': 'ReplyDateTime',
                                            'AttributeType': 'S'
                                          },
                                          {
                                            'AttributeName': 'PostedBy',
                                            'AttributeType': 'S'
                                          }
                                          ],
                                          TableName='Reply',
                                          KeySchema=[
                                          {
                                            'AttributeName': 'Id',
                                            'KeyType': 'HASH',
                                          },
                                          {
                                            'AttributeName': 'ReplyDateTime',
                                            'KeyType': 'RANGE'
                                          }
                                          ],
                                          LocalSecondaryIndexes=[
                                          {
                                            'IndexName': 'PostedBy-Index',
                                            'KeySchema': [
                                                {
                                                    'AttributeName': 'Id',
                                                    'KeyType': 'HASH'
                                                },
                                                {
                                                    'AttributeName': 'PostedBy',
                                                    'KeyType': 'RANGE'
                                                }
                                                ],
                                            'Projection': {
                                                'ProjectionType': 'KEYS_ONLY'
                                                }
                                          },
                                          ],
                                          ProvisionedThroughput={
                                            'ReadCapacityUnits': 5,
                                            'WriteCapacityUnits': 5
                                          })


def upload_data_to_dynamo_db_tables(region):
    dynamodb_conn = connect_to_dynamo(region)

    # Add data to ProductCatalog DynamoDB Table
    dynamodb_conn.batch_write_item(RequestItems={
                                    "ProductCatalog": [
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "N": "101"
                                                    },
                                                    "Title": {
                                                        "S": "Book 101 Title"
                                                    },
                                                    "ISBN": {
                                                        "S": "111-1111111111"
                                                    },
                                                    "Authors": {
                                                        "L": [
                                                            {
                                                                "S": "Author1"
                                                            }
                                                        ]
                                                    },
                                                    "Price": {
                                                        "N": "2"
                                                    },
                                                    "Dimensions": {
                                                        "S": "8.5 x 11.0 x 0.5"
                                                    },
                                                    "PageCount": {
                                                        "N": "500"
                                                    },
                                                    "InPublication": {
                                                        "BOOL": True
                                                    },
                                                    "ProductCategory": {
                                                        "S": "Book"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "N": "102"
                                                    },
                                                    "Title": {
                                                        "S": "Book 102 Title"
                                                    },
                                                    "ISBN": {
                                                        "S": "222-2222222222"
                                                    },
                                                    "Authors": {
                                                        "L": [
                                                            {
                                                                "S": "Author1"
                                                            },
                                                            {
                                                                "S": "Author2"
                                                            }
                                                        ]
                                                    },
                                                    "Price": {
                                                        "N": "20"
                                                    },
                                                    "Dimensions": {
                                                        "S": "8.5 x 11.0 x 0.8"
                                                    },
                                                    "PageCount": {
                                                        "N": "600"
                                                    },
                                                    "InPublication": {
                                                        "BOOL": True
                                                    },
                                                    "ProductCategory": {
                                                        "S": "Book"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "N": "103"
                                                    },
                                                    "Title": {
                                                        "S": "Book 103 Title"
                                                    },
                                                    "ISBN": {
                                                        "S": "333-3333333333"
                                                    },
                                                    "Authors": {
                                                        "L": [
                                                            {
                                                                "S": "Author1"
                                                            },
                                                            {
                                                                "S": "Author2"
                                                            }
                                                        ]
                                                    },
                                                    "Price": {
                                                        "N": "2000"
                                                    },
                                                    "Dimensions": {
                                                        "S": "8.5 x 11.0 x 1.5"
                                                    },
                                                    "PageCount": {
                                                        "N": "600"
                                                    },
                                                    "InPublication": {
                                                        "BOOL": False
                                                    },
                                                    "ProductCategory": {
                                                        "S": "Book"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "N": "201"
                                                    },
                                                    "Title": {
                                                        "S": "18-Bike-201"
                                                    },
                                                    "Description": {
                                                        "S": "201 Description"
                                                    },
                                                    "BicycleType": {
                                                        "S": "Road"
                                                    },
                                                    "Brand": {
                                                        "S": "Mountain A"
                                                    },
                                                    "Price": {
                                                        "N": "100"
                                                    },
                                                    "Color": {
                                                        "L": [
                                                            {
                                                                "S": "Red"
                                                            },
                                                            {
                                                                "S": "Black"
                                                            }
                                                        ]
                                                    },
                                                    "ProductCategory": {
                                                        "S": "Bicycle"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "N": "202"
                                                    },
                                                    "Title": {
                                                        "S": "21-Bike-202"
                                                    },
                                                    "Description": {
                                                        "S": "202 Description"
                                                    },
                                                    "BicycleType": {
                                                        "S": "Road"
                                                    },
                                                    "Brand": {
                                                        "S": "Brand-Company A"
                                                    },
                                                    "Price": {
                                                        "N": "200"
                                                    },
                                                    "Color": {
                                                        "L": [
                                                            {
                                                                "S": "Green"
                                                            },
                                                            {
                                                                "S": "Black"
                                                            }
                                                        ]
                                                    },
                                                    "ProductCategory": {
                                                        "S": "Bicycle"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "N": "203"
                                                    },
                                                    "Title": {
                                                        "S": "19-Bike-203"
                                                    },
                                                    "Description": {
                                                        "S": "203 Description"
                                                    },
                                                    "BicycleType": {
                                                        "S": "Road"
                                                    },
                                                    "Brand": {
                                                        "S": "Brand-Company B"
                                                    },
                                                    "Price": {
                                                        "N": "300"
                                                    },
                                                    "Color": {
                                                        "L": [
                                                            {
                                                                "S": "Red"
                                                            },
                                                            {
                                                                "S": "Green"
                                                            },
                                                            {
                                                                "S": "Black"
                                                            }
                                                        ]
                                                    },
                                                    "ProductCategory": {
                                                        "S": "Bicycle"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "N": "204"
                                                    },
                                                    "Title": {
                                                        "S": "18-Bike-204"
                                                    },
                                                    "Description": {
                                                        "S": "204 Description"
                                                    },
                                                    "BicycleType": {
                                                        "S": "Mountain"
                                                    },
                                                    "Brand": {
                                                        "S": "Brand-Company B"
                                                    },
                                                    "Price": {
                                                        "N": "400"
                                                    },
                                                    "Color": {
                                                        "L": [
                                                            {
                                                                "S": "Red"
                                                            }
                                                        ]
                                                    },
                                                    "ProductCategory": {
                                                        "S": "Bicycle"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "N": "205"
                                                    },
                                                    "Title": {
                                                        "S": "18-Bike-204"
                                                    },
                                                    "Description": {
                                                        "S": "205 Description"
                                                    },
                                                    "BicycleType": {
                                                        "S": "Hybrid"
                                                    },
                                                    "Brand": {
                                                        "S": "Brand-Company C"
                                                    },
                                                    "Price": {
                                                        "N": "500"
                                                    },
                                                    "Color": {
                                                        "L": [
                                                            {
                                                                "S": "Red"
                                                            },
                                                            {
                                                                "S": "Black"
                                                            }
                                                        ]
                                                    },
                                                    "ProductCategory": {
                                                        "S": "Bicycle"
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                })

    # Add data to Forum DynamoDB Table
    dynamodb_conn.batch_write_item(RequestItems={
                                    "Forum": [
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Name": {"S":"Amazon DynamoDB"},
                                                    "Category": {"S":"Amazon Web Services"},
                                                    "Threads": {"N":"2"},
                                                    "Messages": {"N":"4"},
                                                    "Views": {"N":"1000"}
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Name": {"S":"Amazon S3"},
                                                    "Category": {"S":"Amazon Web Services"}
                                                }
                                            }
                                        }
                                    ]
                                })

    # Add data to Thread DynamoDB Table
    dynamodb_conn.batch_write_item(RequestItems={
                                    "Thread": [
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "ForumName": {
                                                        "S": "Amazon DynamoDB"
                                                    },
                                                    "Subject": {
                                                        "S": "DynamoDB Thread 1"
                                                    },
                                                    "Message": {
                                                        "S": "DynamoDB thread 1 message"
                                                    },
                                                    "LastPostedBy": {
                                                        "S": "User A"
                                                    },
                                                    "LastPostedDateTime": {
                                                        "S": "2015-09-22T19:58:22.514Z"
                                                    },
                                                    "Views": {
                                                        "N": "0"
                                                    },
                                                    "Replies": {
                                                        "N": "0"
                                                    },
                                                    "Answered": {
                                                        "N": "0"
                                                    },
                                                    "Tags": {
                                                        "L": [
                                                            {
                                                                "S": "index"
                                                            },
                                                            {
                                                                "S": "primarykey"
                                                            },
                                                            {
                                                                "S": "table"
                                                            }
                                                        ]
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "ForumName": {
                                                        "S": "Amazon DynamoDB"
                                                    },
                                                    "Subject": {
                                                        "S": "DynamoDB Thread 2"
                                                    },
                                                    "Message": {
                                                        "S": "DynamoDB thread 2 message"
                                                    },
                                                    "LastPostedBy": {
                                                        "S": "User A"
                                                    },
                                                    "LastPostedDateTime": {
                                                        "S": "2015-09-15T19:58:22.514Z"
                                                    },
                                                    "Views": {
                                                        "N": "0"
                                                    },
                                                    "Replies": {
                                                        "N": "0"
                                                    },
                                                    "Answered": {
                                                        "N": "0"
                                                    },
                                                    "Tags": {
                                                        "L": [
                                                            {
                                                                "S": "items"
                                                            },
                                                            {
                                                                "S": "attributes"
                                                            },
                                                            {
                                                                "S": "throughput"
                                                            }
                                                        ]
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "ForumName": {
                                                        "S": "Amazon S3"
                                                    },
                                                    "Subject": {
                                                        "S": "S3 Thread 1"
                                                    },
                                                    "Message": {
                                                        "S": "S3 thread 1 message"
                                                    },
                                                    "LastPostedBy": {
                                                        "S": "User A"
                                                    },
                                                    "LastPostedDateTime": {
                                                        "S": "2015-09-29T19:58:22.514Z"
                                                    },
                                                    "Views": {
                                                        "N": "0"
                                                    },
                                                    "Replies": {
                                                        "N": "0"
                                                    },
                                                    "Answered": {
                                                        "N": "0"
                                                    },
                                                    "Tags": {
                                                        "L": [
                                                            {
                                                                "S": "largeobjects"
                                                            },
                                                            {
                                                                "S": "multipart upload"
                                                            }
                                                        ]
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                })

    # Add data to Reply DynamoDB Table
    dynamodb_conn.batch_write_item(RequestItems={
                                    "Reply": [
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "S": "Amazon DynamoDB#DynamoDB Thread 1"
                                                    },
                                                    "ReplyDateTime": {
                                                        "S": "2015-09-15T19:58:22.947Z"
                                                    },
                                                    "Message": {
                                                        "S": "DynamoDB Thread 1 Reply 1 text"
                                                    },
                                                    "PostedBy": {
                                                        "S": "User A"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "S": "Amazon DynamoDB#DynamoDB Thread 1"
                                                    },
                                                    "ReplyDateTime": {
                                                        "S": "2015-09-22T19:58:22.947Z"
                                                    },
                                                    "Message": {
                                                        "S": "DynamoDB Thread 1 Reply 2 text"
                                                    },
                                                    "PostedBy": {
                                                        "S": "User B"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "S": "Amazon DynamoDB#DynamoDB Thread 2"
                                                    },
                                                    "ReplyDateTime": {
                                                        "S": "2015-09-29T19:58:22.947Z"
                                                    },
                                                    "Message": {
                                                        "S": "DynamoDB Thread 2 Reply 1 text"
                                                    },
                                                    "PostedBy": {
                                                        "S": "User A"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "PutRequest": {
                                                "Item": {
                                                    "Id": {
                                                        "S": "Amazon DynamoDB#DynamoDB Thread 2"
                                                    },
                                                    "ReplyDateTime": {
                                                        "S": "2015-10-05T19:58:22.947Z"
                                                    },
                                                    "Message": {
                                                        "S": "DynamoDB Thread 2 Reply 2 text"
                                                    },
                                                    "PostedBy": {
                                                        "S": "User A"
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                })




def main(args):
            operation = args.operation
            region = args.region
            if not region:
                # Default to the region the EC2 instance has been deployed
                region = str(
                json.loads(requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document/').text).get(
                    'region'))
            if operation == 'create':
                create_dynamo_db_tables(region)
            elif operation == 'upload':
                upload_data_to_dynamo_db_tables(region)
            else:
                raise Exception('Unknown operation.Please choose "create" and "upload"')


if __name__ == '__main__':
        parser = create_parser()
        args = parser.parse_args()
        main(args)
