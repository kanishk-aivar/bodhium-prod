#!/usr/bin/env python3
"""
Script to create the oauth_users DynamoDB table for Google OAuth authentication.

This script creates a DynamoDB table with the following structure:
- Primary key: email (String)
- Attributes: id, name, image, createdAt, updatedAt

Usage:
    python create_oauth_users_table.py

Environment variables required:
    AWS_REGION (default: us-east-1)
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
"""

import boto3
import os
import sys
from botocore.exceptions import ClientError
from datetime import datetime

def create_oauth_users_table():
    """Create the oauth_users DynamoDB table."""
    
    # Get AWS region from environment variable
    region = os.getenv('AWS_REGION', 'us-east-1')
    
    try:
        # Create DynamoDB client
        dynamodb = boto3.client('dynamodb', region_name=region)
        
        table_name = 'oauth_users'
        
        # Check if table already exists
        try:
            response = dynamodb.describe_table(TableName=table_name)
            print(f"Table '{table_name}' already exists.")
            print(f"Table status: {response['Table']['TableStatus']}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise e
        
        # Create table
        print(f"Creating table '{table_name}'...")
        
        table_definition = {
            'TableName': table_name,
            'KeySchema': [
                {
                    'AttributeName': 'email',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            'AttributeDefinitions': [
                {
                    'AttributeName': 'email',
                    'AttributeType': 'S'  # String
                }
            ],
            'BillingMode': 'PAY_PER_REQUEST',  # On-demand billing
            'Tags': [
                {
                    'Key': 'Purpose',
                    'Value': 'OAuth Authentication'
                },
                {
                    'Key': 'CreatedBy',
                    'Value': 'Migration Script'
                },
                {
                    'Key': 'CreatedAt',
                    'Value': datetime.now().isoformat()
                }
            ]
        }
        
        response = dynamodb.create_table(**table_definition)
        
        print(f"Table '{table_name}' creation initiated.")
        print(f"Table ARN: {response['TableDescription']['TableArn']}")
        
        # Wait for table to be created
        print("Waiting for table to be created...")
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        
        print(f"✅ Table '{table_name}' created successfully!")
        
        # Get final table info
        response = dynamodb.describe_table(TableName=table_name)
        table_info = response['Table']
        
        print("\n📋 Table Information:")
        print(f"  Table Name: {table_info['TableName']}")
        print(f"  Table Status: {table_info['TableStatus']}")
        print(f"  Table ARN: {table_info['TableArn']}")
        print(f"  Item Count: {table_info.get('ItemCount', 0)}")
        print(f"  Billing Mode: {table_info['BillingModeSummary']['BillingMode']}")
        
        print("\n🔑 Key Schema:")
        for key in table_info['KeySchema']:
            print(f"  {key['AttributeName']}: {key['KeyType']}")
        
        print("\n📝 Attributes:")
        for attr in table_info['AttributeDefinitions']:
            print(f"  {attr['AttributeName']}: {attr['AttributeType']}")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"❌ Error creating table: {error_code} - {error_message}")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def main():
    """Main function."""
    print("🚀 DynamoDB OAuth Users Table Creation Script")
    print("=" * 50)
    
    # Check AWS credentials
    try:
        session = boto3.Session()
        credentials = session.get_credentials()
        if not credentials:
            print("❌ AWS credentials not found. Please configure your AWS credentials.")
            print("   You can use AWS CLI: aws configure")
            print("   Or set environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error checking AWS credentials: {str(e)}")
        sys.exit(1)
    
    # Create the table
    success = create_oauth_users_table()
    
    if success:
        print("\n🎉 Migration completed successfully!")
        print("\n📋 Next steps:")
        print("1. Set up Google OAuth credentials in your environment:")
        print("   - GOOGLE_CLIENT_ID")
        print("   - GOOGLE_CLIENT_SECRET")
        print("   - NEXTAUTH_SECRET")
        print("2. Update your Google Cloud Console OAuth settings")
        print("3. Test the OAuth login flow")
    else:
        print("\n❌ Migration failed. Please check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
