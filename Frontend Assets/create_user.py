#!/usr/bin/env python3
"""
User Creation Script for Bodhium Workflow App

This script creates new users in the DynamoDB users table with hashed passwords.
Usage: python create_user.py <email> <password> [name]
"""

import sys
import argparse
import boto3
import bcrypt
import uuid
import time
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError

# Configure DynamoDB client
def get_dynamodb_client():
    """Get DynamoDB client with proper error handling"""
    try:
        # Try to create client with default credentials (from environment, IAM role, etc.)
        session = boto3.Session()
        dynamodb = session.resource('dynamodb', region_name='us-east-1')
        return dynamodb
    except NoCredentialsError:
        print("Error: AWS credentials not found.")
        print("Please configure your AWS credentials using one of these methods:")
        print("1. AWS CLI: aws configure")
        print("2. Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
        print("3. IAM role (if running on EC2)")
        sys.exit(1)

def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    # Generate salt and hash password
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')

def check_table_exists(dynamodb, table_name: str) -> bool:
    """Check if DynamoDB table exists"""
    try:
        table = dynamodb.Table(table_name)
        table.load()
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            return False
        print(f"Error checking table existence: {e}")
        return False

def create_users_table(dynamodb, table_name: str = 'users'):
    """Create the users table with proper schema"""
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'email',
                    'AttributeType': 'S'
                }
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'email-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'email',
                            'KeyType': 'HASH'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        
        print(f"🔧 Creating table '{table_name}'...")
        
        # Wait for table to be created
        table.wait_until_exists()
        
        print(f"✅ Table '{table_name}' created successfully!")
        return True
        
    except ClientError as e:
        print(f"❌ Error creating table: {e}")
        return False

def check_user_exists(table, email: str) -> bool:
    """Check if user with given email already exists"""
    try:
        response = table.query(
            IndexName='email-index',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={':email': email}
        )
        return len(response['Items']) > 0
    except ClientError as e:
        print(f"Error checking if user exists: {e}")
        return False

def create_user(email: str, password: str, name: str = None):
    """Create a new user in the DynamoDB users table"""
    
    # Get DynamoDB client
    dynamodb = get_dynamodb_client()
    table_name = 'users'
    
    # Check if table exists
    if not check_table_exists(dynamodb, table_name):
        print(f"⚠️  DynamoDB table '{table_name}' not found.")
        
        # Ask user if they want to create the table
        while True:
            response = input(f"Would you like to create the '{table_name}' table automatically? (y/n): ").strip().lower()
            if response in ['y', 'yes']:
                if not create_users_table(dynamodb, table_name):
                    print("Failed to create table. Exiting.")
                    sys.exit(1)
                break
            elif response in ['n', 'no']:
                print("Table creation cancelled.")
                print(f"Please create the '{table_name}' table manually using the AWS CLI:")
                print()
                print("aws dynamodb create-table \\")
                print("    --table-name users \\")
                print("    --attribute-definitions \\")
                print("        AttributeName=id,AttributeType=S \\")
                print("        AttributeName=email,AttributeType=S \\")
                print("    --key-schema AttributeName=id,KeyType=HASH \\")
                print("    --global-secondary-indexes \\")
                print("        'IndexName=email-index,KeySchema=[{AttributeName=email,KeyType=HASH}],Projection={ProjectionType=ALL},ProvisionedThroughput={ReadCapacityUnits=5,WriteCapacityUnits=5}' \\")
                print("    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5")
                sys.exit(1)
            else:
                print("Please answer 'y' or 'n'.")
    
    # Get table reference
    table = dynamodb.Table(table_name)
    
    # Check if user already exists
    if check_user_exists(table, email):
        print(f"Error: User with email '{email}' already exists.")
        sys.exit(1)
    
    # Generate user ID and hash password
    user_id = str(uuid.uuid4())
    hashed_password = hash_password(password)
    current_time = datetime.utcnow().isoformat() + 'Z'
    
    # Prepare user data
    user_data = {
        'id': user_id,
        'email': email,
        'password': hashed_password,
        'createdAt': current_time,
        'updatedAt': current_time
    }
    
    # Add name if provided
    if name:
        user_data['name'] = name
    
    try:
        # Insert user into DynamoDB
        table.put_item(Item=user_data)
        print(f"✅ User created successfully!")
        print(f"   ID: {user_id}")
        print(f"   Email: {email}")
        if name:
            print(f"   Name: {name}")
        print(f"   Created: {current_time}")
        
    except ClientError as e:
        print(f"Error creating user: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description='Create a new user for the Bodhium Workflow app',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python create_user.py user@example.com mypassword123
  python create_user.py user@example.com mypassword123 "John Doe"
  
Prerequisites:
  1. AWS credentials configured (aws configure or environment variables)
  2. DynamoDB table 'users' will be created automatically if it doesn't exist
        """
    )
    
    parser.add_argument('email', help='User email address')
    parser.add_argument('password', help='User password (will be hashed)')
    parser.add_argument('name', nargs='?', help='User display name (optional)')
    
    args = parser.parse_args()
    
    # Validate email format (basic validation)
    if '@' not in args.email or '.' not in args.email:
        print("Error: Please provide a valid email address.")
        sys.exit(1)
    
    # Validate password strength
    if len(args.password) < 8:
        print("Error: Password must be at least 8 characters long.")
        sys.exit(1)
    
    # Create the user
    create_user(args.email, args.password, args.name)

if __name__ == '__main__':
    main()
