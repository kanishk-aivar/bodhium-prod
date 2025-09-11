# Authentication Setup Guide

This guide explains how to set up authentication for the Bodhium Workflow app using NextAuth.js and DynamoDB.

## Prerequisites

1. **AWS Account and Credentials**
   - Configure AWS credentials using one of these methods:
     ```bash
     # Option 1: AWS CLI
     aws configure
     
     # Option 2: Environment variables
     export AWS_ACCESS_KEY_ID=your_access_key
     export AWS_SECRET_ACCESS_KEY=your_secret_key
     export AWS_REGION=us-east-1
     ```

2. **Python Dependencies** (for user creation script)
   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Variables**
   - Copy `.env.example` to `.env.local`
   - Update the values:
   ```bash
   cp .env.example .env.local
   ```
   ```env
   NEXTAUTH_URL=http://localhost:3000
   NEXTAUTH_SECRET=your-secret-key-here-change-this-in-production
   AWS_REGION=us-east-1
   AWS_ACCESS_KEY_ID=your-aws-access-key
   AWS_SECRET_ACCESS_KEY=your-aws-secret-key
   ```

## DynamoDB Table Setup

The authentication system requires a DynamoDB table called `users` with the following structure:

**Table Name:** `users`
**Primary Key:** `id` (String)
**Global Secondary Index:** `email-index` with `email` as partition key

### Automatic Setup (Recommended)

The Python user creation script will automatically create the table when you try to create your first user:

```bash
python create_user.py admin@example.com password123 "Admin User"
```

When the table doesn't exist, it will ask:
```
⚠️  DynamoDB table 'users' not found.
Would you like to create the 'users' table automatically? (y/n): y
```

### Manual Setup (Alternative)

If you prefer to create the table manually:

```bash
aws dynamodb create-table \
    --table-name users \
    --attribute-definitions \
        AttributeName=id,AttributeType=S \
        AttributeName=email,AttributeType=S \
    --key-schema AttributeName=id,KeyType=HASH \
    --global-secondary-indexes \
        'IndexName=email-index,KeySchema=[{AttributeName=email,KeyType=HASH}],Projection={ProjectionType=ALL},ProvisionedThroughput={ReadCapacityUnits=5,WriteCapacityUnits=5}' \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```

## User Management

### Creating Users

Use the Python script to create new users:

```bash
# Create user with email and password
python create_user.py user@example.com password123

# Create user with email, password, and display name
python create_user.py user@example.com password123 "John Doe"
```

### User Login

1. Start the Next.js development server:
   ```bash
   npm run dev
   ```

2. Navigate to `http://localhost:3000`

3. You'll be automatically redirected to `/login` if not authenticated

4. Use the email and password you created with the Python script

## How Authentication Works

1. **Middleware Protection**: All routes except `/login` and API auth routes are protected by middleware
2. **Login Flow**: Users authenticate via the `/login` page using email/password
3. **Session Management**: NextAuth.js handles sessions with JWT tokens
4. **Database Integration**: User credentials are stored in DynamoDB with bcrypt password hashing
5. **Auto-redirect**: Authenticated users trying to access `/login` are redirected to home page

## Security Features

- ✅ Password hashing with bcrypt
- ✅ JWT session tokens
- ✅ Middleware-based route protection
- ✅ Secure credential validation
- ✅ Auto-redirect logic for authenticated users

## Troubleshooting

### "Table not found" error
- Ensure AWS credentials are configured correctly
- Run the Python script which will offer to create the table automatically

### "Authentication failed" error
- Verify the user exists in DynamoDB
- Check password is correct (passwords are case-sensitive)

### Can't access protected routes
- Make sure you're logged in
- Check browser console for any JavaScript errors
- Verify NextAuth configuration in `.env.local`

## Production Considerations

1. **Change NEXTAUTH_SECRET**: Generate a secure random string
2. **Use IAM Roles**: Instead of hardcoded AWS credentials on EC2/Lambda
3. **Enable HTTPS**: Update NEXTAUTH_URL to use HTTPS
4. **Monitor Costs**: DynamoDB provisioned capacity will incur costs
5. **Backup Strategy**: Enable point-in-time recovery for DynamoDB table
