# Orchestrator Trigger API Documentation

## Overview

The Bodhium backend provides two orchestrator trigger endpoints for managing LLM task orchestration:

1. **Main Orchestrator Trigger** - For production workflows with retry support
2. **Adhoc Orchestrator Trigger** - For testing and ad-hoc queries with session tracking

Both endpoints trigger AWS Batch jobs that orchestrate multiple LLM workers (AIM, ChatGPT, AIO, Perplexity) to process queries in parallel.

## Base URL

```
https://your-api-gateway-url.amazonaws.com/prod
```

## Authentication

All endpoints require proper authentication. Include your authentication headers as required by your API Gateway configuration.

---

## 1. Main Orchestrator Trigger

### Endpoint
```
POST /orchestrator-trigger
```

### Description
Triggers the main orchestrator for production workflows. Supports both new requests and retry operations with session-based task grouping and retry mechanisms.

### Features
- ✅ **Session ID Generation** - Automatically generates unique session IDs for new requests
- ✅ **Retry Support** - Handles retry requests with existing session IDs
- ✅ **Task ID Management** - Generates unique task IDs for each worker
- ✅ **Bulk Operations** - Processes multiple products and queries simultaneously
- ✅ **Enhanced Logging** - Comprehensive orchestration event tracking

### Request Headers
```http
Content-Type: application/json
Authorization: Bearer <your-token>
```

### Request Body Schema

#### New Request (Normal Flow)
```json
{
  "job_id": "uuid",
  "selected_queries": [
    {
      "product_id": "123",
      "existing_queries": [
        {
          "query_id": 1,
          "query_text": "What are the key features of this product?"
        }
      ],
      "new_queries": [
        "How does this compare to competitors?",
        "What are the pricing options?"
      ]
    }
  ],
  "options": {}
}
```

#### Retry Request
```json
{
  "job_id": "uuid",
  "options": {
    "retry": true,
    "session_id": "uuid",
    "retry_tasks": [
      {
        "task_id": "uuid-1",
        "model": "chatgpt",
        "query_id": 123,
        "product_id": 456
      },
      {
        "task_id": "uuid-2", 
        "model": "aim",
        "query_id": 124,
        "product_id": 456
      }
    ]
  }
}
```

#### Legacy Retry Request (Deprecated)
```json
{
  "job_id": "uuid",
  "selected_queries": [
    {
      "product_id": "123",
      "existing_queries": [
        {
          "query_id": 1,
          "query_text": "What are the key features of this product?"
        }
      ]
    }
  ],
  "options": {
    "retry": true,
    "session_id": "existing-session-uuid"
  }
}
```

### Request Body Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `job_id` | `string` | ✅ | Unique identifier for the orchestration job |
| `selected_queries` | `array` | ✅ | Array of product groups with their queries |
| `options` | `object` | ❌ | Optional configuration including retry settings |

#### selected_queries Structure
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `product_id` | `string` | ✅ | Product identifier |
| `existing_queries` | `array` | ❌ | Array of existing queries with query_id |
| `new_queries` | `array` | ❌ | Array of new query strings |

#### options Structure
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `retry` | `boolean` | ❌ | Set to `true` for retry requests |
| `session_id` | `string` | ❌ | Required when `retry: true` |
| `retry_tasks` | `array` | ❌ | Array of specific tasks to retry with their models (for selective retries) |

#### retry_tasks Structure
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `task_id` | `string` | ✅ | UUID of the task to retry |
| `model` | `string` | ✅ | Specific model to use for retry (chatgpt, aio, aim, perplexity) |
| `query_id` | `integer` | ✅ | ID of the query to retry |
| `product_id` | `integer` | ✅ | ID of the product associated with the query |

### Response

#### Success Response (202 Accepted)
```json
{
  "status": "accepted",
  "message": "Batch job submitted successfully",
  "job_id": "uuid",
  "batch_job_id": "batch-job-uuid",
  "batch_job_name": "llm-orchestrator-20240115_143022-uuid",
  "selected_queries_count": 1,
  "mode": "Batch_trigger",
  "is_retry": false,
  "session_id": "new-session-uuid",
  "polling_info": {
    "batch_job_id": "batch-job-uuid",
    "job_queue": "your-batch-queue",
    "query_example": "aws batch describe-jobs --jobs batch-job-uuid"
  }
}
```

#### Error Response (400 Bad Request)
```json
{
  "error": "Invalid retry request: session_id not found or no failed tasks"
}
```

### Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `status` | `string` | Status of the request ("accepted") |
| `job_id` | `string` | The job ID provided in the request |
| `batch_job_id` | `string` | AWS Batch job identifier |
| `session_id` | `string` | Generated or reused session ID |
| `is_retry` | `boolean` | Whether this was a retry request |
| `polling_info` | `object` | Information for monitoring the batch job |

### Example Usage

#### cURL - New Request
```bash
curl -X POST https://your-api-gateway-url.amazonaws.com/prod/orchestrator-trigger \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token" \
  -d '{
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "selected_queries": [
      {
        "product_id": "123",
        "new_queries": [
          "What are the key features?",
          "How does it compare to competitors?"
        ]
      }
    ],
    "options": {}
  }'
```

#### cURL - Selective Retry Request (Recommended)
```bash
curl -X POST https://your-api-gateway-url.amazonaws.com/prod/orchestrator-trigger \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token" \
  -d '{
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "options": {
      "retry": true,
      "session_id": "existing-session-uuid",
      "retry_tasks": [
        {
          "task_id": "550e8400-e29b-41d4-a716-446655440001",
          "model": "chatgpt",
          "query_id": 1,
          "product_id": 123
        },
        {
          "task_id": "550e8400-e29b-41d4-a716-446655440002",
          "model": "aim",
          "query_id": 2,
          "product_id": 123
        }
      ]
    }
  }'
```

#### cURL - Legacy Retry Request (Deprecated)
```bash
curl -X POST https://your-api-gateway-url.amazonaws.com/prod/orchestrator-trigger \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token" \
  -d '{
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "selected_queries": [
      {
        "product_id": "123",
        "existing_queries": [
          {
            "query_id": 1,
            "query_text": "What are the key features?"
          }
        ]
      }
    ],
    "options": {
      "retry": true,
      "session_id": "existing-session-uuid"
    }
  }'
```

---

## 2. Adhoc Orchestrator Trigger

### Endpoint
```
POST /adhoc-orchestrator-trigger
```

### Description
Triggers the adhoc orchestrator for testing and ad-hoc queries. Generates session IDs for task grouping but does not support retry operations.

### Features
- ✅ **Session ID Generation** - Automatically generates unique session IDs
- ✅ **Task ID Management** - Generates unique task IDs for each worker
- ✅ **Testing Support** - Ideal for development and testing scenarios
- ✅ **Simple Workflow** - Streamlined processing without retry complexity

### Request Headers
```http
Content-Type: application/json
Authorization: Bearer <your-token>
```

### Request Body Schema
```json
{
  "job_id": "uuid",
  "queries": [
    "What is artificial intelligence?",
    "How does machine learning work?",
    "Explain neural networks"
  ]
}
```

### Request Body Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `job_id` | `string` | ✅ | Unique identifier for the orchestration job |
| `queries` | `array` | ✅ | Array of query strings to process |

### Response

#### Success Response (202 Accepted)
```json
{
  "status": "accepted",
  "message": "Adhoc batch job submitted successfully",
  "job_id": "uuid",
  "batch_job_id": "batch-job-uuid",
  "batch_job_name": "adhoc-orchestrator-20240115_143022-uuid",
  "queries_count": 3,
  "mode": "Adhoc_trigger",
  "session_id": "new-session-uuid",
  "polling_info": {
    "batch_job_id": "batch-job-uuid",
    "job_queue": "your-batch-queue",
    "query_example": "aws batch describe-jobs --jobs batch-job-uuid"
  }
}
```

### Example Usage

#### cURL
```bash
curl -X POST https://your-api-gateway-url.amazonaws.com/prod/adhoc-orchestrator-trigger \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token" \
  -d '{
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "queries": [
      "What is artificial intelligence?",
      "How does machine learning work?",
      "Explain neural networks"
    ]
  }'
```

---

## Common Response Codes

| Status Code | Description |
|-------------|-------------|
| `202 Accepted` | Request accepted, batch job submitted successfully |
| `400 Bad Request` | Invalid request format or missing required fields |
| `401 Unauthorized` | Authentication required or invalid |
| `500 Internal Server Error` | Server error during processing |

## Monitoring and Status

### 1. Batch Job Status
Use the `batch_job_id` from the response to monitor the AWS Batch job:

```bash
aws batch describe-jobs --jobs <batch-job-id>
```

### 2. Orchestration Logs
Check DynamoDB table `OrchestrationLogs` for detailed event tracking:

```bash
aws dynamodb query \
  --table-name OrchestrationLogs \
  --key-condition-expression "pk = :pk" \
  --expression-attribute-values '{":pk": {"S": "JOB#<job-id>"}}'
```

### 3. Worker Status
Monitor individual worker lambda functions in CloudWatch for detailed execution logs.

## Session ID Management

### New Requests
- Automatically generates new `session_id`
- Each worker receives unique `task_id` but same `session_id`
- Enables grouping related tasks together

### Retry Requests
- Requires existing `session_id` from previous request
- Reuses existing `task_id` values
- Sets task status to "retrying" for existing tasks

### Session Benefits
- **Task Grouping**: Filter tasks by session for bulk operations
- **Progress Tracking**: Monitor completion across multiple workers
- **Debugging**: Correlate related operations
- **Retry Operations**: Bulk retry failed tasks within a session

## Error Handling

### Common Error Scenarios

1. **Invalid Session ID**
   - Error: "Invalid retry request: session_id not found or no failed tasks"
   - Solution: Ensure session_id exists and has failed tasks

2. **Missing Required Fields**
   - Error: "job_id is required in the request body"
   - Solution: Include all required fields in request

3. **Invalid Query Format**
   - Error: "No selected_queries provided in the event"
   - Solution: Ensure proper query structure

### Retry Best Practices

1. **Validate Session**: Ensure session_id exists before retry
2. **Check Failures**: Verify failed tasks exist for the session
3. **Monitor Progress**: Track retry attempts and success rates
4. **Error Analysis**: Review failed_tasks table for failure patterns

### Selective Retry vs Legacy Retry

#### Selective Retry (Recommended)
- **Precision**: Retry specific failed tasks with specific models
- **Efficiency**: No unnecessary fan-out to all models
- **Control**: Choose the best model for each retry scenario
- **Performance**: Faster execution and better resource utilization

#### Legacy Retry (Deprecated)
- **Fan-out**: Retries all failed tasks across all models
- **Resource Intensive**: May trigger unnecessary worker executions
- **Less Control**: Cannot specify which model to use for retry

## Rate Limits and Quotas

- **Batch Job Submission**: Limited by AWS Batch service quotas
- **Lambda Invocation**: Limited by worker lambda concurrency
- **Database Operations**: Limited by RDS connection pool
- **S3 Operations**: Limited by S3 service quotas

## Security Considerations

1. **Authentication**: All endpoints require valid authentication
2. **Authorization**: Ensure users have permission to trigger orchestrations
3. **Input Validation**: All input is validated and sanitized
4. **Audit Logging**: All operations are logged for compliance

## Support and Troubleshooting

### Common Issues

1. **Batch Job Stuck**
   - Check AWS Batch job status
   - Verify worker lambda function health
   - Review CloudWatch logs for errors

2. **Session ID Not Found**
   - Verify session_id format (UUID)
   - Check if session exists in database
   - Ensure failed tasks exist for retry

3. **Worker Failures**
   - Check individual worker logs
   - Verify database connectivity
   - Review environment variables

### Getting Help

- **Logs**: Check CloudWatch logs for detailed error information
- **Monitoring**: Use AWS Batch console for job status
- **Database**: Query failed_tasks table for failure details
- **Support**: Contact your system administrator for persistent issues

---

## Changelog

### Version 2.1.0 (Current)
- ✅ Added selective retry support with retry_tasks format
- ✅ Each task can be given a specific model for retry (no fan-out)
- ✅ Enhanced retry validation and error handling
- ✅ Backward compatibility with legacy retry format

### Version 2.0.0
- ✅ Added session ID support for task grouping
- ✅ Added retry mechanism for failed tasks
- ✅ Added failed_tasks table for retry tracking
- ✅ Enhanced logging and monitoring capabilities
- ✅ Improved error handling and validation

### Version 1.0.0
- ✅ Basic orchestrator trigger functionality
- ✅ Worker lambda orchestration
- ✅ Basic error handling and logging
