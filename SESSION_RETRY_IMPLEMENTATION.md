# Session ID and Retry Mechanism Implementation

## Overview

This document describes the implementation of session IDs and retry mechanisms for the Bodhium LLM orchestrator system. The implementation adds the ability to group related tasks and retry failed operations.

## Architecture Changes

### 1. Database Schema Updates

#### New Column in `llmtasks` Table
- **`session_id`**: UUID field to group related tasks together
- **Index**: `idx_llmtasks_session_id` for efficient filtering

#### New Table: `failed_tasks`
- **Purpose**: Store failed task information for retry operations
- **Structure**: Same as `llmtasks` but with auto-increment primary key and retry tracking
- **Key Fields**:
  - `id`: BIGSERIAL primary key
  - `task_id`: UUID reference to original task
  - `session_id`: UUID to group failed tasks
  - `retry_count`: Integer tracking retry attempts
  - `failed_at`: Timestamp when task was marked as failed

### 2. Orchestrator Updates

#### Main Orchestrator (`batches/main-orchestrator/app.py`)
- **Session ID Generation**: Automatically generates `session_id` for new requests
- **Retry Support**: Handles retry requests with existing `session_id`
- **Task ID Generation**: Generates unique `task_id` for each worker call
- **Forwarding**: Passes `session_id` and `task_id` to all workers

#### Adhoc Orchestrator (`batches/adhoc-orchestrator/app.py`)
- **Session ID Support**: Generates `session_id` for task grouping
- **No Retry Logic**: Only supports session grouping, not retry operations

### 3. Lambda Trigger Updates

#### Orchestrator Trigger (`lambdas/orchestrator-trigger/lambda_function.py`)
- **Retry Validation**: Validates retry requests and `session_id`
- **Enhanced Logging**: Includes retry information in all logs
- **Request Schema**: Supports new retry request format

#### Retry Request Format
```json
{
  "tasks": ["uuid", "uuid"],
  "options": {
    "retry": true,
    "session_id": "uuid"
  }
}
```

### 4. Worker Updates

#### All Workers (AIM, ChatGPT, AIO, Perplexity)
- **Session ID Handling**: Accepts and stores `session_id` from orchestrator
- **Task ID Handling**: Uses provided `task_id` instead of generating new ones
- **Retry Logic**: 
  - If `task_id` exists, sets status to "retrying"
  - Creates `failed_tasks` records for failed operations
- **Database Updates**: Modified `create_llm_task` and `update_task_status` functions

## Implementation Details

### Session ID Flow

1. **New Request**: Orchestrator generates new `session_id`
2. **Worker Calls**: Each worker receives same `session_id` but unique `task_id`
3. **Database Storage**: Both `session_id` and `task_id` stored in `llmtasks`
4. **Grouping**: Tasks can be filtered by `session_id` for bulk operations

### Retry Mechanism Flow

1. **Retry Request**: Client sends retry request with `session_id`
2. **Validation**: Orchestrator validates `session_id` and failed tasks
3. **Task Reuse**: Uses existing `task_id` values for retry
4. **Status Update**: Sets existing tasks to "retrying" status
5. **Worker Execution**: Workers process retry requests normally

### Database Operations

#### Creating Tasks
```python
def create_llm_task(job_id, query_id, llm_model_name, product_id, product_name, session_id, task_id):
    # Check if task exists (for retry scenarios)
    if session_id and task_id:
        # Update existing task to "retrying"
        # Return existing task_id
    else:
        # Create new task with generated task_id
```

#### Failed Task Recording
```python
def create_failed_task_record(task_id, error_message):
    # Copy task data from llmtasks to failed_tasks
    # Include retry_count and failed_at timestamp
```

## Usage Examples

### 1. New Request (Normal Flow)
```bash
curl -X POST /orchestrator-trigger \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "uuid",
    "selected_queries": [...],
    "options": {}
  }'
```

### 2. Retry Request
```bash
curl -X POST /orchestrator-trigger \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "uuid",
    "selected_queries": [...],
    "options": {
      "retry": true,
      "session_id": "existing-session-uuid"
    }
  }'
```

## Benefits

### 1. Task Grouping
- **Filtering**: Easily find all tasks for a specific session
- **Monitoring**: Track progress across multiple workers
- **Debugging**: Correlate related operations

### 2. Retry Capability
- **Bulk Retry**: Retry multiple failed tasks at once
- **State Preservation**: Maintain task context across retries
- **Error Tracking**: Track retry attempts and failure reasons

### 3. Operational Efficiency
- **Reduced Duplication**: Avoid creating duplicate task records
- **Better Logging**: Enhanced visibility into retry operations
- **Audit Trail**: Complete history of task attempts

## Migration Guide

### 1. Database Migration
```sql
-- Run the migration script
\i db_migration_session_retry.sql
```

### 2. Deploy Updated Code
- Update orchestrator batch jobs
- Update worker lambda functions
- Update orchestrator trigger lambda

### 3. Verify Functionality
- Test new request flow
- Test retry request flow
- Verify database records

## Monitoring and Debugging

### 1. Session Tracking
```sql
-- Find all tasks for a session
SELECT * FROM llmtasks WHERE session_id = 'uuid';

-- Find failed tasks for a session
SELECT * FROM failed_tasks WHERE session_id = 'uuid';
```

### 2. Retry Monitoring
```sql
-- Check retry counts
SELECT task_id, retry_count, failed_at 
FROM failed_tasks 
WHERE retry_count > 0;
```

### 3. Orchestration Logs
- **DynamoDB**: Check `OrchestrationLogs` table for retry events
- **CloudWatch**: Monitor lambda execution logs
- **Batch Jobs**: Check AWS Batch job status and logs

## Future Enhancements

### 1. Advanced Retry Logic
- **Exponential Backoff**: Implement smart retry timing
- **Max Retry Limits**: Prevent infinite retry loops
- **Retry Policies**: Configurable retry strategies

### 2. Session Management
- **Session Expiry**: Automatic cleanup of old sessions
- **Session Metadata**: Additional session-level information
- **Session Analytics**: Performance metrics per session

### 3. Bulk Operations
- **Selective Retry**: Retry specific failed tasks only
- **Batch Status Updates**: Update multiple tasks simultaneously
- **Progress Tracking**: Real-time retry progress monitoring

## Troubleshooting

### Common Issues

1. **Invalid Session ID**: Ensure UUID format is correct
2. **Missing Failed Tasks**: Check if tasks actually failed before retry
3. **Database Errors**: Verify migration script ran successfully
4. **Worker Errors**: Check worker logs for session_id handling

### Debug Commands

```bash
# Check orchestrator logs
aws logs filter-log-events --log-group-name /aws/lambda/orchestrator-trigger

# Check batch job status
aws batch describe-jobs --jobs job-id

# Check database connectivity
psql -h host -U user -d database -c "SELECT version();"
```

## Conclusion

The session ID and retry mechanism implementation provides a robust foundation for managing complex LLM orchestration workflows. It enables better task grouping, efficient retry operations, and improved operational visibility.

The implementation maintains backward compatibility while adding powerful new capabilities for production environments.
