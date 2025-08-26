# Workers Update Summary - Session ID and Retry Support

## Overview
All 4 worker lambda functions have been updated to support session IDs and retry mechanisms. The changes ensure that workers can handle both new requests and retry requests with proper session tracking.

## Updated Workers

### 1. AIM Worker (`lambdas/workers/aim/crawlforai.py`)
✅ **Updated Functions:**
- `create_llm_task()` - Added session_id and task_id parameters
- `update_task_status()` - Added failed_tasks recording
- `create_failed_task_record()` - New function for failed task tracking
- `lambda_handler()` - Updated to extract and use session_id/task_id

**Key Changes:**
- Accepts `session_id` and `task_id` from orchestrator
- Checks for existing tasks during retry scenarios
- Sets status to "retrying" for existing tasks
- Creates `failed_tasks` records for failed operations
- Enhanced logging with session information

### 2. ChatGPT Worker (`lambdas/workers/ChatGPT/brightdata.py`)
✅ **Updated Functions:**
- `create_llm_task()` - Added session_id and task_id parameters
- `update_task_status()` - Added failed_tasks recording
- `create_failed_task_record()` - New function for failed task tracking
- `lambda_handler()` - Updated to extract and use session_id/task_id

**Key Changes:**
- Same functionality as AIM worker
- Maintains consistency across all workers
- Enhanced error handling and logging

### 3. AIO Worker (`lambdas/workers/aio/lambda_function.py`)
✅ **Updated Functions:**
- `create_llm_task()` - Added session_id and task_id parameters
- `update_task_status()` - Added failed_tasks recording
- `create_failed_task_record()` - New function for failed task tracking
- `lambda_handler()` - Updated to extract and use session_id/task_id

**Key Changes:**
- Same functionality as other workers
- Enhanced orchestration event logging
- Session ID tracking in all database operations

### 4. Perplexity Worker (`lambdas/workers/perplexity/lambda_function.py`)
✅ **Updated Functions:**
- `create_llm_task()` - Added session_id and task_id parameters
- `update_task_status()` - Added failed_tasks recording
- `create_failed_task_record()` - New function for failed task tracking
- `lambda_handler()` - Updated to extract and use session_id/task_id

**Key Changes:**
- Same functionality as other workers
- Enhanced orchestration event logging
- Session ID tracking in all database operations

## Common Changes Across All Workers

### 1. Function Signature Updates
```python
# Before
def create_llm_task(job_id, query_id, llm_model_name, product_id, product_name):

# After  
def create_llm_task(job_id, query_id, llm_model_name, product_id, product_name, session_id=None, task_id=None):
```

### 2. Retry Logic Implementation
```python
# Check if task_id already exists (for retry scenarios)
if session_id:
    cursor.execute("SELECT status FROM llmtasks WHERE task_id = %s", (task_id,))
    existing_task = cursor.fetchone()
    
    if existing_task:
        # Task exists, update status to "retrying"
        cursor.execute(
            "UPDATE llmtasks SET status = %s, session_id = %s WHERE task_id = %s",
            ("retrying", session_id, task_id)
        )
        return task_id
```

### 3. Failed Tasks Recording
```python
def create_failed_task_record(task_id, error_message):
    """Create a record in failed_tasks table when a task fails"""
    # Copies task data from llmtasks to failed_tasks
    # Includes retry_count and failed_at timestamp
```

### 4. Enhanced Database Operations
```python
# Insert new task with session_id
cursor.execute("""
    INSERT INTO llmtasks (
        task_id, job_id, query_id, llm_model_name, status, 
        created_at, product_id, product_name, session_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""", (task_id, job_id, query_id, llm_model_name, "created", 
       datetime.now(timezone.utc), product_id, product_name, session_id))
```

### 5. Lambda Handler Updates
```python
# Extract session_id and task_id from event
session_id = event.get('session_id')
provided_task_id = event.get('task_id')

# Pass to create_llm_task
task_id = create_llm_task(job_id, query_id, llm_model_name, product_id, product_name, session_id, provided_task_id)
```

## Database Schema Requirements

### 1. llmtasks Table
- **New Column**: `session_id UUID` (nullable)
- **Index**: `idx_llmtasks_session_id` for efficient filtering

### 2. failed_tasks Table
- **New Table**: Stores failed task information for retry operations
- **Structure**: Same as llmtasks with additional retry tracking fields
- **Key Fields**: `id` (BIGSERIAL), `retry_count`, `failed_at`

## Benefits of These Updates

### 1. **Session Tracking**
- Group related tasks together
- Filter tasks by session for bulk operations
- Enhanced monitoring and debugging capabilities

### 2. **Retry Support**
- Reuse existing task IDs for retry operations
- Maintain task context across retry attempts
- Track retry counts and failure reasons

### 3. **Consistency**
- All workers now handle session_id and task_id uniformly
- Standardized error handling and logging
- Consistent database operations across workers

### 4. **Operational Efficiency**
- Reduced duplicate task creation
- Better visibility into retry operations
- Enhanced audit trail for task attempts

## Testing Recommendations

### 1. **New Request Flow**
- Verify session_id is generated and stored
- Check that task_id is unique for each worker
- Confirm database records include session_id

### 2. **Retry Request Flow**
- Test with existing session_id
- Verify task status changes to "retrying"
- Check failed_tasks table creation

### 3. **Error Scenarios**
- Test worker failures and failed_tasks recording
- Verify session_id propagation in error cases
- Check logging consistency across workers

## Deployment Notes

### 1. **Database Migration**
- Run `db_migration_session_retry.sql` before deploying workers
- Verify new columns and tables are created
- Check index creation for performance

### 2. **Worker Deployment**
- Deploy all 4 workers simultaneously
- Ensure environment variables are properly set
- Test with small batch of requests first

### 3. **Monitoring**
- Watch CloudWatch logs for any errors
- Monitor database performance with new indexes
- Check DynamoDB orchestration logs for session information

## Backward Compatibility

✅ **Maintained**: All existing functionality continues to work
✅ **Enhanced**: New features are additive, not breaking
✅ **Optional**: session_id and task_id are optional parameters
✅ **Gradual**: Can be rolled out incrementally if needed

## Next Steps

1. **Deploy Database Migration**
2. **Deploy Updated Workers**
3. **Test New Request Flow**
4. **Test Retry Request Flow**
5. **Monitor Performance and Logs**
6. **Update Frontend for Session Filtering**

All workers are now ready for the session ID and retry mechanism implementation!
