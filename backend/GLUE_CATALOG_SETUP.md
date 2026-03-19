# AWS Glue Catalog Setup Guide

This guide explains how to properly configure AWS Glue catalogs with PyIceberg for S3 access.

## Understanding PyIceberg Property Namespaces

PyIceberg uses **different property namespaces** for different components:

### 1. Glue API (boto3 client)
Properties for connecting to AWS Glue service:
- `client.region` or `glue.region` - AWS region
- `client.access-key-id` or `glue.access-key-id` - AWS access key
- `client.secret-access-key` or `glue.secret-access-key` - AWS secret key
- `client.session-token` or `glue.session-token` - AWS session token (optional)
- `client.profile-name` or `glue.profile-name` - AWS profile name (optional)

### 2. S3 FileIO (PyArrow S3 client)
Properties for accessing S3 data:
- `s3.region` - **REQUIRED** for S3 access with credentials
- `s3.access-key-id` - S3 access key
- `s3.secret-access-key` - S3 secret key
- `s3.session-token` - S3 session token (optional)
- `s3.endpoint` - Custom S3 endpoint (for MinIO, etc.)

## Configuration Methods

### Method 1: Using boto3-style parameters (Recommended)

The backend automatically normalizes boto3 parameter names to PyIceberg properties:

```json
{
  "name": "my-glue-catalog",
  "type": "glue",
  "properties": {
    "region_name": "us-east-1",
    "aws_access_key_id": "AKIA...",
    "aws_secret_access_key": "..."
  }
}
```

The backend will automatically:
1. Convert `region_name` → `client.region`
2. Convert `aws_access_key_id` → `client.access-key-id`
3. Convert `aws_secret_access_key` → `client.secret-access-key`
4. Copy `client.region` → `s3.region` (required for S3 access)
5. Copy `client.access-key-id` → `s3.access-key-id`
6. Copy `client.secret-access-key` → `s3.secret-access-key`

### Method 2: Using AWS Profile

```json
{
  "name": "my-glue-catalog",
  "type": "glue",
  "properties": {
    "region_name": "us-east-1",
    "profile_name": "production"
  }
}
```

### Method 3: Explicit PyIceberg properties

```json
{
  "name": "my-glue-catalog",
  "type": "glue",
  "properties": {
    "client.region": "us-east-1",
    "client.access-key-id": "AKIA...",
    "client.secret-access-key": "...",
    "s3.region": "us-east-1",
    "s3.access-key-id": "AKIA...",
    "s3.secret-access-key": "..."
  }
}
```

## Common Errors and Solutions

### Error: ACCESS_DENIED during HeadObject operation

**Cause**: Missing `s3.region` property for PyArrow S3 client.

**Solution**: Ensure `region_name` or `s3.region` is set in catalog properties.

### Error: Empty path component in path

**Cause**: Table location in Glue has malformed path with double slashes (`//`).

**Example**: `s3://bucket/db/table//metadata/file.json`

**Solution**: This is a data quality issue in Glue metadata. Fix the table location in AWS Glue:
```python
# AWS CLI
aws glue update-table --database-name mydb --table-input '{
  "Name": "mytable",
  "StorageDescriptor": {
    "Location": "s3://bucket/db/table"
  }
}'
```

### Error: NoSuchTableError or table not found

**Cause**: Insufficient IAM permissions.

**Solution**: Ensure IAM user/role has these permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    }
  ]
}
```

## Testing Your Configuration

Use the catalog test endpoint to verify your configuration:

```bash
curl -X POST http://localhost:8000/api/catalogs/test \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-glue",
    "type": "glue",
    "properties": {
      "region_name": "us-east-1",
      "aws_access_key_id": "AKIA...",
      "aws_secret_access_key": "..."
    }
  }'
```

## Direct PyIceberg Usage

If using PyIceberg directly in Python:

```python
from pyiceberg.catalog.glue import GlueCatalog

# Method 1: boto3-style (PyIceberg handles internally)
catalog = GlueCatalog(
    "my_catalog",
    **{
        "region_name": "us-east-1",
        "aws_access_key_id": "AKIA...",
        "aws_secret_access_key": "...",
    }
)

# Method 2: Explicit properties (recommended for clarity)
catalog = GlueCatalog(
    "my_catalog",
    **{
        "client.region": "us-east-1",
        "client.access-key-id": "AKIA...",
        "client.secret-access-key": "...",
        "s3.region": "us-east-1",           # Required for S3 access
        "s3.access-key-id": "AKIA...",      # Required for S3 access
        "s3.secret-access-key": "...",      # Required for S3 access
    }
)
```

## Reference

- [PyIceberg Glue Catalog Documentation](https://py.iceberg.apache.org/reference/pyiceberg/catalog/glue/)
- [PyIceberg Configuration](https://py.iceberg.apache.org/configuration/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
