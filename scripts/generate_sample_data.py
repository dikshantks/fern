#!/usr/bin/env python3
"""
Generate sample Iceberg tables with multiple snapshots for testing.

Usage:
    source firn_venv/bin/activate
    python scripts/generate_sample_data.py

Prerequisites:
    - MinIO running at localhost:9000
    - Hive Metastore running at localhost:9083
    - 'warehouse' bucket created in MinIO
"""

import random
from datetime import datetime, timedelta

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, In, LessThan
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    DoubleType,
    BooleanType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform


def create_catalog():
    """Create a connection to the Hive catalog."""
    return load_catalog(
        "default",
        **{
            "type": "hive",
            "uri": "thrift://localhost:9083",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
        }
    )


def create_namespace(catalog, namespace: str):
    """Create a namespace if it doesn't exist."""
    try:
        catalog.create_namespace(namespace)
        print(f"Created namespace: {namespace}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"Namespace {namespace} already exists")
        else:
            raise


def create_sales_table(catalog):
    """Create a sample sales table with partitioning."""
    namespace = "demo"
    table_name = "sales"
    
    create_namespace(catalog, namespace)
    
    # Define schema
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "product_name", StringType(), required=True),
        NestedField(3, "category", StringType(), required=True),
        NestedField(4, "amount", DoubleType(), required=True),
        NestedField(5, "quantity", IntegerType(), required=True),
        NestedField(6, "customer_id", LongType(), required=False),
        NestedField(7, "is_returned", BooleanType(), required=False),
        NestedField(8, "sale_date", TimestampType(), required=True),
    )
    
    # Define partition spec (partition by day)
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=8,
            field_id=1000,
            transform=DayTransform(),
            name="sale_date_day"
        )
    )
    
    # Drop table if exists
    try:
        catalog.drop_table((namespace, table_name))
        print(f"Dropped existing table: {namespace}.{table_name}")
    except Exception:
        pass
    
    # Create table
    table = catalog.create_table(
        identifier=(namespace, table_name),
        schema=schema,
        location=f"s3a://warehouse/{namespace}/{table_name}",
        partition_spec=partition_spec,
    )
    
    print(f"Created table: {namespace}.{table_name}")
    return table


def create_users_table(catalog):
    """Create a sample users table without partitioning."""
    namespace = "demo"
    table_name = "users"
    
    create_namespace(catalog, namespace)
    
    # Define schema
    schema = Schema(
        NestedField(1, "user_id", LongType(), required=True),
        NestedField(2, "username", StringType(), required=True),
        NestedField(3, "email", StringType(), required=True),
        NestedField(4, "full_name", StringType(), required=False),
        NestedField(5, "created_at", TimestampType(), required=True),
        NestedField(6, "is_active", BooleanType(), required=True),
        NestedField(7, "country", StringType(), required=False),
    )
    
    # Drop table if exists
    try:
        catalog.drop_table((namespace, table_name))
        print(f"Dropped existing table: {namespace}.{table_name}")
    except Exception:
        pass
    
    # Create table
    table = catalog.create_table(
        identifier=(namespace, table_name),
        schema=schema,
        location=f"s3a://warehouse/{namespace}/{table_name}",
    )
    
    print(f"Created table: {namespace}.{table_name}")
    return table


def create_mor_orders_table(catalog):
    """
    Create a Merge-on-Read (MOR) orders table with identifier field for equality deletes.

    NOTE: PyIceberg does NOT yet support MOR for deletes. It always falls back to
    copy-on-write (rewriting data files). You will NOT see delete.parquet files
    in MinIO when using PyIceberg's table.delete(). To get actual delete files
    (equality/positional), use Apache Spark or Flink with Iceberg instead.
    """
    namespace = "demo"
    table_name = "orders"

    create_namespace(catalog, namespace)

    # Schema with id as identifier field (for equality deletes)
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "order_date", TimestampType(), required=True),
        NestedField(3, "customer_id", LongType(), required=True),
        NestedField(4, "product_id", LongType(), required=True),
        NestedField(5, "quantity", IntegerType(), required=True),
        NestedField(6, "amount", DoubleType(), required=True),
        NestedField(7, "status", StringType(), required=True),
        identifier_field_ids=[1],  # id is primary key for equality deletes
    )

    # Drop table if exists
    try:
        catalog.drop_table((namespace, table_name))
        print(f"Dropped existing table: {namespace}.{table_name}")
    except Exception:
        pass

    # Create MOR table
    table = catalog.create_table(
        identifier=(namespace, table_name),
        schema=schema,
        location=f"s3a://warehouse/{namespace}/{table_name}",
        properties={
            "write.delete.mode": "merge-on-read",
            "write.update.mode": "merge-on-read",
        },
    )

    print(f"Created MOR table: {namespace}.{table_name}")
    return table


def generate_sales_data(num_records: int, start_date: datetime) -> pa.Table:
    """Generate sample sales data."""
    categories = ["Electronics", "Clothing", "Food", "Books", "Toys", "Home", "Sports"]
    products = {
        "Electronics": ["Laptop", "Phone", "Tablet", "Headphones", "Camera"],
        "Clothing": ["Shirt", "Pants", "Jacket", "Shoes", "Hat"],
        "Food": ["Snacks", "Beverages", "Frozen", "Fresh", "Canned"],
        "Books": ["Fiction", "Non-Fiction", "Technical", "Children", "Comics"],
        "Toys": ["Action Figure", "Board Game", "Puzzle", "Doll", "LEGO"],
        "Home": ["Furniture", "Decor", "Kitchen", "Bedding", "Storage"],
        "Sports": ["Equipment", "Apparel", "Accessories", "Footwear", "Nutrition"],
    }
    
    data = {
        "id": [],
        "product_name": [],
        "category": [],
        "amount": [],
        "quantity": [],
        "customer_id": [],
        "is_returned": [],
        "sale_date": [],
    }
    
    for i in range(num_records):
        category = random.choice(categories)
        product = random.choice(products[category])
        
        data["id"].append(i + 1)
        data["product_name"].append(f"{product}_{random.randint(100, 999)}")
        data["category"].append(category)
        data["amount"].append(round(random.uniform(10, 1000), 2))
        data["quantity"].append(random.randint(1, 10))
        data["customer_id"].append(random.randint(1, 10000) if random.random() > 0.1 else None)
        data["is_returned"].append(random.random() < 0.05)
        data["sale_date"].append(
            start_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
        )
    
    # Explicit schema to match the Iceberg table (required vs optional, int32 vs int64)
    arrow_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("product_name", pa.string(), nullable=False),
        pa.field("category", pa.string(), nullable=False),
        pa.field("amount", pa.float64(), nullable=False),
        pa.field("quantity", pa.int32(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=True),
        pa.field("is_returned", pa.bool_(), nullable=True),
        pa.field("sale_date", pa.timestamp("us"), nullable=False),
    ])
    return pa.Table.from_pydict(data, schema=arrow_schema)


def generate_users_data(num_records: int, start_date: datetime) -> pa.Table:
    """Generate sample users data."""
    countries = ["USA", "UK", "Canada", "Germany", "France", "Japan", "Australia", "India"]
    first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
    
    data = {
        "user_id": [],
        "username": [],
        "email": [],
        "full_name": [],
        "created_at": [],
        "is_active": [],
        "country": [],
    }
    
    for i in range(num_records):
        first = random.choice(first_names)
        last = random.choice(last_names)
        username = f"{first.lower()}{last.lower()}{random.randint(1, 999)}"
        
        data["user_id"].append(i + 1)
        data["username"].append(username)
        data["email"].append(f"{username}@example.com")
        data["full_name"].append(f"{first} {last}" if random.random() > 0.2 else None)
        data["created_at"].append(
            start_date + timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23)
            )
        )
        data["is_active"].append(random.random() > 0.1)
        data["country"].append(random.choice(countries) if random.random() > 0.1 else None)
    
    # Explicit schema to match the Iceberg table (required vs optional)
    arrow_schema = pa.schema([
        pa.field("user_id", pa.int64(), nullable=False),
        pa.field("username", pa.string(), nullable=False),
        pa.field("email", pa.string(), nullable=False),
        pa.field("full_name", pa.string(), nullable=True),
        pa.field("created_at", pa.timestamp("us"), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("country", pa.string(), nullable=True),
    ])
    return pa.Table.from_pydict(data, schema=arrow_schema)


def generate_orders_data(
    num_records: int,
    start_date: datetime,
    start_id: int = 1,
    statuses: list[str] | None = None,
) -> pa.Table:
    """Generate sample orders data for the MOR table."""
    if statuses is None:
        statuses = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]

    data = {
        "id": [],
        "order_date": [],
        "customer_id": [],
        "product_id": [],
        "quantity": [],
        "amount": [],
        "status": [],
    }

    for i in range(num_records):
        data["id"].append(start_id + i)
        data["order_date"].append(
            start_date
            + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
            )
        )
        data["customer_id"].append(random.randint(1, 5000))
        data["product_id"].append(random.randint(100, 999))
        data["quantity"].append(random.randint(1, 10))
        data["amount"].append(round(random.uniform(10, 500), 2))
        data["status"].append(random.choice(statuses))

    arrow_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("order_date", pa.timestamp("us"), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("product_id", pa.int64(), nullable=False),
        pa.field("quantity", pa.int32(), nullable=False),
        pa.field("amount", pa.float64(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ])
    return pa.Table.from_pydict(data, schema=arrow_schema)


def simulate_sales_operations(catalog):
    """Simulate various DML operations on the sales table."""
    table = catalog.load_table(("demo", "sales"))
    base_date = datetime(2024, 1, 1)
    
    print("\n--- Simulating Sales Table Operations ---")
    
    # Operation 1: Initial data load
    print("\n[1/5] Initial data load (1000 records)...")
    data1 = generate_sales_data(1000, base_date)
    table.append(data1)
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")
    
    # Operation 2: Daily append
    print("\n[2/5] Daily append (500 records)...")
    data2 = generate_sales_data(500, base_date + timedelta(days=1))
    table.append(data2)
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")
    
    # Operation 3: Another append
    print("\n[3/5] Another append (300 records)...")
    data3 = generate_sales_data(300, base_date + timedelta(days=2))
    table.append(data3)
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")
    
    # Operation 4: Overwrite (simulating data correction)
    print("\n[4/5] Overwrite operation (800 records)...")
    data4 = generate_sales_data(800, base_date + timedelta(days=3))
    table.overwrite(data4)
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")
    
    # Operation 5: Recovery append
    print("\n[5/5] Recovery append (600 records)...")
    data5 = generate_sales_data(600, base_date + timedelta(days=4))
    table.append(data5)
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")
    
    print(f"\nSales table now has {len(list(table.snapshots()))} snapshots")
    return table


def simulate_users_operations(catalog):
    """Simulate various DML operations on the users table."""
    table = catalog.load_table(("demo", "users"))
    base_date = datetime(2023, 1, 1)
    
    print("\n--- Simulating Users Table Operations ---")
    
    # Operation 1: Initial users
    print("\n[1/3] Initial users load (500 records)...")
    data1 = generate_users_data(500, base_date)
    table.append(data1)
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")
    
    # Operation 2: More users
    print("\n[2/3] Adding more users (200 records)...")
    data2 = generate_users_data(200, base_date + timedelta(days=30))
    table.append(data2)
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")
    
    # Operation 3: Another batch
    print("\n[3/3] Another batch (150 records)...")
    data3 = generate_users_data(150, base_date + timedelta(days=60))
    table.append(data3)
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")
    
    print(f"\nUsers table now has {len(list(table.snapshots()))} snapshots")
    return table


def simulate_mor_operations(catalog):
    """
    Simulate various DML operations on the MOR orders table:

    - Insert: Initial load and appends
    - Equality delete: Delete by primary key (id IN (...))
    - Positional delete: Delete by non-key predicate (amount < 50)
    - Merge into: Delete + append to simulate upsert

    Note: PyIceberg uses copy-on-write for all deletes (rewrites data files);
    no delete.parquet files are created. Use Spark/Flink for actual MOR delete files.
    """
    table = catalog.load_table(("demo", "orders"))
    base_date = datetime(2024, 6, 1)

    print("\n--- Simulating MOR Orders Table Operations ---")

    # Operation 1: Initial insert
    print("\n[1/6] Initial insert (100 records)...")
    data1 = generate_orders_data(100, base_date, start_id=1)
    table.append(data1)
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")

    # Operation 2: Equality delete (delete by primary key - id)
    # Uses id IN (...) - produces equality delete files when id is identifier
    print("\n[2/6] Equality delete: DELETE WHERE id IN (5, 10, 15, 20, 25)...")
    table.delete(delete_filter=In("id", [5, 10, 15, 20, 25]))
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")

    # Operation 3: Positional delete (delete by non-key predicate)
    # amount < 50 requires scanning files - typically produces positional delete files
    print("\n[3/6] Positional delete: DELETE WHERE amount < 50...")
    table.delete(delete_filter=LessThan("amount", 50))
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")

    # Operation 4: Another insert (new orders)
    print("\n[4/6] Append new orders (50 records)...")
    data4 = generate_orders_data(50, base_date + timedelta(days=7), start_id=101)
    table.append(data4)
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")

    # Operation 5: Merge into simulation
    # Delete existing rows by id, then append "updated" versions
    print("\n[5/6] Merge into: delete rows 30, 40, 50 then insert updated versions...")
    table.delete(delete_filter=In("id", [30, 40, 50]))
    merge_data = pa.Table.from_pydict(
        {
            "id": [30, 40, 50],
            "order_date": [
                base_date + timedelta(days=10),
                base_date + timedelta(days=11),
                base_date + timedelta(days=12),
            ],
            "customer_id": [1001, 1002, 1003],
            "product_id": [201, 202, 203],
            "quantity": [2, 3, 1],
            "amount": [199.99, 299.99, 49.99],
            "status": ["SHIPPED", "DELIVERED", "CONFIRMED"],
        },
        schema=pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("order_date", pa.timestamp("us"), nullable=False),
            pa.field("customer_id", pa.int64(), nullable=False),
            pa.field("product_id", pa.int64(), nullable=False),
            pa.field("quantity", pa.int32(), nullable=False),
            pa.field("amount", pa.float64(), nullable=False),
            pa.field("status", pa.string(), nullable=False),
        ]),
    )
    table.append(merge_data)
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")

    # Operation 6: Delete by status (equality on non-identifier column - positional)
    # status is not in identifier_field_ids, so this produces positional delete files
    print("\n[6/6] Positional delete: DELETE WHERE status = 'CANCELLED'...")
    table.delete(delete_filter=EqualTo("status", "CANCELLED"))
    print(f"    Snapshot ID: {table.current_snapshot().snapshot_id}")

    print(f"\nOrders (MOR) table now has {len(list(table.snapshots()))} snapshots")
    return table


def main():
    """Main function to generate sample data."""
    print("=" * 60)
    print("Iceberg Sample Data Generator")
    print("=" * 60)
    
    print("\nConnecting to catalog...")
    catalog = create_catalog()
    
    print("\n--- Creating Tables ---")
    create_sales_table(catalog)
    create_users_table(catalog)
    create_mor_orders_table(catalog)

    print("\n--- Generating Sample Data ---")
    simulate_sales_operations(catalog)
    simulate_users_operations(catalog)
    simulate_mor_operations(catalog)
    
    print("\n" + "=" * 60)
    print("Sample data generation complete!")
    print("=" * 60)
    
    print("\nYou can now:")
    print("1. Start the backend API: cd backend && uvicorn app.main:app --reload")
    print("2. Access API docs: http://localhost:8000/docs")
    print("3. Register the catalog via API:")
    print('   POST http://localhost:8000/api/catalogs')
    print('   {')
    print('     "name": "local-hive",')
    print('     "type": "hive",')
    print('     "properties": {')
    print('       "uri": "thrift://localhost:9083",')
    print('       "s3.endpoint": "http://localhost:9000",')
    print('       "s3.access-key-id": "minioadmin",')
    print('       "s3.secret-access-key": "minioadmin"')
    print('     }')
    print('   }')
    print("4. List tables: GET http://localhost:8000/api/tables?catalog=local-hive")
    print("5. View snapshots: GET http://localhost:8000/api/tables/demo/sales/snapshots?catalog=local-hive")


if __name__ == "__main__":
    main()
