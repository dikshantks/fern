# Firn

A web application for visualizing Apache Iceberg table metadata, snapshots, manifests, and statistics.

## Features

- **Catalog Management**: Connect to Hive Metastore or AWS Glue catalogs
- **Snapshot Lineage DAG**: Interactive graph visualization of snapshot history
- **Manifest Tree**: Hierarchical view of manifest lists, manifests, and data files
- **Data File Inspector**: Deep inspection of Parquet files including row groups, column stats, and data sampling
- **Puffin Statistics**: View NDV (distinct value counts) from Theta sketches
- **Storage Analytics**: Charts showing file size distribution, format breakdown, and partition stats
- **Operation Timeline**: History of DML operations with record/file counts



## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     React Frontend                               │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │ Snapshot │ │ Manifest │ │  Data    │ │  Puffin  │           │
│  │   DAG    │ │   Tree   │ │  Files   │ │  Viewer  │           │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘           │
└────────────────────────────┬────────────────────────────────────┘
                             │ REST API
┌────────────────────────────┴────────────────────────────────────┐
│                     FastAPI Backend                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │ Catalog  │ │ Metadata │ │ Manifest │ │  Puffin  │           │
│  │ Service  │ │ Service  │ │ Service  │ │ Service  │           │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘           │
└────────────────────────────┬────────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
   ┌────┴────┐          ┌────┴────┐          ┌────┴────┐
   │  Hive   │          │  AWS    │          │  S3 /   │
   │Metastore│          │  Glue   │          │  MinIO  │
   └─────────┘          └─────────┘          └─────────┘
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- Node.js 18+ (for local development)

### Using Docker Compose

1. Start all services:

```bash
cd docker
docker-compose up -d
```

This starts:
- MinIO (S3-compatible storage) at http://localhost:9000 (console: http://localhost:9001)
- PostgreSQL (Hive Metastore backing DB)
- Hive Metastore at thrift://localhost:9083
- Backend API at http://localhost:8000
- Frontend at http://localhost:3000

2. Generate sample data:

```bash
cd scripts
pip install pyiceberg pyarrow
python generate_sample_data.py
```

3. Open http://localhost:3000 and add a catalog:
   - Name: `local-hive`
   - Type: Hive
   - URI: `thrift://localhost:9083`
   - S3 Endpoint: `http://localhost:9000`
   - Access Key: `minioadmin`
   - Secret Key: `minioadmin`

### Local Development

#### Backend

```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env

# Run the server
uvicorn app.main:app --reload
```

API docs available at http://localhost:8000/docs

#### Frontend

```bash
cd frontend

# Install dependencies
npm install

# Copy environment file
cp .env.example .env

# Run development server
npm run dev
```

Frontend available at http://localhost:3000

## API Endpoints

### Catalogs
- `POST /api/catalogs` - Register a catalog
- `GET /api/catalogs` - List catalogs
- `GET /api/catalogs/{name}/test` - Test connectivity
- `DELETE /api/catalogs/{name}` - Remove catalog

### Tables
- `GET /api/tables?catalog=<name>` - List tables
- `GET /api/tables/{namespace}/{table}?catalog=<name>` - Get table metadata
- `GET /api/tables/{namespace}/{table}/metadata?catalog=<name>` - Get raw metadata.json

### Snapshots
- `GET /api/tables/{namespace}/{table}/snapshots?catalog=<name>` - Get snapshot graph
- `GET /api/tables/{namespace}/{table}/snapshots/{id}?catalog=<name>` - Get snapshot details
- `POST /api/tables/{namespace}/{table}/snapshots/compare?catalog=<name>&snapshot1=<id>&snapshot2=<id>` - Compare snapshots

### Manifests
- `GET /api/tables/{namespace}/{table}/snapshots/{id}/manifests?catalog=<name>` - Get manifest list
- `GET /api/tables/{namespace}/{table}/manifests?catalog=<name>&path=<manifest_path>` - Get manifest entries

### Data Files
- `GET /api/tables/{namespace}/{table}/snapshots/{id}/files?catalog=<name>` - List data files
- `GET /api/tables/{namespace}/{table}/files/inspect?catalog=<name>&path=<file_path>` - Inspect file
- `GET /api/tables/{namespace}/{table}/files/sample?catalog=<name>&path=<file_path>&rows=10` - Sample data

### Statistics
- `GET /api/tables/{namespace}/{table}/statistics?catalog=<name>` - List Puffin files
- `GET /api/tables/{namespace}/{table}/statistics/{snapshot_id}?catalog=<name>` - Get decoded stats

### Analytics
- `GET /api/tables/{namespace}/{table}/analytics/storage?catalog=<name>` - Storage analytics
- `GET /api/tables/{namespace}/{table}/analytics/history?catalog=<name>` - Operation history

## Tech Stack

### Backend
- Python 3.11+
- FastAPI + Uvicorn
- pyiceberg (Iceberg SDK)
- boto3 (S3/MinIO access)
- fastavro (Avro manifest parsing)
- pyarrow (Parquet inspection)
- Pydantic v2

### Frontend
- React 18 + TypeScript
- Vite
- React Flow (DAG visualization)
- TanStack Query (data fetching)
- Tailwind CSS
- Recharts (analytics charts)
- Lucide React (icons)

## License

MIT
