"""FastAPI application entry point."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.models import CatalogType
from app.routers import (
    catalogs,
    tables,
    snapshots,
    manifests,
    data_files,
    puffin,
    analytics,
    health,
    spark_optimization,
)
from app.services import catalog_service


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan handler for startup/shutdown events."""
    # Startup
    print("Starting Iceberg Metadata Visualizer API...")

    # Auto-register default catalog if configured via environment variables
    if settings.default_catalog_name and settings.default_catalog_uri:
        try:
            catalog_type = CatalogType(settings.default_catalog_type)
            properties: dict[str, str] = {"uri": settings.default_catalog_uri}

            # Add default S3 settings if configured
            if settings.default_s3_endpoint:
                properties["s3.endpoint"] = settings.default_s3_endpoint
            if settings.default_s3_access_key:
                properties["s3.access-key-id"] = settings.default_s3_access_key
            if settings.default_s3_secret_key:
                properties["s3.secret-access-key"] = settings.default_s3_secret_key

            info = catalog_service.register_catalog(
                name=settings.default_catalog_name,
                catalog_type=catalog_type,
                properties=properties,
            )
            print(
                f"Auto-registered default catalog '{info.name}': "
                f"{info.namespace_count} namespaces, {info.table_count} tables"
            )
        except Exception as e:
            print(f"Warning: failed to auto-register default catalog: {e}")

    yield
    # Shutdown
    print("Shutting down Iceberg Metadata Visualizer API...")


app = FastAPI(
    title="Iceberg Metadata Visualizer API",
    description="API for visualizing Apache Iceberg table metadata, snapshots, manifests, and statistics.",
    version="0.1.0",
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(catalogs.router, prefix="/api/catalogs", tags=["Catalogs"])
app.include_router(tables.router, prefix="/api/tables", tags=["Tables"])
app.include_router(snapshots.router, prefix="/api/tables", tags=["Snapshots"])
app.include_router(manifests.router, prefix="/api/tables", tags=["Manifests"])
app.include_router(data_files.router, prefix="/api/tables", tags=["Data Files"])
app.include_router(puffin.router, prefix="/api/tables", tags=["Statistics"])
app.include_router(analytics.router, prefix="/api/tables", tags=["Analytics"])
app.include_router(health.router, prefix="/api/health", tags=["Health"])
app.include_router(spark_optimization.router, prefix="/api/optimization", tags=["optimization"])


@app.get("/")
async def root() -> dict:
    """Root endpoint returning API info."""
    return {
        "name": "Iceberg Metadata Visualizer API",
        "version": "0.1.0",
        "docs": "/docs",
        "openapi": "/openapi.json",
    }


@app.get("/health")
async def health() -> dict:
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.reload,
    )
