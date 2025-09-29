# Data Catalog MVP Setup Guide

## Overview

This data catalog system provides automated schema discovery from Microsoft SQL Server, stores metadata in Neo4j graph database, and offers an interactive web interface built with Streamlit.

## Architecture Components

- **Schema Extractor**: Automated SQL Server metadata extraction
- **FastAPI Backend**: REST API for data access and management
- **Neo4j Database**: Graph storage for relationships and metadata
- **Streamlit Frontend**: Interactive web interface
- **Background Scheduler**: Automated schema refresh

## Quick Start (Docker)

### Prerequisites
- Docker and Docker Compose
- Access to Microsoft SQL Server
- 8GB+ RAM recommended

### 1. Clone and Setup

```bash
# Create project directory
mkdir data-catalog-mvp
cd data-catalog-mvp

# Create directory structure
mkdir -p app streamlit_app

# Copy the provided files:
# - requirements.txt → ./requirements.txt
# - docker-compose.yml → ./docker-compose.yml
# - Dockerfile.api → ./Dockerfile.api
# - Dockerfile.streamlit → ./Dockerfile.streamlit
# - schema_extractor.py → ./schema_extractor.py
# - FastAPI backend code → ./app/main.py
# - Streamlit frontend code → ./streamlit_app/main.py
```

### 2. Configure Environment

```bash
# Edit docker-compose.yml and update:
# - NEO4J_PASSWORD (line 12)
# - SQL_SERVER_CONN (line 28)

# Example SQL Server connection string:
# mssql+pymssql://username:password@server_host:1433/database_name
```

### 3. Launch Services

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f api
docker-compose logs -f frontend
```

### 4. Access Applications

- **Neo4j Browser**: http://localhost:7474
- **API Documentation**: http://localhost:8000/docs
- **Data Catalog UI**: http://localhost:8501

## Manual Installation (Development)

### Prerequisites
- Python 3.11+
- Neo4j Community Edition
- Microsoft SQL Server access

### 1. Install Neo4j

```bash
# Download Neo4j Community Edition
# https://neo4j.com/download-center/

# Or using Docker
docker run -d \
    --name neo4j \
    -p 7474:7474 -p 7687:7687 \
    -e NEO4J_AUTH=neo4j/your_password \
    neo4j:5.13-community
```

### 2. Python Environment Setup

```bash
# Create virtual environment
python -m venv data-catalog-env
source data-catalog-env/bin/activate  # Linux/Mac
# data-catalog-env\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration

Create `.env` file:

```bash
# .env
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password_here
SQL_SERVER_CONN=mssql+pymssql://user:pass@server/database
API_BASE_URL=http://localhost:8000
```

### 4. Run Services

Terminal 1 - API:
```bash
cd app
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Terminal 2 - Frontend:
```bash
cd streamlit_app
streamlit run main.py --server.port 8501
```

## Initial Schema Load

### Automatic (Recommended)
The system automatically extracts schema on first API startup and daily at 2 AM.

### Manual Trigger
```bash
# Via API
curl -X POST http://localhost:8000/refresh-schema

# Or use the Admin page in Streamlit UI
```

### Direct Python Execution
```python
import asyncio
from schema_extractor import SchemaExtractor

async def main():
    extractor = SchemaExtractor(
        sql_server_conn_str="your_connection_string",
        neo4j_uri="bolt://localhost:7687",
        neo4j_user="neo4j",
        neo4j_password="your_password"
    )
    
    schema_data = await extractor.extract_full_schema()
    await extractor.load_to_neo4j(schema_data)
    extractor.close()

asyncio.run(main())
```

## Configuration Options

### SQL Server Connection Strings

```python
# Windows Authentication
"mssql+pymssql://server_name/database_name?trusted_connection=yes"

# SQL Authentication
"mssql+pymssql://username:password@server_name:1433/database_name"

# Advanced options
"mssql+pymssql://user:pass@server/db?charset=utf8&timeout=30"
```

### Neo4j Configuration

```yaml
# docker-compose.yml additions for production
environment:
  - NEO4J_dbms_memory_heap_initial__size=512m
  - NEO4J_dbms_memory_heap_max__size=2G
  - NEO4J_dbms_memory_pagecache_size=1G
  - NEO4J_dbms_logs_query_enabled=true
```

## Usage Guide

### 1. Dashboard
- View catalog statistics
- Quick search functionality
- System health status

### 2. Search
- Full-text search across all metadata
- Filter by object type (database, schema, table, column)
- Click results to view details

### 3. Table Details
- Complete column information
- Foreign key relationships
- Data lineage connections
- Related data products

### 4. Data Lineage
- Interactive graph visualization
- Configurable depth levels
- Upstream/downstream dependency analysis

### 5. Data Products
- Create logical data products
- Link to source tables
- Tag and categorize products
- Track ownership and documentation

## API Endpoints

### Schema Management
- `POST /refresh-schema` - Trigger schema refresh
- `GET /stats` - Get catalog statistics

### Search & Discovery
- `GET /search?q=query&type_filter=table&limit=50` - Search catalog
- `GET /table/{database}/{schema}/{table}` - Table details
- `GET /lineage/{database}/{schema}/{table}?depth=2` - Lineage graph

### Data Products
- `GET /data-products` - List data products
- `POST /data-products` - Create data product

## Troubleshooting

### Common Issues

**1. SQL Server Connection Fails**
```bash
# Check connection string format
# Verify SQL Server allows remote connections
# Ensure pymssql is installed correctly
pip install pymssql --no-cache-dir
```

**2. Neo4j Connection Issues**
```bash
# Verify Neo4j is running
docker ps | grep neo4j

# Check Neo4j logs
docker logs neo4j

# Test connection
cypher-shell -u neo4j -p your_password "RETURN 1"
```

**3. Schema Extraction Errors**
```python
# Check SQL Server permissions
# User needs SELECT on information_schema views
# For cross-database queries, may need sysadmin or cross db owner chaining
```

**4. Streamlit Not Loading**
```bash
# Clear browser cache
# Check API connectivity
curl http://localhost:8000/health

# Restart services
docker-compose restart frontend
```

### Performance Optimization

**Large Databases (1000+ tables)**
```python
# Modify schema_extractor.py
# Add table filtering
def should_include_table(self, table_name: str) -> bool:
    # Skip temp tables, system tables
    return not table_name.startswith(('tmp_', 'sys_', '#'))

# Batch processing
async def extract_tables_batch(self, tables: List[str], batch_size=50):
    for i in range(0, len(tables), batch_size):
        batch = tables[i:i + batch_size]
        # Process batch
```

**Neo4j Query Optimization**
```cypher
// Add indexes for better search performance
CREATE INDEX table_name_index FOR (t:Table) ON (t.name)
CREATE INDEX column_name_index FOR (c:Column) ON (c.name)
CREATE FULLTEXT INDEX entity_names FOR (n:Database|Schema|Table|Column) ON EACH [n.name]
```

## Production Deployment

### Security Considerations
1. Change default Neo4j password
2. Use environment variables for all secrets
3. Enable HTTPS for web interfaces
4. Restrict network access
5. Regular security updates

### Monitoring
```yaml
# Add to docker-compose.yml
  prometheus:
    image: prom/prometheus
    ports:
      - "9090:9090"
    
  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
```

### Backup Strategy
```bash
# Neo4j backup
docker exec neo4j neo4j-admin dump --database=neo4j --to=/backups/

# Automated backups
# Add cron job or scheduled task
```

## Extending the System

### Adding New Data Sources
1. Create new extractor class inheriting from base
2. Implement source-specific metadata extraction
3. Map to common schema format
4. Register with scheduler

### Custom Data Product Types
```python
# Extend DataProduct model
class AnalyticsProduct(DataProduct):
    refresh_frequency: str
    quality_checks: List[str]
    business_metrics: Dict[str, Any]
```

### Advanced Lineage
- Parse SQL queries for column-level lineage
- Integration with dbt for transformation lineage
- Real-time change data capture

## Support and Development

### Logging Configuration
```python
# In main.py
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_catalog.log'),
        logging.StreamHandler()
    ]
)
```

### Testing
```bash
# Run tests
pytest tests/

# API testing
curl -X GET "http://localhost:8000/search?q=customer&limit=10"
```

This setup provides a robust foundation for your data catalog MVP with room for expansion as requirements grow.
