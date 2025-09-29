from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import asyncio
from neo4j import GraphDatabase
from contextlib import asynccontextmanager
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Import our schema extractor and utilities
from schema_extractor import SchemaExtractor
from neo4j_utils import convert_neo4j_datetime, safe_get_datetime

# Pydantic models
class SearchResult(BaseModel):
    id: str
    name: str
    type: str  # 'database', 'schema', 'table', 'column'
    path: str  # full path like 'database.schema.table.column'
    description: Optional[str] = None
    metadata: Dict[str, Any] = {}

class DataProduct(BaseModel):
    name: str
    description: str
    owner: str
    tags: List[str] = []
    source_tables: List[str] = []  # List of table paths
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class TableDetail(BaseModel):
    name: str
    schema: str
    database: str
    type: str
    row_count: Optional[int]
    columns: List[Dict[str, Any]]
    foreign_keys: List[Dict[str, Any]] = []
    referenced_by: List[Dict[str, Any]] = []
    data_products: List[str] = []
    last_analyzed: Optional[datetime] = None

class LineageNode(BaseModel):
    id: str
    name: str
    type: str
    metadata: Dict[str, Any] = {}

class LineageEdge(BaseModel):
    source: str
    target: str
    relationship: str

class LineageGraph(BaseModel):
    nodes: List[LineageNode]
    edges: List[LineageEdge]

# Global variables
neo4j_driver = None
schema_extractor = None
scheduler = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global neo4j_driver, schema_extractor, scheduler
    
    # Initialize Neo4j connection
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
    neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    
    # Initialize schema extractor
    sql_conn_str = os.getenv("SQL_SERVER_CONN", "mssql+pymssql://user:password@server/database")
    schema_extractor = SchemaExtractor(sql_conn_str, neo4j_uri, neo4j_user, neo4j_password)
    
    # Initialize scheduler for automated schema refresh
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        refresh_schema_job,
        CronTrigger(hour=2, minute=0),  # Daily at 2 AM
        id='schema_refresh',
        replace_existing=True
    )
    scheduler.start()
    
    yield
    
    # Shutdown
    if neo4j_driver:
        neo4j_driver.close()
    if schema_extractor:
        schema_extractor.close()
    if scheduler:
        scheduler.shutdown()

app = FastAPI(
    title="Data Catalog API",
    description="API for managing data catalog with schema discovery and data products",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_neo4j_session():
    """Dependency to get Neo4j session"""
    with neo4j_driver.session() as session:
        yield session

# Background job for schema refresh
async def refresh_schema_job():
    """Background job to refresh schema data"""
    try:
        schema_data = await schema_extractor.extract_full_schema()
        await schema_extractor.load_to_neo4j(schema_data)
        print(f"Schema refresh completed at {datetime.utcnow()}")
    except Exception as e:
        print(f"Schema refresh failed: {e}")

# API Endpoints

@app.post("/refresh-schema")
async def refresh_schema(background_tasks: BackgroundTasks):
    """Trigger manual schema refresh"""
    background_tasks.add_task(refresh_schema_job)
    return {"message": "Schema refresh initiated"}

@app.get("/databases", response_model=List[str])
async def list_databases(session = Depends(get_neo4j_session)):
    """Get list of all databases"""
    query = """
    MATCH (db:Database)
    RETURN db.name as name
    ORDER BY db.name
    """
    
    try:
        result = session.run(query)
        databases = [record["name"] for record in result]
        return databases
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list databases: {str(e)}")

@app.get("/databases/{database}/schemas", response_model=List[str])
async def list_schemas(database: str, session = Depends(get_neo4j_session)):
    """Get list of schemas for a specific database"""
    query = """
    MATCH (db:Database {name: $database})-[:CONTAINS]->(schema:Schema)
    RETURN schema.name as name
    ORDER BY schema.name
    """
    
    try:
        result = session.run(query, {"database": database})
        schemas = [record["name"] for record in result]
        return schemas
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {str(e)}")

@app.get("/databases/{database}/schemas/{schema}/tables", response_model=List[Dict[str, str]])
async def list_tables(database: str, schema: str, session = Depends(get_neo4j_session)):
    """Get list of tables for a specific database and schema"""
    query = """
    MATCH (db:Database {name: $database})-[:CONTAINS]->(s:Schema {name: $schema})-[:CONTAINS]->(t:Table)
    RETURN t.name as name, t.type as type
    ORDER BY t.name
    """
    
    try:
        result = session.run(query, {"database": database, "schema": schema})
        tables = [{"name": record["name"], "type": record["type"]} for record in result]
        return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {str(e)}")

@app.get("/search", response_model=List[SearchResult])
async def search_catalog(
    q: str = Query(..., description="Search query"),
    type_filter: Optional[str] = Query(None, description="Filter by type: database, schema, table, column"),
    limit: int = Query(50, le=100),
    session = Depends(get_neo4j_session)
):
    """Search across the data catalog"""
    
    # Build Cypher query with full-text search
    where_clauses = ["toLower(n.name) CONTAINS toLower($query)"]
    params = {"query": q, "limit": limit}
    
    if type_filter:
        # Map type filter to node labels
        type_mapping = {
            "database": "Database",
            "schema": "Schema", 
            "table": "Table",
            "column": "Column"
        }
        if type_filter in type_mapping:
            node_label = type_mapping[type_filter]
            query = f"""
            MATCH path = (n:{node_label})
            WHERE {' AND '.join(where_clauses)}
            WITH n, path
            LIMIT $limit
            RETURN n, labels(n) as node_type, 
                   [node in nodes(path) | node.name] as path_parts
            ORDER BY n.name
            """
        else:
            raise HTTPException(status_code=400, detail="Invalid type filter")
    else:
        query = f"""
        MATCH path = (n)
        WHERE {' AND '.join(where_clauses)}
        WITH n, path
        LIMIT $limit
        RETURN n, labels(n) as node_type,
               [node in nodes(path) | node.name] as path_parts
        ORDER BY n.name
        """
    
    try:
        result = session.run(query, params)
        
        search_results = []
        for record in result:
            node = record["n"]
            node_type = record["node_type"][0].lower()  # Primary label
            path_parts = record["path_parts"]
            
            search_results.append(SearchResult(
                id=f"{node_type}_{node['name']}",
                name=node["name"],
                type=node_type,
                path=".".join(reversed(path_parts)),
                metadata={
                    key: value for key, value in node.items()
                    if key not in ["name"]
                }
            ))
        
        return search_results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@app.get("/table/{database}/{schema}/{table}", response_model=TableDetail)
async def get_table_details(
    database: str,
    schema: str, 
    table: str,
    session = Depends(get_neo4j_session)
):
    """Get detailed information about a specific table"""
    
    query = """
    MATCH (db:Database {name: $database})-[:CONTAINS]->(s:Schema {name: $schema})-[:CONTAINS]->(t:Table {name: $table})
    OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
    OPTIONAL MATCH (t)-[fk:REFERENCES]->(ref_table:Table)
    OPTIONAL MATCH (ref_by_table:Table)-[:REFERENCES]->(t)
    OPTIONAL MATCH (dp:DataProduct)-[:SOURCES_FROM]->(t)
    
    RETURN t, 
           collect(DISTINCT c) as columns,
           collect(DISTINCT {table: ref_table.name, relationship: type(fk), columns: fk.constrained_columns}) as foreign_keys,
           collect(DISTINCT ref_by_table.name) as referenced_by,
           collect(DISTINCT dp.name) as data_products
    """
    
    try:
        result = session.run(query, {
            "database": database,
            "schema": schema, 
            "table": table
        })
        
        record = result.single()
        if not record:
            raise HTTPException(status_code=404, detail="Table not found")
        
        table_node = record["t"]
        columns = [dict(col) for col in record["columns"]]
        foreign_keys = [fk for fk in record["foreign_keys"] if fk["table"]]
        referenced_by = [{"table": ref} for ref in record["referenced_by"] if ref]
        data_products = record["data_products"]
        
        # Convert last_analyzed datetime if present
        last_analyzed = safe_get_datetime(table_node, "last_analyzed")
        
        return TableDetail(
            name=table_node["name"],
            schema=schema,
            database=database,
            type=table_node.get("type", "table"),
            row_count=table_node.get("row_count"),
            columns=columns,
            foreign_keys=foreign_keys,
            referenced_by=referenced_by,
            data_products=data_products,
            last_analyzed=last_analyzed
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get table details: {str(e)}")

@app.get("/lineage/{database}/{schema}/{table}", response_model=LineageGraph)
async def get_table_lineage(
    database: str,
    schema: str,
    table: str,
    depth: int = Query(2, ge=1, le=5, description="Lineage depth"),
    session = Depends(get_neo4j_session)
):
    """Get lineage graph for a table"""
    
    query = f"""
    MATCH (db:Database {{name: $database}})-[:CONTAINS]->(s:Schema {{name: $schema}})-[:CONTAINS]->(t:Table {{name: $table}})
    CALL apoc.path.subgraphAll(t, {{
        relationshipFilter: "REFERENCES|<REFERENCES",
        minLevel: 0,
        maxLevel: $depth
    }})
    YIELD nodes, relationships
    
    RETURN nodes, relationships
    """
    
    try:
        result = session.run(query, {
            "database": database,
            "schema": schema,
            "table": table,
            "depth": depth
        })
        
        record = result.single()
        if not record:
            # Fallback to simpler query if APOC is not available
            return await get_simple_lineage(database, schema, table, depth, session)
        
        nodes = []
        edges = []
        
        for node in record["nodes"]:
            nodes.append(LineageNode(
                id=f"{list(node.labels)[0]}_{node['name']}",
                name=node["name"],
                type=list(node.labels)[0].lower(),
                metadata=dict(node)
            ))
        
        for rel in record["relationships"]:
            edges.append(LineageEdge(
                source=f"{list(rel.start_node.labels)[0]}_{rel.start_node['name']}",
                target=f"{list(rel.end_node.labels)[0]}_{rel.end_node['name']}",
                relationship=rel.type
            ))
        
        return LineageGraph(nodes=nodes, edges=edges)
        
    except Exception as e:
        # Fallback to simple lineage if advanced query fails
        return await get_simple_lineage(database, schema, table, depth, session)

async def get_simple_lineage(database: str, schema: str, table: str, depth: int, session) -> LineageGraph:
    """Simple lineage query without APOC"""
    query = """
    MATCH (db:Database {name: $database})-[:CONTAINS]->(s:Schema {name: $schema})-[:CONTAINS]->(t:Table {name: $table})
    OPTIONAL MATCH (t)-[:REFERENCES*1..2]->(ref_table:Table)
    OPTIONAL MATCH (source_table:Table)-[:REFERENCES*1..2]->(t)
    
    RETURN t,
           collect(DISTINCT ref_table) as referenced_tables,
           collect(DISTINCT source_table) as source_tables
    """
    
    result = session.run(query, {
        "database": database,
        "schema": schema,
        "table": table
    })
    
    record = result.single()
    if not record:
        raise HTTPException(status_code=404, detail="Table not found")
    
    nodes = []
    edges = []
    
    # Central table
    central_table = record["t"]
    nodes.append(LineageNode(
        id=f"Table_{central_table['name']}",
        name=central_table["name"],
        type="table",
        metadata=dict(central_table)
    ))
    
    # Referenced tables (downstream)
    for ref_table in record["referenced_tables"]:
        if ref_table:
            nodes.append(LineageNode(
                id=f"Table_{ref_table['name']}",
                name=ref_table["name"],
                type="table",
                metadata=dict(ref_table)
            ))
            edges.append(LineageEdge(
                source=f"Table_{central_table['name']}",
                target=f"Table_{ref_table['name']}",
                relationship="REFERENCES"
            ))
    
    # Source tables (upstream)
    for source_table in record["source_tables"]:
        if source_table:
            nodes.append(LineageNode(
                id=f"Table_{source_table['name']}",
                name=source_table["name"],
                type="table", 
                metadata=dict(source_table)
            ))
            edges.append(LineageEdge(
                source=f"Table_{source_table['name']}",
                target=f"Table_{central_table['name']}",
                relationship="REFERENCES"
            ))
    
    return LineageGraph(nodes=nodes, edges=edges)

@app.post("/data-products", response_model=Dict[str, str])
async def create_data_product(
    data_product: DataProduct,
    session = Depends(get_neo4j_session)
):
    """Create a new data product"""
    
    query = """
    CREATE (dp:DataProduct {
        name: $name,
        description: $description,
        owner: $owner,
        tags: $tags,
        created_at: datetime(),
        updated_at: datetime()
    })
    WITH dp
    UNWIND $source_tables as table_path
    MATCH path = ()-[:CONTAINS*]->(t:Table)
    WHERE [node in nodes(path) | node.name] = split(table_path, '.')
    CREATE (dp)-[:SOURCES_FROM]->(t)
    
    RETURN dp.name as name
    """
    
    try:
        result = session.run(query, {
            "name": data_product.name,
            "description": data_product.description,
            "owner": data_product.owner,
            "tags": data_product.tags,
            "source_tables": data_product.source_tables
        })
        
        record = result.single()
        return {"message": f"Data product '{record['name']}' created successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create data product: {str(e)}")

@app.get("/data-products", response_model=List[DataProduct])
async def list_data_products(session = Depends(get_neo4j_session)):
    """List all data products"""
    
    query = """
    MATCH (dp:DataProduct)
    OPTIONAL MATCH (dp)-[:SOURCES_FROM]->(t:Table)
    OPTIONAL MATCH path = ()-[:CONTAINS*]->(t)
    
    RETURN dp,
           collect(DISTINCT [node in nodes(path) | node.name]) as source_table_paths
    ORDER BY dp.name
    """
    
    try:
        result = session.run(query)
        
        data_products = []
        for record in result:
            dp_node = record["dp"]
            source_tables = [".".join(reversed(path)) for path in record["source_table_paths"] if path]
            
            # Convert Neo4j DateTime to Python datetime if present
            created_at = safe_get_datetime(dp_node, "created_at")
            updated_at = safe_get_datetime(dp_node, "updated_at")
            
            data_products.append(DataProduct(
                name=dp_node["name"],
                description=dp_node["description"],
                owner=dp_node["owner"],
                tags=dp_node.get("tags", []),
                source_tables=source_tables,
                created_at=created_at,
                updated_at=updated_at
            ))
        
        return data_products
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list data products: {str(e)}")

@app.get("/stats", response_model=Dict[str, Any])
async def get_catalog_stats(session = Depends(get_neo4j_session)):
    """Get catalog statistics"""
    
    query = """
    MATCH (db:Database) WITH count(db) as database_count
    MATCH (s:Schema) WITH database_count, count(s) as schema_count
    MATCH (t:Table) WITH database_count, schema_count, count(t) as table_count
    MATCH (c:Column) WITH database_count, schema_count, table_count, count(c) as column_count
    MATCH (dp:DataProduct) WITH database_count, schema_count, table_count, column_count, count(dp) as data_product_count
    
    RETURN database_count, schema_count, table_count, column_count, data_product_count
    """
    
    try:
        result = session.run(query)
        record = result.single()
        
        if record:
            return {
                "databases": record["database_count"],
                "schemas": record["schema_count"], 
                "tables": record["table_count"],
                "columns": record["column_count"],
                "data_products": record["data_product_count"]
            }
        else:
            return {
                "databases": 0,
                "schemas": 0,
                "tables": 0, 
                "columns": 0,
                "data_products": 0
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)