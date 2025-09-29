import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import hashlib

from sqlalchemy import create_engine, MetaData, inspect, text
from sqlalchemy.engine import Engine
from neo4j import GraphDatabase
import pymssql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SchemaExtractor:
    def __init__(self, sql_server_conn_str: str, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        self.sql_engine = create_engine(sql_server_conn_str)
        self.neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        
    async def extract_full_schema(self) -> Dict[str, Any]:
        """Extract complete schema from SQL Server"""
        logger.info("Starting schema extraction...")
        
        inspector = inspect(self.sql_engine)
        schema_data = {
            'databases': [],
            'extraction_timestamp': datetime.utcnow().isoformat(),
            'schema_hash': None
        }
        
        # Get all databases (requires sysadmin rights or cross-db permissions)
        try:
            with self.sql_engine.connect() as conn:
                # Use text() wrapper for raw SQL
                databases_query = text("SELECT name FROM sys.databases WHERE database_id > 4 AND state = 0")  # Skip system DBs and offline DBs
                result = conn.execute(databases_query)
                database_names = [row[0] for row in result]
                logger.info(f"Found {len(database_names)} databases: {database_names}")
        except Exception as e:
            logger.warning(f"Could not get database list: {e}. Using current database only.")
            # Get current database name from connection string
            current_db = self.sql_engine.url.database
            if current_db:
                database_names = [current_db]
            else:
                # Try to get current database name
                try:
                    with self.sql_engine.connect() as conn:
                        result = conn.execute(text("SELECT DB_NAME()"))
                        current_db = result.scalar()
                        database_names = [current_db] if current_db else ['master']
                except:
                    database_names = ['master']
        
        # Process databases with better error handling
        for db_name in database_names:
            try:
                logger.info(f"Processing database: {db_name}")
                db_schema = await self._extract_database_schema(db_name, inspector)
                if db_schema and db_schema.get('schemas'):
                    schema_data['databases'].append(db_schema)
                    logger.info(f"Successfully processed database {db_name} with {len(db_schema.get('schemas', []))} schemas")
                else:
                    logger.warning(f"No data extracted for database: {db_name}")
            except Exception as e:
                logger.error(f"Failed to process database {db_name}: {e}")
                continue
        
        # Generate hash for change detection
        schema_data['schema_hash'] = self._generate_schema_hash(schema_data)
        logger.info(f"Schema extraction completed. Total databases processed: {len(schema_data['databases'])}")
        return schema_data
    
    async def _extract_database_schema(self, db_name: str, inspector) -> Dict[str, Any]:
        """Extract schema for a specific database"""
        logger.info(f"Extracting schema for database: {db_name}")
        
        try:
            # Create new engine for specific database
            db_engine = create_engine(
                self.sql_engine.url.set(database=db_name),
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600    # Recycle connections after 1 hour
            )
            
            # Test connection
            with db_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            db_inspector = inspect(db_engine)
            
            # Get schemas with better error handling
            try:
                schemas = db_inspector.get_schema_names()
                logger.info(f"Found {len(schemas)} schemas in database {db_name}: {schemas}")
            except Exception as e:
                logger.warning(f"Could not get schema names for {db_name}: {e}")
                schemas = ['dbo']  # Default schema
            
            db_schema = {
                'name': db_name,
                'schemas': [],
                'extraction_time': datetime.utcnow().isoformat()
            }
            
            # Process each schema
            for schema_name in schemas:
                try:
                    logger.info(f"Processing schema: {db_name}.{schema_name}")
                    schema_data = await self._extract_schema_tables(db_inspector, schema_name, db_name)
                    if schema_data and (schema_data['tables'] or schema_data['views']):  # Include schemas with tables or views
                        db_schema['schemas'].append(schema_data)
                        logger.info(f"Successfully processed schema {schema_name} with {len(schema_data['tables'])} tables and {len(schema_data['views'])} views")
                    else:
                        logger.info(f"Schema {schema_name} has no tables or views, skipping")
                except Exception as e:
                    logger.error(f"Error processing schema {db_name}.{schema_name}: {e}")
                    continue
            
            # Close the database-specific engine
            db_engine.dispose()
            
            return db_schema
            
        except Exception as e:
            logger.error(f"Error extracting database {db_name}: {e}")
            return None
    
    async def _extract_schema_tables(self, inspector, schema_name: str, db_name: str) -> Dict[str, Any]:
        """Extract tables and metadata for a schema"""
        try:
            # Use error handling for table/view discovery
            try:
                tables = inspector.get_table_names(schema=schema_name)
            except Exception as e:
                logger.warning(f"Could not get tables for {db_name}.{schema_name}: {e}")
                tables = []
            
            try:
                views = inspector.get_view_names(schema=schema_name)
            except Exception as e:
                logger.warning(f"Could not get views for {db_name}.{schema_name}: {e}")
                views = []
            
            logger.info(f"Schema {schema_name}: {len(tables)} tables, {len(views)} views")
            
            schema_data = {
                'name': schema_name,
                'tables': [],
                'views': []
            }
            
            # Process tables with progress logging
            for i, table_name in enumerate(tables):
                if i % 10 == 0:  # Log progress every 10 tables
                    logger.info(f"Processing table {i+1}/{len(tables)} in {db_name}.{schema_name}")
                
                table_data = await self._extract_table_metadata(inspector, table_name, schema_name, 'table', db_name)
                if table_data:
                    schema_data['tables'].append(table_data)
            
            # Process views
            for i, view_name in enumerate(views):
                if i % 10 == 0:  # Log progress every 10 views
                    logger.info(f"Processing view {i+1}/{len(views)} in {db_name}.{schema_name}")
                
                view_data = await self._extract_table_metadata(inspector, view_name, schema_name, 'view', db_name)
                if view_data:
                    schema_data['views'].append(view_data)
            
            return schema_data
            
        except Exception as e:
            logger.error(f"Error extracting schema tables for {db_name}.{schema_name}: {e}")
            return {
                'name': schema_name,
                'tables': [],
                'views': [],
                'error': str(e)
            }
    
    async def _extract_table_metadata(self, inspector, table_name: str, schema_name: str, object_type: str, db_name: str) -> Dict[str, Any]:
        """Extract detailed metadata for a table/view"""
        try:
            # Get basic table info with error handling
            try:
                columns = inspector.get_columns(table_name, schema=schema_name)
            except Exception as e:
                logger.warning(f"Could not get columns for {db_name}.{schema_name}.{table_name}: {e}")
                columns = []
            
            try:
                pk_constraint = inspector.get_pk_constraint(table_name, schema=schema_name)
            except Exception as e:
                logger.warning(f"Could not get PK for {db_name}.{schema_name}.{table_name}: {e}")
                pk_constraint = {}
            
            try:
                foreign_keys = inspector.get_foreign_keys(table_name, schema=schema_name)
            except Exception as e:
                logger.warning(f"Could not get FKs for {db_name}.{schema_name}.{table_name}: {e}")
                foreign_keys = []
            
            try:
                indexes = inspector.get_indexes(table_name, schema=schema_name)
            except Exception as e:
                logger.warning(f"Could not get indexes for {db_name}.{schema_name}.{table_name}: {e}")
                indexes = []
            
            # Get row count (approximation) - skip for views or if it takes too long
            row_count = None
            if object_type == 'table':
                row_count = await self._get_row_count(inspector.bind, table_name, schema_name)
            
            table_data = {
                'name': table_name,
                'schema': schema_name,
                'database': db_name,
                'type': object_type,
                'columns': [
                    {
                        'name': col['name'],
                        'type': str(col['type']),
                        'nullable': col['nullable'],
                        'default': str(col['default']) if col.get('default') else None,
                        'primary_key': col['name'] in (pk_constraint.get('constrained_columns', []) if pk_constraint else [])
                    }
                    for col in columns
                ],
                'primary_key': pk_constraint.get('constrained_columns', []) if pk_constraint else [],
                'foreign_keys': [
                    {
                        'constrained_columns': fk['constrained_columns'],
                        'referred_table': fk['referred_table'],
                        'referred_schema': fk.get('referred_schema'),
                        'referred_columns': fk['referred_columns']
                    }
                    for fk in foreign_keys
                ],
                'indexes': [
                    {
                        'name': idx.get('name'),
                        'columns': idx['column_names'],
                        'unique': idx.get('unique', False)
                    }
                    for idx in indexes
                ],
                'row_count': row_count,
                'last_analyzed': datetime.utcnow().isoformat()
            }
            
            return table_data
            
        except Exception as e:
            logger.error(f"Error extracting metadata for {db_name}.{schema_name}.{table_name}: {e}")
            return {
                'name': table_name,
                'schema': schema_name,
                'database': db_name,
                'type': object_type,
                'columns': [],
                'error': str(e)
            }
    
    async def _get_row_count(self, engine, table_name: str, schema_name: str) -> Optional[int]:
        """Get approximate row count for table using sys.dm_db_partition_stats for better performance"""
        try:
            with engine.connect() as conn:
                # Use system DMV for faster row count estimation
                query = text("""
                    SELECT SUM(row_count) 
                    FROM sys.dm_db_partition_stats ps
                    INNER JOIN sys.objects o ON ps.object_id = o.object_id
                    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                    WHERE s.name = :schema_name 
                    AND o.name = :table_name 
                    AND ps.index_id IN (0, 1)
                """)
                result = conn.execute(query, {"schema_name": schema_name, "table_name": table_name})
                row_count = result.scalar()
                return row_count if row_count is not None else 0
        except Exception as e:
            logger.debug(f"Could not get row count for {schema_name}.{table_name}: {e}")
            # Fallback to COUNT(*) but with timeout
            try:
                with engine.connect() as conn:
                    query = text(f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]")
                    # Set a timeout for the query (5 seconds)
                    result = conn.execute(query.execution_options(timeout=5))
                    return result.scalar()
            except Exception as e2:
                logger.debug(f"Fallback row count also failed for {schema_name}.{table_name}: {e2}")
                return None
    
    def _generate_schema_hash(self, schema_data: Dict[str, Any]) -> str:
        """Generate hash for change detection"""
        # Remove timestamp and hash fields for consistent hashing
        data_copy = schema_data.copy()
        data_copy.pop('extraction_timestamp', None)
        data_copy.pop('schema_hash', None)
        
        schema_str = str(sorted(data_copy.items()))
        return hashlib.sha256(schema_str.encode()).hexdigest()
    
    async def load_to_neo4j(self, schema_data: Dict[str, Any]):
        """Load extracted schema into Neo4j"""
        logger.info("Loading schema data to Neo4j...")
        
        with self.neo4j_driver.session() as session:
            # Clear existing schema data (for MVP - in production, do incremental updates)
            logger.info("Clearing existing data...")
            session.run("MATCH (n:Database) DETACH DELETE n")
            session.run("MATCH (n:Schema) DETACH DELETE n")
            session.run("MATCH (n:Table) DETACH DELETE n")
            session.run("MATCH (n:Column) DETACH DELETE n")
            
            # Load databases
            for db_data in schema_data['databases']:
                await self._load_database(session, db_data)
                logger.info(f"Loaded database: {db_data['name']}")
    
    async def _load_database(self, session, db_data: Dict[str, Any]):
        """Load database and its schemas to Neo4j"""
        # Create database node
        session.run("""
            CREATE (db:Database {
                name: $name,
                extraction_time: $extraction_time
            })
        """, name=db_data['name'], extraction_time=db_data['extraction_time'])
        
        # Load schemas
        for schema_data in db_data['schemas']:
            await self._load_schema(session, db_data['name'], schema_data)
    
    async def _load_schema(self, session, db_name: str, schema_data: Dict[str, Any]):
        """Load schema and its objects to Neo4j"""
        # Create schema node and relationship
        session.run("""
            MATCH (db:Database {name: $db_name})
            CREATE (schema:Schema {name: $schema_name})
            CREATE (db)-[:CONTAINS]->(schema)
        """, db_name=db_name, schema_name=schema_data['name'])
        
        # Load tables
        for table_data in schema_data['tables']:
            await self._load_table(session, db_name, schema_data['name'], table_data)
        
        # Load views
        for view_data in schema_data['views']:
            await self._load_table(session, db_name, schema_data['name'], view_data)
    
    async def _load_table(self, session, db_name: str, schema_name: str, table_data: Dict[str, Any]):
        """Load table/view and its columns to Neo4j"""
        # Create table/view node with proper relationship chain
        session.run("""
            MATCH (db:Database {name: $db_name})-[:CONTAINS]->(schema:Schema {name: $schema_name})
            CREATE (table:Table {
                name: $table_name,
                type: $table_type,
                row_count: $row_count,
                last_analyzed: $last_analyzed,
                database: $db_name,
                schema: $schema_name
            })
            CREATE (schema)-[:CONTAINS]->(table)
        """, 
        db_name=db_name, 
        schema_name=schema_name, 
        table_name=table_data['name'],
        table_type=table_data['type'],
        row_count=table_data.get('row_count'),
        last_analyzed=table_data.get('last_analyzed'))
        
        # Load columns
        for col_data in table_data.get('columns', []):
            session.run("""
                MATCH (db:Database {name: $db_name})-[:CONTAINS]->(schema:Schema {name: $schema_name})-[:CONTAINS]->(table:Table {name: $table_name})
                CREATE (col:Column {
                    name: $col_name,
                    type: $col_type,
                    nullable: $nullable,
                    default_value: $default_value,
                    primary_key: $primary_key
                })
                CREATE (table)-[:HAS_COLUMN]->(col)
            """,
            db_name=db_name,
            schema_name=schema_name,
            table_name=table_data['name'],
            col_name=col_data['name'],
            col_type=col_data['type'],
            nullable=col_data['nullable'],
            default_value=col_data.get('default'),
            primary_key=col_data.get('primary_key', False))
        
        # Create foreign key relationships
        for fk_data in table_data.get('foreign_keys', []):
            await self._create_foreign_key_relationship(session, db_name, schema_name, table_data['name'], fk_data)
    
    async def _create_foreign_key_relationship(self, session, db_name: str, schema_name: str, table_name: str, fk_data: Dict[str, Any]):
        """Create foreign key relationships in Neo4j"""
        ref_schema = fk_data.get('referred_schema', schema_name)
        
        # More robust query for foreign key relationships
        session.run("""
            MATCH (db:Database {name: $db_name})
            MATCH (db)-[:CONTAINS]->(source_schema:Schema {name: $schema_name})-[:CONTAINS]->(source_table:Table {name: $source_table})
            MATCH (db)-[:CONTAINS]->(target_schema:Schema {name: $ref_schema})-[:CONTAINS]->(target_table:Table {name: $target_table})
            CREATE (source_table)-[:REFERENCES {
                constrained_columns: $constrained_columns,
                referred_columns: $referred_columns
            }]->(target_table)
        """,
        db_name=db_name,
        schema_name=schema_name,
        source_table=table_name,
        ref_schema=ref_schema,
        target_table=fk_data['referred_table'],
        constrained_columns=fk_data['constrained_columns'],
        referred_columns=fk_data['referred_columns'])
    
    def close(self):
        """Close database connections"""
        if hasattr(self, 'neo4j_driver'):
            self.neo4j_driver.close()
        if hasattr(self, 'sql_engine'):
            self.sql_engine.dispose()


# Usage example
async def main():
    extractor = SchemaExtractor(
        sql_server_conn_str="mssql+pymssql://user:password@server/database",
        neo4j_uri="bolt://localhost:7687",
        neo4j_user="neo4j",
        neo4j_password="password"
    )
    
    try:
        schema_data = await extractor.extract_full_schema()
        await extractor.load_to_neo4j(schema_data)
        logger.info("Schema extraction and loading completed successfully!")
    except Exception as e:
        logger.error(f"Schema extraction failed: {e}")
    finally:
        extractor.close()

if __name__ == "__main__":
    asyncio.run(main())