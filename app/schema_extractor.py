import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
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
        
    async def get_table_last_modified(self, database: str, schema: str, table: str) -> Optional[datetime]:
        """Get the last modification time of a table from SQL Server system tables"""
        try:
            db_engine = create_engine(self.sql_engine.url.set(database=database))
            
            with db_engine.connect() as conn:
                # Query sys.objects and sys.tables to get modify_date
                query = text("""
                    SELECT 
                        COALESCE(t.modify_date, o.modify_date) as last_modified
                    FROM sys.objects o
                    LEFT JOIN sys.tables t ON o.object_id = t.object_id
                    WHERE o.name = :table_name 
                    AND SCHEMA_NAME(o.schema_id) = :schema_name
                    AND o.type IN ('U', 'V')  -- U = User table, V = View
                """)
                
                result = conn.execute(query, {"table_name": table, "schema_name": schema})
                row = result.fetchone()
                
                if row and row[0]:
                    return row[0]
                
                return None
                
        except Exception as e:
            logger.warning(f"Could not get last modified time for {database}.{schema}.{table}: {e}")
            return None
    
    async def get_tables_needing_refresh(self, database: str = None, schema: str = None, table: str = None) -> List[Dict[str, str]]:
        """
        Get list of tables that need refresh based on modification time
        Can filter by database, schema, or table
        """
        with self.neo4j_driver.session() as session:
            # Build query based on filters
            where_clauses = []
            params = {}
            
            if database:
                where_clauses.append("db.name = $database")
                params["database"] = database
            if schema:
                where_clauses.append("s.name = $schema")
                params["schema"] = schema
            if table:
                where_clauses.append("t.name = $table")
                params["table"] = table
            
            where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            query = f"""
            MATCH (db:Database)-[:CONTAINS]->(s:Schema)-[:CONTAINS]->(t:Table)
            {where_clause}
            RETURN db.name as database, s.name as schema, t.name as table, 
                   t.last_analyzed as last_analyzed
            ORDER BY db.name, s.name, t.name
            """
            
            result = session.run(query, params)
            tables_to_check = []
            
            for record in result:
                db_name = record["database"]
                schema_name = record["schema"]
                table_name = record["table"]
                last_analyzed = record["last_analyzed"]
                
                # Convert Neo4j datetime to Python datetime if needed
                if last_analyzed and hasattr(last_analyzed, 'to_native'):
                    last_analyzed = last_analyzed.to_native()
                
                # Get last modified time from SQL Server
                last_modified = await self.get_table_last_modified(db_name, schema_name, table_name)
                
                # Determine if refresh is needed
                needs_refresh = False
                if last_modified is None:
                    # Can't determine, so refresh to be safe
                    needs_refresh = True
                    reason = "Unable to determine last modification time"
                elif last_analyzed is None:
                    # Never analyzed before
                    needs_refresh = True
                    reason = "Never analyzed"
                elif last_modified > last_analyzed:
                    # Modified since last analysis
                    needs_refresh = True
                    reason = f"Modified {last_modified} > Last analyzed {last_analyzed}"
                else:
                    reason = "Up to date"
                
                tables_to_check.append({
                    "database": db_name,
                    "schema": schema_name,
                    "table": table_name,
                    "last_analyzed": last_analyzed,
                    "last_modified": last_modified,
                    "needs_refresh": needs_refresh,
                    "reason": reason
                })
            
            return tables_to_check
    
    async def refresh_specific_table(self, database: str, schema: str, table: str) -> Dict[str, Any]:
        """Refresh metadata for a specific table"""
        logger.info(f"Refreshing table: {database}.{schema}.{table}")
        
        try:
            # Create engine for specific database
            db_engine = create_engine(self.sql_engine.url.set(database=database))
            db_inspector = inspect(db_engine)
            
            # Extract table metadata
            table_data = await self._extract_table_metadata(db_inspector, table, schema, 'table')
            
            # Update in Neo4j
            with self.neo4j_driver.session() as session:
                # Delete existing table and its relationships
                session.run("""
                    MATCH (db:Database {name: $database})-[:CONTAINS]->(s:Schema {name: $schema})-[:CONTAINS]->(t:Table {name: $table})
                    DETACH DELETE t
                """, database=database, schema=schema, table=table)
                
                # Recreate table with updated metadata
                await self._load_table(session, database, schema, table_data)
            
            logger.info(f"Successfully refreshed {database}.{schema}.{table}")
            return {
                "status": "success",
                "database": database,
                "schema": schema,
                "table": table,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error refreshing {database}.{schema}.{table}: {e}")
            return {
                "status": "error",
                "database": database,
                "schema": schema,
                "table": table,
                "error": str(e)
            }
    
    async def refresh_specific_schema(self, database: str, schema: str) -> Dict[str, Any]:
        """Refresh all tables in a specific schema"""
        logger.info(f"Refreshing schema: {database}.{schema}")
        
        try:
            db_engine = create_engine(self.sql_engine.url.set(database=database))
            db_inspector = inspect(db_engine)
            
            tables = db_inspector.get_table_names(schema=schema)
            views = db_inspector.get_view_names(schema=schema)
            
            refreshed_tables = []
            errors = []
            
            # Process tables
            for table_name in tables:
                result = await self.refresh_specific_table(database, schema, table_name)
                if result["status"] == "success":
                    refreshed_tables.append(table_name)
                else:
                    errors.append(result)
            
            # Process views
            for view_name in views:
                result = await self.refresh_specific_table(database, schema, view_name)
                if result["status"] == "success":
                    refreshed_tables.append(view_name)
                else:
                    errors.append(result)
            
            return {
                "status": "success" if not errors else "partial",
                "database": database,
                "schema": schema,
                "refreshed_tables": refreshed_tables,
                "errors": errors,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error refreshing schema {database}.{schema}: {e}")
            return {
                "status": "error",
                "database": database,
                "schema": schema,
                "error": str(e)
            }
    
    async def refresh_specific_database(self, database: str) -> Dict[str, Any]:
        """Refresh all schemas and tables in a specific database"""
        logger.info(f"Refreshing database: {database}")
        
        try:
            db_engine = create_engine(self.sql_engine.url.set(database=database))
            db_inspector = inspect(db_engine)
            
            schemas = db_inspector.get_schema_names()
            refreshed_schemas = []
            errors = []
            
            for schema_name in schemas:
                result = await self.refresh_specific_schema(database, schema_name)
                if result["status"] in ["success", "partial"]:
                    refreshed_schemas.append(schema_name)
                if result.get("errors"):
                    errors.extend(result["errors"])
            
            return {
                "status": "success" if not errors else "partial",
                "database": database,
                "refreshed_schemas": refreshed_schemas,
                "errors": errors,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error refreshing database {database}: {e}")
            return {
                "status": "error",
                "database": database,
                "error": str(e)
            }
    
    async def incremental_refresh(self, force_all: bool = False) -> Dict[str, Any]:
        """
        Perform incremental refresh - only update tables that have changed
        
        Args:
            force_all: If True, refresh all tables regardless of modification time
        """
        logger.info("Starting incremental schema refresh...")
        
        if force_all:
            logger.info("Force refresh enabled - will refresh all tables")
            # Use existing full refresh logic
            return await self.extract_full_schema()
        
        # Get tables that need refresh
        tables_to_refresh = await self.get_tables_needing_refresh()
        
        # Filter to only tables that need refresh
        tables_needing_update = [t for t in tables_to_refresh if t["needs_refresh"]]
        
        logger.info(f"Found {len(tables_needing_update)} tables needing refresh out of {len(tables_to_refresh)} total")
        
        refreshed = []
        errors = []
        
        for table_info in tables_needing_update:
            result = await self.refresh_specific_table(
                table_info["database"],
                table_info["schema"],
                table_info["table"]
            )
            
            if result["status"] == "success":
                refreshed.append(f"{table_info['database']}.{table_info['schema']}.{table_info['table']}")
            else:
                errors.append(result)
        
        return {
            "status": "success" if not errors else "partial",
            "total_tables_checked": len(tables_to_refresh),
            "tables_needing_refresh": len(tables_needing_update),
            "tables_refreshed": len(refreshed),
            "refreshed_tables": refreshed,
            "errors": errors,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def extract_full_schema(self) -> Dict[str, Any]:
        """Extract complete schema from SQL Server"""
        logger.info("Starting full schema extraction...")
        
        inspector = inspect(self.sql_engine)
        schema_data = {
            'databases': [],
            'extraction_timestamp': datetime.utcnow().isoformat(),
            'schema_hash': None
        }
        
        # Get all databases (requires sysadmin rights or cross-db permissions)
        try:
            with self.sql_engine.connect() as conn:
                databases_query = text("SELECT name FROM sys.databases WHERE database_id > 4")  # Skip system DBs
                result = conn.execute(databases_query)
                database_names = [row[0] for row in result]
        except Exception as e:
            logger.warning(f"Could not get database list: {e}. Using current database only.")
            database_names = [self.sql_engine.url.database or 'master']
        
        for db_name in database_names:
            db_schema = await self._extract_database_schema(db_name, inspector)
            if db_schema:
                schema_data['databases'].append(db_schema)
        
        # Generate hash for change detection
        schema_data['schema_hash'] = self._generate_schema_hash(schema_data)
        return schema_data
    
    async def _extract_database_schema(self, db_name: str, inspector) -> Dict[str, Any]:
        """Extract schema for a specific database"""
        logger.info(f"Extracting schema for database: {db_name}")
        
        try:
            # Switch to specific database
            db_engine = create_engine(
                self.sql_engine.url.set(database=db_name)
            )
            db_inspector = inspect(db_engine)
            
            schemas = db_inspector.get_schema_names()
            db_schema = {
                'name': db_name,
                'schemas': [],
                'extraction_time': datetime.utcnow().isoformat()
            }
            
            for schema_name in schemas:
                schema_data = await self._extract_schema_tables(db_inspector, schema_name)
                if schema_data['tables']:  # Only include schemas with tables
                    db_schema['schemas'].append(schema_data)
            
            return db_schema
            
        except Exception as e:
            logger.error(f"Error extracting database {db_name}: {e}")
            return None
    
    async def _extract_schema_tables(self, inspector, schema_name: str) -> Dict[str, Any]:
        """Extract tables and metadata for a schema"""
        tables = inspector.get_table_names(schema=schema_name)
        views = inspector.get_view_names(schema=schema_name)
        
        schema_data = {
            'name': schema_name,
            'tables': [],
            'views': []
        }
        
        # Process tables
        for table_name in tables:
            table_data = await self._extract_table_metadata(inspector, table_name, schema_name, 'table')
            schema_data['tables'].append(table_data)
        
        # Process views
        for view_name in views:
            view_data = await self._extract_table_metadata(inspector, view_name, schema_name, 'view')
            schema_data['views'].append(view_data)
        
        return schema_data
    
    async def _extract_table_metadata(self, inspector, table_name: str, schema_name: str, object_type: str) -> Dict[str, Any]:
        """Extract detailed metadata for a table/view"""
        try:
            columns = inspector.get_columns(table_name, schema=schema_name)
            pk_constraint = inspector.get_pk_constraint(table_name, schema=schema_name)
            foreign_keys = inspector.get_foreign_keys(table_name, schema=schema_name)
            indexes = inspector.get_indexes(table_name, schema=schema_name)
            
            # Get row count (approximation)
            row_count = await self._get_row_count(inspector.bind, table_name, schema_name)
            
            table_data = {
                'name': table_name,
                'schema': schema_name,
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
            logger.error(f"Error extracting metadata for {schema_name}.{table_name}: {e}")
            return {
                'name': table_name,
                'schema': schema_name,
                'type': object_type,
                'columns': [],
                'error': str(e)
            }
    
    async def _get_row_count(self, engine, table_name: str, schema_name: str) -> Optional[int]:
        """Get approximate row count for table"""
        try:
            with engine.connect() as conn:
                query = text(f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]")
                result = conn.execute(query)
                return result.scalar()
        except Exception as e:
            logger.debug(f"Could not get row count for {schema_name}.{table_name}: {e}")
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
            session.run("MATCH (n:Database)-[*]-() DETACH DELETE n")
            
            # Load databases
            for db_data in schema_data['databases']:
                await self._load_database(session, db_data)
    
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
        # Create schema node
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
        # Create table/view node
        session.run("""
            MATCH (schema:Schema {name: $schema_name})<-[:CONTAINS]-(db:Database {name: $db_name})
            CREATE (table:Table {
                name: $table_name,
                type: $table_type,
                row_count: $row_count,
                last_analyzed: datetime($last_analyzed)
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
                MATCH (table:Table {name: $table_name})<-[:CONTAINS]-(schema:Schema {name: $schema_name})<-[:CONTAINS]-(db:Database {name: $db_name})
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
        
        session.run("""
            MATCH (source_table:Table {name: $source_table})<-[:CONTAINS]-(source_schema:Schema {name: $source_schema})<-[:CONTAINS]-(db:Database {name: $db_name})
            MATCH (target_table:Table {name: $target_table})<-[:CONTAINS]-(target_schema:Schema {name: $ref_schema})<-[:CONTAINS]-(db)
            CREATE (source_table)-[:REFERENCES {
                constrained_columns: $constrained_columns,
                referred_columns: $referred_columns
            }]->(target_table)
        """,
        db_name=db_name,
        source_schema=schema_name,
        source_table=table_name,
        target_table=fk_data['referred_table'],
        ref_schema=ref_schema,
        constrained_columns=fk_data['constrained_columns'],
        referred_columns=fk_data['referred_columns'])
    
    def close(self):
        """Close database connections"""
        if hasattr(self, 'neo4j_driver'):
            self.neo4j_driver.close()


# Usage example
async def main():
    extractor = SchemaExtractor(
        sql_server_conn_str="mssql+pymssql://user:password@server/database",
        neo4j_uri="bolt://localhost:7687",
        neo4j_user="neo4j",
        neo4j_password="password"
    )
    
    try:
        # Option 1: Incremental refresh (smart)
        result = await extractor.incremental_refresh()
        logger.info(f"Incremental refresh result: {result}")
        
        # Option 2: Full refresh (force all)
        # schema_data = await extractor.extract_full_schema()
        # await extractor.load_to_neo4j(schema_data)
        
        # Option 3: Refresh specific table
        # result = await extractor.refresh_specific_table("MyDB", "dbo", "MyTable")
        
        logger.info("Schema extraction completed successfully!")
    except Exception as e:
        logger.error(f"Schema extraction failed: {e}")
    finally:
        extractor.close()

if __name__ == "__main__":
    asyncio.run(main())