import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import networkx as nx
from datetime import datetime
import json

# Page config
st.set_page_config(
    page_title="Data Catalog",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Configuration
API_BASE_URL = st.secrets.get("API_BASE_URL", "http://localhost:8000")

# Helper functions
@st.cache_data(ttl=300)  # Cache for 5 minutes
def search_catalog(query, type_filter=None, limit=50):
    """Search the data catalog"""
    params = {"q": query, "limit": limit}
    if type_filter and type_filter != "All":
        params["type_filter"] = type_filter.lower()
    
    try:
        response = requests.get(f"{API_BASE_URL}/search", params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Search failed: {e}")
        return []

@st.cache_data(ttl=300)
def get_table_details(database, schema, table):
    """Get detailed information about a table"""
    try:
        response = requests.get(f"{API_BASE_URL}/table/{database}/{schema}/{table}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to get table details: {e}")
        return None

@st.cache_data(ttl=300)
def get_table_lineage(database, schema, table, depth=2):
    """Get lineage graph for a table"""
    try:
        response = requests.get(
            f"{API_BASE_URL}/lineage/{database}/{schema}/{table}",
            params={"depth": depth}
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to get lineage: {e}")
        return None

@st.cache_data(ttl=60)
def get_catalog_stats():
    """Get catalog statistics"""
    try:
        response = requests.get(f"{API_BASE_URL}/stats")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to get stats: {e}")
        return {}

@st.cache_data(ttl=300)
def get_data_products():
    """Get list of data products"""
    try:
        response = requests.get(f"{API_BASE_URL}/data-products")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to get data products: {e}")
        return []

@st.cache_data(ttl=300)
def get_databases():
    """Get list of all databases"""
    try:
        response = requests.get(f"{API_BASE_URL}/databases")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to get databases: {e}")
        return []

@st.cache_data(ttl=300)
def get_schemas(database):
    """Get list of schemas for a database"""
    try:
        response = requests.get(f"{API_BASE_URL}/databases/{database}/schemas")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to get schemas: {e}")
        return []

@st.cache_data(ttl=300)
def get_tables(database, schema):
    """Get list of tables for a database and schema"""
    try:
        response = requests.get(f"{API_BASE_URL}/databases/{database}/schemas/{schema}/tables")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to get tables: {e}")
        return []

def create_data_product(name, description, owner, tags, source_tables):
    """Create a new data product"""
    data = {
        "name": name,
        "description": description,
        "owner": owner,
        "tags": tags,
        "source_tables": source_tables,

    }
    
    try:
        response = requests.post(f"{API_BASE_URL}/data-products", json=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to create data product: {e}")
        return None

def refresh_schema():
    """Trigger schema refresh"""
    try:
        response = requests.post(f"{API_BASE_URL}/refresh-schema")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to refresh schema: {e}")
        return None

def create_lineage_graph(lineage_data):
    """Create interactive lineage graph using Plotly"""
    if not lineage_data or not lineage_data.get("nodes"):
        return go.Figure()
    
    # Create NetworkX graph for layout calculation
    G = nx.DiGraph()
    
    # Add nodes
    for node in lineage_data["nodes"]:
        G.add_node(node["id"], **node["metadata"])
    
    # Add edges
    for edge in lineage_data["edges"]:
        G.add_edge(edge["source"], edge["target"], relationship=edge["relationship"])
    
    # Calculate layout
    pos = nx.spring_layout(G, k=3, iterations=50)
    
    # Prepare data for Plotly
    node_trace = go.Scatter(
        x=[pos[node["id"]][0] for node in lineage_data["nodes"]],
        y=[pos[node["id"]][1] for node in lineage_data["nodes"]],
        mode='markers+text',
        text=[node["name"] for node in lineage_data["nodes"]],
        textposition="middle center",
        marker=dict(
            size=[30 if node["type"] == "table" else 20 for node in lineage_data["nodes"]],
            color=[
                "lightblue" if node["type"] == "table" else 
                "lightgreen" if node["type"] == "view" else "lightgray"
                for node in lineage_data["nodes"]
            ],
            line=dict(width=2, color="black")
        ),
        hovertemplate="<b>%{text}</b><br>Type: %{customdata}<extra></extra>",
        customdata=[node["type"] for node in lineage_data["nodes"]],
        name="Tables"
    )
    
    edge_traces = []
    for edge in lineage_data["edges"]:
        x0, y0 = pos[edge["source"]]
        x1, y1 = pos[edge["target"]]
        
        edge_trace = go.Scatter(
            x=[x0, x1, None],
            y=[y0, y1, None],
            mode='lines',
            line=dict(width=2, color="gray"),
            showlegend=False,
            hoverinfo='none'
        )
        edge_traces.append(edge_trace)
    
    # Create figure
    fig = go.Figure(data=[node_trace] + edge_traces)
    fig.update_layout(
        title="Data Lineage Graph",
        showlegend=False,
        hovermode='closest',
        margin=dict(b=20,l=5,r=5,t=40),
        annotations=[ 
            dict(
                text="Click and drag nodes to explore relationships",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.005, y=-0.002,
                xanchor='left', yanchor='bottom',
                font=dict(color="#888", size=12)
            )
        ],
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor='white'
    )
    
    return fig

def refresh_incremental(force_all=False):
    """Trigger incremental schema refresh"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/refresh-schema/incremental",
            params={"force_all": force_all}
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to refresh schema: {e}")
        return None

def refresh_database(database):
    """Trigger database refresh"""
    try:
        response = requests.post(f"{API_BASE_URL}/refresh-schema/database/{database}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to refresh database: {e}")
        return None

def refresh_schema_specific(database, schema):
    """Trigger schema refresh"""
    try:
        response = requests.post(f"{API_BASE_URL}/refresh-schema/database/{database}/schema/{schema}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to refresh schema: {e}")
        return None

def refresh_table_specific(database, schema, table):
    """Trigger table refresh"""
    try:
        response = requests.post(f"{API_BASE_URL}/refresh-schema/database/{database}/schema/{schema}/table/{table}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to refresh table: {e}")
        return None

@st.cache_data(ttl=60)
def check_schema_changes(database=None, schema=None, table=None):
    """Check which tables need refresh"""
    try:
        params = {}
        if database:
            params["database"] = database
        if schema:
            params["schema"] = schema
        if table:
            params["table"] = table
        
        response = requests.get(f"{API_BASE_URL}/refresh-schema/check-changes", params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to check schema changes: {e}")
        return None

# Main application
def main():
    st.title("🗃️ Data Catalog")
    st.markdown("*Discover, explore, and manage your data ecosystem*")
    
    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a page",
        ["🏠 Dashboard", "🔍 Search", "📊 Table Details", "🌐 Data Lineage", "📦 Data Products", "⚙️ Admin"]
    )
    
    # Dashboard Page
    if page == "🏠 Dashboard":
        st.header("Dashboard")
        
        # Get stats
        stats = get_catalog_stats()
        
        if stats:
            # Metrics row
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Databases", stats.get("databases", 0))
            with col2:
                st.metric("Schemas", stats.get("schemas", 0))
            with col3:
                st.metric("Tables", stats.get("tables", 0))
            with col4:
                st.metric("Columns", stats.get("columns", 0))
            with col5:
                st.metric("Data Products", stats.get("data_products", 0))
            
            st.markdown("---")
        
        # Recent activity and quick search
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("Quick Search")
            quick_search = st.text_input("Search tables, columns, or schemas...")
            if quick_search:
                results = search_catalog(quick_search, limit=10)
                if results:
                    for result in results[:5]:
                        with st.expander(f"{result['name']} ({result['type']})"):
                            st.write(f"**Path:** {result['path']}")
                            if result.get('metadata'):
                                st.json(result['metadata'])
        
        with col2:
            st.subheader("Quick Actions")
            # NO NO NO NO NO NO NO (oh Mama Mia, Mama Mia, Mama Mia, let me go)
            # if st.button("🔄 Refresh Schema", width='stretch'):
            #     with st.spinner("Refreshing schema..."):
            #         result = refresh_schema()
            #         if result:
            #             st.success("Schema refresh initiated!")
            #             st.cache_data.clear()
            # BEELZEBUB HAS A FUNCTION PUT ASIDE FOR ME, FOR ME, FOR MEEEEE
            
            if st.button("📦 Create Data Product", width='stretch'):
                st.session_state.page = "📦 Data Products"
                st.rerun()
    
    # Search Page
    elif page == "🔍 Search":
        st.header("Search Data Catalog")
        
        col1, col2, col3 = st.columns([3, 1, 1])
        
        with col1:
            search_query = st.text_input("Search query", placeholder="Enter table name, column name, or keyword...")
        
        with col2:
            type_filter = st.selectbox(
                "Filter by type",
                ["All", "Database", "Schema", "Table", "Column"]
            )
        
        with col3:
            limit = st.number_input("Results limit", min_value=10, max_value=100, value=50)
        
        if search_query:
            with st.spinner("Searching..."):
                results = search_catalog(search_query, type_filter, limit)
            
            if results:
                st.success(f"Found {len(results)} results")
                
                # Results table
                df = pd.DataFrame([
                    {
                        "Name": r["name"],
                        "Type": r["type"].title(),
                        "Path": r["path"],
                        "Metadata": len(r.get("metadata", {}))
                    }
                    for r in results
                ])
                
                # Make table clickable
                event = st.dataframe(
                    df,
                    width='stretch',
                    on_select="rerun",
                    selection_mode="single-row"
                )
                
                # Handle row selection
                if event.selection and event.selection.rows:
                    selected_idx = event.selection.rows[0]
                    selected_result = results[selected_idx]
                    
                    if selected_result["type"] == "table":
                        path_parts = selected_result["path"].split(".")
                        if len(path_parts) >= 3:
                            st.session_state.selected_database = path_parts[0]
                            st.session_state.selected_schema = path_parts[1]
                            st.session_state.selected_table = path_parts[2]
                            st.session_state.page = "📊 Table Details"
                            st.rerun()
            else:
                st.info("No results found. Try a different search term.")
    
    # Table Details Page
    elif page == "📊 Table Details":
        st.header("Table Details")
        
        # Initialize session state for selections if not exists
        if 'selected_database' not in st.session_state:
            st.session_state.selected_database = None
        if 'selected_schema' not in st.session_state:
            st.session_state.selected_schema = None
        if 'selected_table' not in st.session_state:
            st.session_state.selected_table = None
        
        # Get list of databases
        databases = get_databases()
        
        if not databases:
            st.warning("No databases found. Please refresh the schema from the Admin page.")
            return
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Database dropdown
            database_index = 0
            if st.session_state.selected_database and st.session_state.selected_database in databases:
                database_index = databases.index(st.session_state.selected_database)
            
            selected_database = st.selectbox(
                "Database",
                options=databases,
                index=database_index,
                key="db_select"
            )
            
            # Update session state if changed
            if selected_database != st.session_state.selected_database:
                st.session_state.selected_database = selected_database
                st.session_state.selected_schema = None
                st.session_state.selected_table = None
                st.rerun()
        
        with col2:
            # Schema dropdown
            schemas = []
            schema_index = 0
            
            if selected_database:
                schemas = get_schemas(selected_database)
                
                if schemas:
                    if st.session_state.selected_schema and st.session_state.selected_schema in schemas:
                        schema_index = schemas.index(st.session_state.selected_schema)
                    
                    selected_schema = st.selectbox(
                        "Schema",
                        options=schemas,
                        index=schema_index,
                        key="schema_select"
                    )
                    
                    # Update session state if changed
                    if selected_schema != st.session_state.selected_schema:
                        st.session_state.selected_schema = selected_schema
                        st.session_state.selected_table = None
                        st.rerun()
                else:
                    st.selectbox("Schema", options=[], disabled=True)
                    selected_schema = None
            else:
                st.selectbox("Schema", options=[], disabled=True)
                selected_schema = None
        
        with col3:
            # Table dropdown
            tables = []
            table_index = 0
            
            if selected_database and selected_schema:
                tables_data = get_tables(selected_database, selected_schema)
                tables = [t["name"] for t in tables_data]
                
                if tables:
                    if st.session_state.selected_table and st.session_state.selected_table in tables:
                        table_index = tables.index(st.session_state.selected_table)
                    
                    selected_table = st.selectbox(
                        "Table",
                        options=tables,
                        index=table_index,
                        key="table_select"
                    )
                    
                    # Update session state if changed
                    if selected_table != st.session_state.selected_table:
                        st.session_state.selected_table = selected_table
                        st.rerun()
                else:
                    st.selectbox("Table", options=[], disabled=True)
                    selected_table = None
            else:
                st.selectbox("Table", options=[], disabled=True)
                selected_table = None
        
        # Display table details if all selections are made
        if selected_database and selected_schema and selected_table:
            with st.spinner("Loading table details..."):
                table_details = get_table_details(selected_database, selected_schema, selected_table)
            
            if table_details:
                # Table info
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Table Type", table_details["type"].title())
                with col2:
                    st.metric("Row Count", f"{table_details.get('row_count', 0):,}" if table_details.get('row_count') else "Unknown")
                with col3:
                    st.metric("Column Count", len(table_details.get("columns", [])))
                
                st.markdown("---")
                
                # Columns
                if table_details.get("columns"):
                    st.subheader("Columns")
                    columns_df = pd.DataFrame(table_details["columns"])
                    st.dataframe(columns_df, width='stretch')
                
                # Foreign Keys
                if table_details.get("foreign_keys"):
                    st.subheader("Foreign Key Relationships")
                    fk_df = pd.DataFrame(table_details["foreign_keys"])
                    st.dataframe(fk_df, width='stretch')
                
                # Referenced By
                if table_details.get("referenced_by"):
                    st.subheader("Referenced By")
                    ref_df = pd.DataFrame(table_details["referenced_by"])
                    st.dataframe(ref_df, width='stretch')
                
                # Data Products
                if table_details.get("data_products"):
                    st.subheader("Related Data Products")
                    for dp in table_details["data_products"]:
                        st.write(f"• {dp}")

                # Table metadata section
                if table_details.get("last_analyzed"):
                    st.markdown("---")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.markdown("**Last Analyzed:**")
                        last_analyzed = pd.to_datetime(table_details["last_analyzed"])
                        st.write(last_analyzed.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    with col2:
                        # Check if table needs refresh
                        if st.button("🔍 Check for Changes", key="check_changes_btn"):
                            with st.spinner("Checking for schema changes..."):
                                changes = check_schema_changes(
                                    database=selected_database,
                                    schema=selected_schema,
                                    table=selected_table
                                )
                                
                                if changes and changes.get("tables_needing_refresh"):
                                    table_status = changes["tables_needing_refresh"][0]
                                    if table_status["needs_refresh"]:
                                        st.warning(f"⚠️ Table has changed! {table_status['reason']}")
                                    else:
                                        st.success("✅ Table is up to date")
                                else:
                                    st.success("✅ Table is up to date")
                    
                    with col3:
                        # Refresh this table
                        if st.button("🔄 Refresh This Table", key="refresh_table_btn"):
                            with st.spinner(f"Refreshing {selected_database}.{selected_schema}.{selected_table}..."):
                                result = refresh_table_specific(selected_database, selected_schema, selected_table)
                                if result:
                                    st.success("Table refresh initiated! Reload this page in a few moments.")
                                    st.cache_data.clear()
                
                st.markdown("---")
                
                # Quick actions
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("View Lineage"):
                        st.session_state.lineage_database = selected_database
                        st.session_state.lineage_schema = selected_schema
                        st.session_state.lineage_table = selected_table
                        st.session_state.page = "🌐 Data Lineage"
                        st.rerun()
                
                with col2:
                    if st.button("Create Data Product"):
                        st.session_state.dp_source_tables = [f"{selected_database}.{selected_schema}.{selected_table}"]
                        st.session_state.page = "📦 Data Products"
                        st.rerun()
        else:
            st.info("Please select a database, schema, and table to view details.")    

    # Data Lineage Page  
    elif page == "🌐 Data Lineage":
        st.header("Data Lineage")
        
        col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
        
        with col1:
            database = st.text_input(
                "Database",
                value=st.session_state.get("lineage_database", "")
            )
        with col2:
            schema = st.text_input(
                "Schema", 
                value=st.session_state.get("lineage_schema", "")
            )
        with col3:
            table = st.text_input(
                "Table",
                value=st.session_state.get("lineage_table", "")
            )
        with col4:
            depth = st.selectbox("Depth", [1, 2, 3, 4, 5], index=1)
        
        if database and schema and table:
            with st.spinner("Loading lineage..."):
                lineage_data = get_table_lineage(database, schema, table, depth)
            
            if lineage_data and lineage_data.get("nodes"):
                # Create and display graph
                fig = create_lineage_graph(lineage_data)
                st.plotly_chart(fig, width='stretch', height=600)
                
                # Lineage summary
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Upstream Dependencies")
                    upstream = [
                        edge["source"] for edge in lineage_data.get("edges", [])
                        if edge["target"] == f"Table_{table}"
                    ]
                    if upstream:
                        for dep in upstream:
                            st.write(f"• {dep.replace('Table_', '')}")
                    else:
                        st.info("No upstream dependencies found")
                
                with col2:
                    st.subheader("Downstream Dependencies")
                    downstream = [
                        edge["target"] for edge in lineage_data.get("edges", [])
                        if edge["source"] == f"Table_{table}"
                    ]
                    if downstream:
                        for dep in downstream:
                            st.write(f"• {dep.replace('Table_', '')}")
                    else:
                        st.info("No downstream dependencies found")
            else:
                st.info("No lineage data found for this table")
    
    # Data Products Page
    elif page == "📦 Data Products":
        st.header("Data Products")
        
        tab1, tab2 = st.tabs(["📋 View Products", "➕ Create Product"])
        
        with tab1:
            data_products = get_data_products()
            
            if data_products:
                for dp in data_products:
                    with st.expander(f"📦 {dp['name']} (Owner: {dp['owner']})"):
                        st.write(f"**Description:** {dp['description']}")
                        
                        if dp.get('tags'):
                            st.write(f"**Tags:** {', '.join(dp['tags'])}")
                        
                        if dp.get('source_tables'):
                            st.write("**Source Tables:**")
                            for table in dp['source_tables']:
                                st.write(f"• {table}")
                        
                        if dp.get('created_at'):
                            st.write(f"**Created:** {dp['created_at']}")
            else:
                st.info("No data products found")
        
        with tab2:
            with st.form("create_data_product"):
                name = st.text_input("Product Name*")
                description = st.text_area("Description*")
                owner = st.text_input("Owner*")
                tags_input = st.text_input("Tags (comma-separated)")
                
                # Source tables
                st.subheader("Source Tables")
                source_tables = st.text_area(
                    "Source tables (one per line, format: database.schema.table)",
                    value="\n".join(st.session_state.get("dp_source_tables", []))
                )
                
                submitted = st.form_submit_button("Create Data Product")
                
                if submitted and name and description and owner:
                    tags = [tag.strip() for tag in tags_input.split(",") if tag.strip()]
                    tables = [table.strip() for table in source_tables.split("\n") if table.strip()]
                    
                    result = create_data_product(name, description, owner, tags, tables)
                    if result:
                        st.success(f"Data product '{name}' created successfully!")
                        st.cache_data.clear()
                        st.rerun()
    
    # Admin Page
    elif page == "⚙️ Admin":
        st.header("Administration")
        
        tab1, tab2, tab3 = st.tabs(["🔄 Schema Refresh", "📊 Change Detection", "⚙️ System"])
        
        with tab1:
            st.subheader("Schema Refresh Options")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### Full Refresh")
                st.info("Refreshes all databases, schemas, and tables. This can take a long time.")
                
                if st.button("🔄 Full Schema Refresh", type="primary", use_container_width=True):
                    with st.spinner("Initiating full schema refresh..."):
                        result = refresh_schema()
                        if result:
                            st.success("Full schema refresh initiated! This will run in the background.")
                            st.cache_data.clear()
            
            with col2:
                st.markdown("### Incremental Refresh (Smart)")
                st.info("Only refreshes tables that have changed since last analysis. Much faster!")
                
                if st.button("🔄 Incremental Refresh", type="primary", use_container_width=True):
                    with st.spinner("Initiating incremental refresh..."):
                        result = refresh_incremental(force_all=False)
                        if result:
                            st.success("Incremental refresh initiated! Only changed tables will be updated.")
                            st.cache_data.clear()
            
            st.markdown("---")
            
            # Selective refresh
            st.subheader("Selective Refresh")
            st.markdown("Refresh specific databases, schemas, or tables")
            
            refresh_type = st.radio(
                "Select what to refresh:",
                ["Database", "Schema", "Table"],
                horizontal=True
            )
            
            if refresh_type == "Database":
                databases = get_databases()
                if databases:
                    selected_db = st.selectbox("Select Database", databases)
                    
                    if st.button(f"🔄 Refresh Database: {selected_db}", use_container_width=True):
                        with st.spinner(f"Refreshing database {selected_db}..."):
                            result = refresh_database(selected_db)
                            if result:
                                st.success(result.get("message", "Database refresh initiated!"))
                                st.cache_data.clear()
            
            elif refresh_type == "Schema":
                col1, col2 = st.columns(2)
                
                with col1:
                    databases = get_databases()
                    if databases:
                        selected_db = st.selectbox("Select Database", databases, key="schema_refresh_db")
                
                with col2:
                    if selected_db:
                        schemas = get_schemas(selected_db)
                        if schemas:
                            selected_schema = st.selectbox("Select Schema", schemas)
                        else:
                            st.warning("No schemas found")
                            selected_schema = None
                    else:
                        selected_schema = None
                
                if selected_db and selected_schema:
                    if st.button(f"🔄 Refresh Schema: {selected_db}.{selected_schema}", use_container_width=True):
                        with st.spinner(f"Refreshing schema {selected_db}.{selected_schema}..."):
                            result = refresh_schema_specific(selected_db, selected_schema)
                            if result:
                                st.success(result.get("message", "Schema refresh initiated!"))
                                st.cache_data.clear()
            
            elif refresh_type == "Table":
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    databases = get_databases()
                    if databases:
                        selected_db = st.selectbox("Select Database", databases, key="table_refresh_db")
                
                with col2:
                    if selected_db:
                        schemas = get_schemas(selected_db)
                        if schemas:
                            selected_schema = st.selectbox("Select Schema", schemas, key="table_refresh_schema")
                        else:
                            st.warning("No schemas found")
                            selected_schema = None
                    else:
                        selected_schema = None
                
                with col3:
                    if selected_db and selected_schema:
                        tables_data = get_tables(selected_db, selected_schema)
                        if tables_data:
                            tables = [t["name"] for t in tables_data]
                            selected_table = st.selectbox("Select Table", tables)
                        else:
                            st.warning("No tables found")
                            selected_table = None
                    else:
                        selected_table = None
                
                if selected_db and selected_schema and selected_table:
                    if st.button(f"🔄 Refresh Table: {selected_db}.{selected_schema}.{selected_table}", use_container_width=True):
                        with st.spinner(f"Refreshing table {selected_db}.{selected_schema}.{selected_table}..."):
                            result = refresh_table_specific(selected_db, selected_schema, selected_table)
                            if result:
                                st.success(result.get("message", "Table refresh initiated!"))
                                st.cache_data.clear()
        
        with tab2:
            st.subheader("Schema Change Detection")
            st.markdown("Check which tables have changed since last analysis")
            
            # Filter options
            col1, col2, col3 = st.columns(3)
            
            with col1:
                databases = get_databases()
                filter_db = st.selectbox("Filter by Database", ["All"] + databases, key="change_detect_db")
                if filter_db == "All":
                    filter_db = None
            
            with col2:
                if filter_db:
                    schemas = get_schemas(filter_db)
                    filter_schema = st.selectbox("Filter by Schema", ["All"] + schemas, key="change_detect_schema")
                    if filter_schema == "All":
                        filter_schema = None
                else:
                    filter_schema = None
                    st.selectbox("Filter by Schema", ["All"], disabled=True)
            
            with col3:
                if filter_db and filter_schema:
                    tables_data = get_tables(filter_db, filter_schema)
                    tables = [t["name"] for t in tables_data]
                    filter_table = st.selectbox("Filter by Table", ["All"] + tables, key="change_detect_table")
                    if filter_table == "All":
                        filter_table = None
                else:
                    filter_table = None
                    st.selectbox("Filter by Table", ["All"], disabled=True)
            
            if st.button("🔍 Check for Changes", use_container_width=True):
                with st.spinner("Checking for schema changes..."):
                    changes = check_schema_changes(
                        database=filter_db,
                        schema=filter_schema,
                        table=filter_table
                    )
                
                if changes:
                    # Summary metrics
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Total Tables", changes["total_tables"])
                    with col2:
                        st.metric("Need Refresh", changes["needs_refresh"], 
                                 delta=f"{changes['needs_refresh']} changes")
                    with col3:
                        st.metric("Up to Date", changes["up_to_date"])
                    
                    st.markdown("---")
                    
                    # Tables needing refresh
                    if changes["tables_needing_refresh"]:
                        st.subheader("⚠️ Tables Needing Refresh")
                        
                        needs_refresh_df = pd.DataFrame(changes["tables_needing_refresh"])
                        
                        # Format datetime columns
                        for col in ["last_analyzed", "last_modified"]:
                            if col in needs_refresh_df.columns:
                                needs_refresh_df[col] = pd.to_datetime(needs_refresh_df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
                        
                        st.dataframe(
                            needs_refresh_df[[
                                "database", "schema", "table", 
                                "last_analyzed", "last_modified", "reason"
                            ]],
                            use_container_width=True
                        )
                        
                        # Bulk refresh option
                        if st.button("🔄 Refresh All Tables Shown Above", type="primary", use_container_width=True):
                            with st.spinner("Initiating refresh for changed tables..."):
                                result = refresh_incremental(force_all=False)
                                if result:
                                    st.success("Refresh initiated for all changed tables!")
                                    st.cache_data.clear()
                    else:
                        st.success("✅ All tables are up to date!")
                    
                    # Show up-to-date tables in expander
                    if changes["tables_up_to_date"]:
                        with st.expander(f"✅ Up-to-Date Tables ({len(changes['tables_up_to_date'])})"):
                            up_to_date_df = pd.DataFrame(changes["tables_up_to_date"])
                            
                            # Format datetime columns
                            for col in ["last_analyzed", "last_modified"]:
                                if col in up_to_date_df.columns:
                                    up_to_date_df[col] = pd.to_datetime(up_to_date_df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
                            
                            st.dataframe(
                                up_to_date_df[[
                                    "database", "schema", "table", 
                                    "last_analyzed", "last_modified"
                                ]],
                                use_container_width=True
                            )
        
        with tab3:
            st.subheader("System Management")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### Scheduled Refresh")
                st.info("Schema is automatically refreshed daily at 2:00 AM (incremental refresh)")
                
                if st.button("⏱️ View Schedule"):
                    st.code("""
Scheduled Jobs:
- Incremental Refresh: Daily at 2:00 AM
- Full Refresh: Manual only
                    """)
            
            with col2:
                st.markdown("### Cache Management")
                st.info("Clear cached data to force fresh queries")
                
                if st.button("🗑️ Clear Cache", use_container_width=True):
                    st.cache_data.clear()
                    st.success("Cache cleared!")
            
            st.markdown("---")
            
            # System status
            st.subheader("System Status")
            
            col1, col2 = st.columns(2)
            
            with col1:
                try:
                    response = requests.get(f"{API_BASE_URL}/health")
                    if response.status_code == 200:
                        st.success("✅ API is healthy")
                        health_data = response.json()
                        st.json(health_data)
                    else:
                        st.error("❌ API is not responding correctly")
                except:
                    st.error("❌ Cannot connect to API")
            
            with col2:
                stats = get_catalog_stats()
                if stats:
                    st.markdown("**Catalog Statistics:**")
                    for key, value in stats.items():
                        st.metric(key.replace("_", " ").title(), value)

if __name__ == "__main__":
    main()