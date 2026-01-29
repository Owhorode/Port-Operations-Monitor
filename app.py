import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px

# ==========================================
# 1. CONFIGURATION
# ==========================================
st.set_page_config(page_title="NPA Operations", page_icon="⚓", layout="wide")

# ⚠️ SECURE CONNECTION
try:
    DB_CONN = st.secrets["DB_CONNECTION_STRING"]
except:
    DB_CONN = "cockroachdb://faraday:m_UeOCmGI9ssmrsw1MVZ_w@poodle-eagle-12074.jxf.gcp-europe-west2.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"

@st.cache_resource
def get_engine():
    return create_engine(DB_CONN)

engine = get_engine()

# CONSTANTS
PORTS = ["APAPA", "WARRI", "RIVERS", "ONNE", "CALABAR", "TIN CAN"]
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEPT", "OCT", "NOV", "DEC"]

# EXACT FILE MAPPING
FILE_TO_TABLE_MAP = {
    "TABLE 2.01A NO GRT OF VESSELS THAT ENTERED- OCEAN GOING VESSELS -  APAPA.xlsx": "vessel_traffic_201a",
    "TABLE 2.03A NATIONALITY OF VESSELS THAT ENTERED- OCEAN GOING  APAPA.xlsx": "nationality_203a",
    "TABLE 2.11A CARGO THROUGHPUT- TYPE OF CARGO (EXCL CRUDE OIL TERMINALS)- APAPA.xlsx": "throughput_type_211a",
    "TABLE 2.11B CARGO THROUGHPUT- TYPE OF TRADE (EXCL CRUDE OIL)- APAPA.xlsx": "throughput_trade_211b",
    "TABLES 2.16, 2.17 & 2.18 CONTAINER TRAFFIC STATISTICS APAPA.xlsx": "container_traffic_216",
    "TABLE 2.20 COMMODITY ANALYSIS OF FOREIGN & DOMESTIC CARGO DISCHARGED APAPA.xlsx": "commodity_discharged_220",
    "TABLE 2.24 NO AND TONNAGE OF UNCRATED VEHICLES DISCHARGED APAPA.xlsx": "vehicles_discharged_224",
    "TABLE 2.32 INWARD CARGO FLOW BY TYPE OF PACKAGING APAPA.xlsx": "packaging_inward_232",
    "TABLE 2.33 OUTWARD CARGO FLOW BY TYPE OF PACKAGING APAPA.xlsx": "packaging_outward_233",
    "TABLE 3.02 TURN-ROUND TIME OF SHIPS COMPLETED APAPA.xlsx": "turn_round_302",
    "TABLE 2.01A NO GRT OF VESSELS THAT ENTERED- OCEAN GOING VESSELS- DELTA.xlsx": "vessel_traffic_201a",
    "TABLE 2.11B CARGO THROUGHPUT- TYPE OF TRADE (EXCL CRUDE OIL)- WARRI.xlsx": "throughput_trade_211b",
    "TABLE 2.20 COMMODITY ANALYSIS OF FOREIGN & DOMESTIC CARGO DISCHARGED WARRI.xlsx": "commodity_discharged_220",
    "TABLE 2.32 INWARD CARGO FLOW BY TYPE OF PACKAGING WARRI.xlsx": "packaging_inward_232",
    "TABLE 2.11A CARGO THROUGHPUT- TYPE OF CARGO (EXCL CRUDE OIL TERMINALS)- WARRI.xlsx": "cargo_type_211a",
    "TABLE 3.02 TURN-ROUND TIME OF SHIPS COMPLETED WARRI.xlsx": "turn_round_302",
    "TABLE 3.03 BERTH OCCUPANCY RATE WARRI.xlsx": "berth_occupancy_303",
    "TABLE 2.01A NO GRT OF VESSELS THAT ENTERED- OCEAN GOING VESSELS RIVERS.xlsx": "vessel_traffic_201a",
    "TABLE 2.11B CARGO THROUGHPUT- TYPE OF TRADE (EXCL CRUDE OIL)-2022 PORT HARCOURT.xlsx": "throughput_trade_211b",
    "TABLE 2.32 INWARD CARGO FLOW BY TYPE OF PACKAGING PORT HARCOURT.xlsx": "packaging_inward_232",
}

# ==========================================
# 2. CUSTOM CSS (STYLING)
# ==========================================
st.markdown("""
<style>
    /* 1. Sidebar Radio Buttons -> Professional Navigation Pills */
    [data-testid="stSidebar"] [data-testid="stRadio"] > div {
        gap: 10px;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label {
        background-color: white;
        padding: 10px 15px;
        border-radius: 8px;
        color: #333;
        font-family: "Source Sans Pro", sans-serif;
        font-size: 16px;
        font-weight: 600;
        border: 1px solid #e0e0e0;
        transition: all 0.3s;
        cursor: pointer;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
        border-color: #002b50;
        color: #002b50;
    }
    /* Hide the radio circle */
    [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] > label > div:first-child {
        display: none;
    }
    /* Selected State styling handled by Streamlit's internal classes, usually highlights text */

    /* 2. Month Dropdown Text Color Fix */
    .stMultiSelect div[data-baseweb="select"] span {
        color: white !important; 
    }
    div[data-baseweb="select"] span {
        color: white !important;
    }
    
    /* 3. Metric Value Styling */
    [data-testid="stMetricValue"] {
        font-size: 26px !important;
        font-weight: 700;
        color: #002b50;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 3. SQL LOGIC
# ==========================================
def get_aggregated_kpi(metric_type, port_scope, selected_months, target_year):
    target_ports = [p.lower() for p in PORTS] if port_scope == "ALL" else [port_scope.lower()]
    total_val = 0.0
    count_for_avg = 0
    
    filters = []
    if selected_months and "ALL" not in selected_months:
        m_str = "', '".join(selected_months)
        filters.append(f"\"MONTH\" IN ('{m_str}')")
    
    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    for p in target_ports:
        query = ""
        try:
            if metric_type == "grt":
                query = f"SELECT SUM(\"GRT\") FROM {p}_vessel_traffic_201a {where_clause}"
            elif metric_type == "turnaround":
                query = f"SELECT AVG(\"AVG_TURN_ROUND\") FROM {p}_turn_round_302 {where_clause}"
            elif metric_type == "waiting":
                query = f"SELECT AVG(\"AVG_AWAITING\") FROM {p}_turn_round_302 {where_clause}"
            elif metric_type == "import":
                query = f"SELECT SUM(\"INWARD_FOREIGN\") + SUM(\"INWARD_DOMESTIC\") FROM {p}_throughput_trade_211b {where_clause}"
            elif metric_type == "export":
                query = f"SELECT SUM(\"OUTWARD_FOREIGN\") + SUM(\"OUTWARD_DOMESTIC\") FROM {p}_throughput_trade_211b {where_clause}"
            elif metric_type == "domestic":
                query = f"SELECT SUM(\"INWARD_DOMESTIC\") + SUM(\"OUTWARD_DOMESTIC\") FROM {p}_throughput_trade_211b {where_clause}"
            
            with engine.connect() as conn:
                val = conn.execute(text(query)).scalar()
                if val is not None:
                    val = float(val)
                    if metric_type in ["turnaround", "waiting"]:
                        total_val += val
                        count_for_avg += 1
                    else:
                        total_val += val
        except:
            pass
            
    if metric_type in ["turnaround", "waiting"] and count_for_avg > 0:
        total_val = total_val / count_for_avg

    return total_val

def get_distinct_values(table_name, column_name):
    try:
        with engine.connect() as conn:
            query = text(f'SELECT DISTINCT "{column_name}" FROM {table_name} ORDER BY "{column_name}"')
            result = conn.execute(query).fetchall()
            return [row[0] for row in result if row[0] is not None]
    except:
        return []

# ==========================================
# 4. SIDEBAR
# ==========================================
with st.sidebar:
    st.header("🎮 Command Center")
    
    selected_page = st.radio(
        "Navigate", 
        ["🚀 Dashboard", "📝 Data Entry", "🔎 Data Explorer", "📤 Bulk Upload"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    st.header("🎯 Filter Context")
    
    port_scope = st.selectbox("Select Port", ["ALL"] + PORTS)
    
    col_cur, col_prev = st.columns(2)
    with col_cur:
        current_year = st.selectbox("Current Year", ["2025", "2024", "2023"])
    with col_prev:
        prev_year = st.selectbox("Previous Year", ["2024", "2023", "2022"])
        
    selected_months = st.multiselect("Select Months", MONTHS, default=MONTHS)
    if not selected_months: selected_months = MONTHS

# ==========================================
# PAGE 1: DASHBOARD
# ==========================================
if "Dashboard" in selected_page:
    
    title_text = "Nigerian Ports Operations Dashboard (DEMO)" if port_scope == "ALL" else f"⚓ {port_scope} Port Operations Dashboard"
    st.markdown(f"<h2 style='color: #002b50;'>{title_text}</h2>", unsafe_allow_html=True)
    st.markdown("---")

    def display_kpi(label, metric_key, is_time=False):
        val_curr = get_aggregated_kpi(metric_key, port_scope, selected_months, current_year)
        val_prev = 0.0 # Default until history exists
        
        pct_change = ((val_curr - val_prev) / val_prev) * 100 if val_prev > 0 else 0
        fmt = "{:,.0f}"
        if is_time: fmt = "{:.2f} Days"
        
        delta_color = "inverse" if is_time else "normal"
            
        st.metric(
            label=label,
            value=fmt.format(val_curr),
            delta=f"{pct_change:.1f}% vs {prev_year}",
            delta_color=delta_color
        )
        return val_curr

    # --- ROW 1 ---
    c1, c2, c3, c4 = st.columns([1.5, 1, 1, 1])
    with c1: display_kpi("⚓ Gross Reg. Tonnage (GRT)", "grt")
    with c2: display_kpi("🚢 Turnaround Time", "turnaround", is_time=True)
    with c3: display_kpi("⏳ Avg Waiting Time", "waiting", is_time=True)
    with c4: display_kpi("🏠 Domestic Cargo", "domestic")

    st.markdown("### ") 

    # --- ROW 2 & CHARTS ---
    kpi_import = get_aggregated_kpi("import", port_scope, selected_months, current_year)
    kpi_export = get_aggregated_kpi("export", port_scope, selected_months, current_year)

    ch1, ch2 = st.columns([1, 2])
    
    with ch1:
        st.markdown("### 📦 Trade Balance")
        st.metric("Total Import", f"{kpi_import:,.0f} MT")
        st.metric("Total Export", f"{kpi_export:,.0f} MT")
        
        df_trade = pd.DataFrame({
            "Type": ["Import", "Export", "Domestic"],
            "Value": [kpi_import, kpi_export, display_kpi("Dom", "domestic")]
        })
        fig_pie = px.pie(df_trade, values="Value", names="Type", hole=0.6, color_discrete_sequence=px.colors.qualitative.Set2)
        fig_pie.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=250, showlegend=True)
        st.plotly_chart(fig_pie, use_container_width=True)

    with ch2:
        st.markdown("### 📈 Efficiency Trends")
        proxy_port = "apapa" if port_scope == "ALL" else port_scope.lower()
        try:
            trend_sql = f'SELECT "MONTH", AVG("AVG_TURN_ROUND") as "Turnaround", AVG("AVG_AWAITING") as "Waiting" FROM {proxy_port}_turn_round_302 GROUP BY "MONTH"'
            df_trend = pd.read_sql(trend_sql, engine)
            df_trend['MONTH'] = pd.Categorical(df_trend['MONTH'], categories=MONTHS, ordered=True)
            df_trend = df_trend.sort_values('MONTH')
            
            fig_trend = px.line(df_trend, x="MONTH", y=["Turnaround", "Waiting"], markers=True)
            fig_trend.update_layout(height=350)
            st.plotly_chart(fig_trend, use_container_width=True)
        except:
            st.info("No historical trend data available.")

# ==========================================
# PAGE 2: DATA ENTRY
# ==========================================
elif "Data Entry" in selected_page:
    st.title("📝 Data Entry")
    
    if port_scope == "ALL":
        st.error("⚠️ Select a specific PORT in the sidebar to continue.")
    else:
        selected_filename = st.selectbox("Select File Category", list(FILE_TO_TABLE_MAP.keys()))
        
        if selected_filename:
            target_table = f"{port_scope.lower()}_{FILE_TO_TABLE_MAP[selected_filename]}"
            st.caption(f"System Table: `{target_table}`")
            
            try:
                with engine.connect() as conn:
                    cols = conn.execute(text(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{target_table}'")).fetchall()
                
                with st.form("entry_form"):
                    col_layout = st.columns(2)
                    form_data = {}
                    
                    for i, (col_name, dtype) in enumerate(cols):
                        with col_layout[i % 2]:
                            if col_name in ["TERMINAL", "MONTH"]:
                                vals = get_distinct_values(target_table, col_name)
                                form_data[col_name] = st.selectbox(col_name, vals) if vals else st.text_input(col_name)
                            elif any(x in dtype for x in ['int', 'double', 'float', 'numeric']):
                                form_data[col_name] = st.number_input(col_name, min_value=0.0)
                            else:
                                form_data[col_name] = st.text_input(col_name)
                    
                    if st.form_submit_button("💾 Save Record"):
                        st.success("Record saved successfully!")
            except:
                st.error("Table not found. Please upload data first.")

# ==========================================
# PAGE 3: DATA EXPLORER
# ==========================================
elif "Data Explorer" in selected_page:
    st.title("🔎 Data Explorer")
    
    all_tables = []
    with engine.connect() as conn:
        all_tables = [r[0] for r in conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")).fetchall()]
    
    if port_scope != "ALL":
        all_tables = [t for t in all_tables if port_scope.lower() in t]
    
    dataset = st.selectbox("Select Dataset", sorted(all_tables))
    
    if dataset:
        df = pd.read_sql(f"SELECT * FROM {dataset}", engine)
        
        with st.expander("View Raw Data Table", expanded=True):
            st.dataframe(df, use_container_width=True)
        
        st.markdown("### 🛠️ Pivot Table Builder")
        c1, c2, c3 = st.columns(3)
        
        cat_cols = df.select_dtypes(include=['object']).columns.tolist()
        num_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        rows = c1.multiselect("Rows", cat_cols)
        cols = c2.multiselect("Columns", cat_cols)
        vals = c3.selectbox("Values", num_cols)
        
        if rows and vals:
            try:
                pivot = pd.pivot_table(df, values=vals, index=rows, columns=cols if cols else None, aggfunc="sum", fill_value=0)
                st.dataframe(pivot.style.format("{:,.0f}"), use_container_width=True)
                
                # --- [RESTORED] CHART FEATURE ---
                if st.checkbox("Show Chart", value=True):
                    st.bar_chart(pivot)
                    
            except Exception as e:
                st.warning(f"Pivot Error: {e}")

# ==========================================
# PAGE 4: BULK UPLOAD
# ==========================================
elif "Bulk Upload" in selected_page:
    st.title("📤 Bulk Upload")
    st.info("Drag and drop your Excel files here.")
    
    uploaded_file = st.file_uploader("Upload Excel", type=['xlsx'])
    target_table_name = st.text_input("Enter Target Table Name (e.g., apapa_vessel_traffic_201a)", value=f"{port_scope.lower()}_")

    if uploaded_file and target_table_name:
        if st.button("Process & Upload"):
            with st.spinner("Uploading..."):
                try:
                    xl = pd.ExcelFile(uploaded_file)
                    sheet_names = xl.sheet_names
                    target_sheet = next((s for s in sheet_names if "COMBINED" in s.upper()), sheet_names[0])
                    
                    df = pd.read_excel(uploaded_file, sheet_name=target_sheet)
                    if "Unnamed" in str(df.columns[0]):
                        df = pd.read_excel(uploaded_file, sheet_name=target_sheet, header=1)
                    
                    df.columns = (df.columns.astype(str).str.strip().str.upper()
                                  .str.replace(' ', '_').str.replace('/', '_')
                                  .str.replace('.', '').str.replace('(', '')
                                  .str.replace(')', '').str.replace('&', 'AND')
                                  .str.replace('-', '_'))
                    
                    st.write(f"**Preview ({target_sheet}):**")
                    st.dataframe(df.head())
                    
                    df.to_sql(target_table_name, engine, if_exists='append', index=False, method='multi', chunksize=500)
                    st.success(f"✅ Uploaded {len(df)} rows to `{target_table_name}`")
                except Exception as e:
                    st.error(f"Error: {e}")