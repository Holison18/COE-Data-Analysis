import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd

# 1. Page Setup
st.set_page_config(page_title="KNUST Analytics", layout="wide")
st.title("KNUST Engineering: Historical Data Explorer")
st.markdown("### Interactive Dashboard for Student Performance Analysis (50-Year Dataset)")

# 2. Connect to Database
@st.cache_resource
def get_con():
    try:
        # Connect in read-only mode string
        return duckdb.connect('knust_engineering.duckdb', read_only=True)
    except Exception as e:
        return None

con = get_con()

if not con:
    st.error("Database connection failed. Please ensure 'knust_engineering.duckdb' exists and is not locked.")
    st.stop()

# 3. Sidebar Configuration
st.sidebar.header("Filter Data")

# 3. Sidebar Configuration
st.sidebar.image("https://upload.wikimedia.org/wikipedia/en/thumb/8/87/KNUST_Emblem.png/220px-KNUST_Emblem.png", use_container_width=True)
st.sidebar.header("Data Filters")

# Filter: Faculty
try:
    faculties = con.execute("SELECT DISTINCT faculty FROM course_summary ORDER BY faculty").df()['faculty'].dropna().tolist()
    # Add "All Faculties" option
    selected_faculty = st.sidebar.selectbox("Select Faculty", ["All Faculties"] + faculties)
except:
    st.error("Could not read faculties. Database might be empty.")
    st.stop()

# Filter: Program (Dependent on Faculty)
if selected_faculty == "All Faculties":
    programs = con.execute("SELECT DISTINCT program FROM course_summary ORDER BY program").df()['program'].dropna().tolist()
    selected_prog = st.sidebar.selectbox("Select Program", ["All Programs"] + programs)
    context_label = "College of Engineering"
else:
    programs = con.execute(f"SELECT DISTINCT program FROM course_summary WHERE faculty = '{selected_faculty}' ORDER BY program").df()['program'].dropna().tolist()
    selected_prog = st.sidebar.selectbox("Select Program", ["All Programs"] + programs)
    context_label = f"{selected_faculty}"

# Filter: Course (Dependent on Program)
if selected_prog == "All Programs":
    if selected_faculty == "All Faculties":
        courses_q = "SELECT DISTINCT course_code FROM course_summary ORDER BY course_code"
    else:
        courses_q = f"SELECT DISTINCT course_code FROM course_summary WHERE faculty = '{selected_faculty}' ORDER BY course_code"
    
    courses = con.execute(courses_q).df()['course_code'].dropna().tolist()
    selected_course = st.sidebar.selectbox("Select Course", ["All Courses"] + courses)
    sub_context_label = "All Programs"
else:
    courses = con.execute(f"SELECT DISTINCT course_code FROM course_summary WHERE program = '{selected_prog}' ORDER BY course_code").df()['course_code'].dropna().tolist()
    selected_course = st.sidebar.selectbox("Select Course", ["All Courses"] + courses)
    sub_context_label = selected_prog
    
st.sidebar.divider()
st.sidebar.markdown("**Note**: Data spans ~50 years of records.")

# 4. KPI Metrics (Dynamic based on selection)
st.markdown(f"## Dashboard: {sub_context_label}")
if selected_faculty != "All Faculties":
         st.caption(f"Faculty: {selected_faculty}")

col1, col2, col3, col4 = st.columns(4)

# Build WHERE clause helper
def build_where(prog, fac, alias=""):
    prefix = f"{alias}." if alias else ""
    clauses = []
    if prog != "All Programs":
        clauses.append(f"{prefix}program = '{prog}'")
    elif fac != "All Faculties":
        clauses.append(f"{prefix}faculty = '{fac}'")
    return " AND ".join(clauses) if clauses else "1=1"

where_clause_summ = build_where(selected_prog, selected_faculty)
where_clause_marks = build_where(selected_prog, selected_faculty)

with col1:
    # Aggregating across filtered scope
    total_marks = con.execute(f"SELECT COUNT(*) FROM student_marks WHERE {where_clause_marks}").fetchone()[0]
    st.metric("Total Exam Records", f"{total_marks:,}", help="Total individual student exam entries.")

with col2:
    avg_perf = con.execute(f"SELECT AVG(mark) FROM student_marks WHERE {where_clause_marks}").fetchone()[0]
    st.metric("Overall Average Mark", f"{avg_perf:.2f}" if avg_perf else "N/A", help="Average score across all courses in selection.")

with col3:
    # Distinct students
    total_students = con.execute(f"SELECT COUNT(DISTINCT student_id) FROM student_marks WHERE {where_clause_marks}").fetchone()[0]
    st.metric("Total Students", f"{total_students:,}", help="Unique students processed.")

with col4:
    # Failure Rate (Mark < 50)
    fail_count = con.execute(f"SELECT COUNT(*) FROM student_marks WHERE {where_clause_marks} AND mark < 50").fetchone()[0]
    fail_rate = (fail_count / total_marks * 100) if total_marks else 0
    st.metric("Overall Failure Rate", f"{fail_rate:.1f}%", delta=f"-{fail_rate:.1f}%", delta_color="inverse", help="Percentage of marks below 50.")

st.divider()

# 5. Tabs
tabs = ["Overview & Trends", "Faculty/Program Comparison", "Difficulty Analysis", "Student Stats"]
tab_overview, tab_comp, tab_diff, tab_stud = st.tabs(tabs)

# --- TAB: OVERVIEW & TRENDS ---
with tab_overview:
    st.header("Historical Performance Trends")
    st.write("How has performance changed over the decades?")
    
    st.write("Distribution of student marks across academic years. (Box Plot shows Median, Range, and Outliers)")
    
    # Yearly Trend - Box Plot
    # Use sampling to ensure performance with large datasets
    q_trend = f"""
        SELECT academic_year, mark 
        FROM student_marks 
        WHERE {where_clause_marks} AND academic_year IS NOT NULL
        USING SAMPLE 20%
    """
    df_trend = con.execute(q_trend).df()
    
    if not df_trend.empty:
        # Sort by year
        df_trend = df_trend.sort_values('academic_year')
        
        fig_trend = px.box(df_trend, x='academic_year', y='mark', 
                           title="Academic Performance Distribution (Year-over-Year)",
                           labels={'mark': 'Marks (0-100)', 'academic_year': 'Academic Year'},
                           color_discrete_sequence=['#FFD700']) # KNUST Gold
        
        fig_trend.update_layout(
            plot_bgcolor='white',
            yaxis_range=[0, 100], # Fix y-axis to standard marking scale
            xaxis_title="Academic Year",
            yaxis_title="Student Marks"
        )
        st.plotly_chart(fig_trend, use_container_width=True)
        
        with st.expander("How to read this Box Plot"):
            st.markdown("""
            - **Middle Line**: The Median mark (50th percentile).
            - **Box**: Represents the middle 50% of students (25th to 75th percentile).
            - **Whiskers**: The range of typical marks.
            - **Dots**: Outliers (Unusually high or low marks).
            """)
    else:
        st.info("No trend data available for the current selection.")

# --- TAB: FACULTY/PROGRAM COMPARISON ---
with tab_comp:
    st.header("Comparative Analysis")
    
    if selected_prog == "All Programs":
        # Compare Programs or Facs
        group_col = "program" if selected_faculty != "All Faculties" else "faculty"
        
        st.subheader(f"Performance by {group_col.title()}")
        q_comp = f"""
            SELECT {group_col}, AVG(mark) as avg_mark, COUNT(*) as records
            FROM student_marks
            WHERE {where_clause_marks}
            GROUP BY {group_col}
            ORDER BY avg_mark DESC
        """
        df_comp = con.execute(q_comp).df()
        
        col_c1, col_c2 = st.columns([2,1])
        with col_c1:
            fig_comp = px.bar(df_comp, x=group_col, y='avg_mark', color='avg_mark',
                             title=f"Which {group_col.title()} performs best?",
                             text_auto='.2f', # Show values on bars
                             color_continuous_scale='ylgn')
            
            fig_comp.update_layout(
                yaxis_range=[0, 100], # Fix y-axis to standard 0-100 scale to prevent cutting off
                xaxis_title=group_col.title(),
                yaxis_title="Average Mark"
            )
            st.plotly_chart(fig_comp, use_container_width=True)
        with col_c2:
            st.write(f"**Top Performing {group_col.title()}:**")
            top = df_comp.iloc[0]
            st.success(f"{top[group_col]} ({top['avg_mark']:.2f})")
            
            st.write(f"**Lowest Performing {group_col.title()}:**")
            bot = df_comp.iloc[-1]
            st.error(f"{bot[group_col]} ({bot['avg_mark']:.2f})")
            
    else:
        st.info("Select 'All Programs' to view comparative analysis between departments.")

# --- TAB: DIFFICULTY ANALYSIS ---
with tab_diff:
    st.header("Course Difficulty Analysis")
    st.write("identifying the 'Gatekeeper' courses with the lowest average marks.")
    
    # Top 15 Hardest Courses
    q_hard = f"""
        SELECT course_code, AVG(mark) as avg_mark, COUNT(*) as students_taken,
               SUM(CASE WHEN mark < 50 THEN 1 ELSE 0 END) as failures
        FROM student_marks 
        WHERE {where_clause_marks}
        GROUP BY course_code
        HAVING COUNT(*) > 50  -- Filter out tiny classes
        ORDER BY avg_mark ASC
        LIMIT 15
    """
    df_hard = con.execute(q_hard).df()
    
    if not df_hard.empty:
        # Fetch Course Names (Mapping)
        # We take the most common or first name found for each code to handle potential variations
        q_names = f"""
            SELECT course_code, MAX(course_name) as course_name
            FROM course_summary 
            WHERE course_name IS NOT NULL
            GROUP BY course_code
        """
        df_names = con.execute(q_names).df()
        
        # Ensure course_code is clean for merging
        # Fix: 'PETE\n356' (Detailed) vs 'PETE 356' (Summary) mismatch
        df_hard['course_code'] = df_hard['course_code'].astype(str).str.replace('\n', ' ').str.strip()
        df_names['course_code'] = df_names['course_code'].astype(str).str.replace('\n', ' ').str.strip()
        
        # Merge names into hard courses
        df_hard = df_hard.merge(df_names, on='course_code', how='left')
        
        # Calculate Fail Rate
        df_hard['fail_rate'] = (df_hard['failures'] / df_hard['students_taken']) * 100
        
        fig_diff = px.scatter(df_hard, x='avg_mark', y='fail_rate', size='students_taken',
                              hover_name='course_name', hover_data=['course_code'], # Show Name on hover
                              color='fail_rate',
                              title="Course Difficulty Matrix (Low Avg Mark vs High Fail Rate)",
                              labels={'avg_mark': 'Average Mark', 'fail_rate': 'Failure Rate (%)'},
                              color_continuous_scale='RdYlGn_r')
        st.plotly_chart(fig_diff, use_container_width=True)
        
        st.subheader("The 'Toughest' Courses List")
        # Reorder columns to show Name next to Code
        cols = ['course_code', 'course_name', 'avg_mark', 'fail_rate', 'students_taken']
        st.dataframe(df_hard[cols].style.format({
            'avg_mark': '{:.2f}',
            'fail_rate': '{:.1f}%'
        }))
    else:
        st.warning("Not enough data to determine difficult courses (need >50 records per course).")

# --- TAB: STUDENT STATS ---
with tab_stud:
    st.header("Student Performance Distribution")
    
    if selected_course != "All Courses":
         st.subheader(f"Grade Distribution: {selected_course}")
         q_dist = f"""
            SELECT mark, academic_year FROM student_marks 
            WHERE {where_clause_marks} AND course_code = '{selected_course}'
         """
         df_dist = con.execute(q_dist).df()
         if not df_dist.empty:
             fig_hist = px.histogram(df_dist, x='mark', nbins=20, 
            title=f"Histogram of Marks: {selected_course}",
                                   color_discrete_sequence=['black'])
             st.plotly_chart(fig_hist, use_container_width=True)
    else:
        st.subheader("Overall Grade Distribution")
        # Sample to avoid heavy load if millions of rows
        q_dist = f"""
            SELECT mark FROM student_marks 
            WHERE {where_clause_marks}
            USING SAMPLE 10%
        """
        df_dist = con.execute(q_dist).df()
        fig_hist = px.histogram(df_dist, x='mark', nbins=20, title="Grade Distribution (10% Sample)",
                               color_discrete_sequence=['black'])
        st.plotly_chart(fig_hist, use_container_width=True)