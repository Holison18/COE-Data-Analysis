import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd

# 1. Page Setup
st.set_page_config(page_title="KNUST Analytics", layout="wide")
st.title("KNUST Engineering: Historical Data Explorer")
st.markdown("### Comprehensive Student Performance & Retention Dashboard")

# 2. Connect to Database
@st.cache_resource
def get_con():
    try:
        return duckdb.connect('knust_engineering.duckdb', read_only=True)
    except Exception as e:
        return None

con = get_con()

if not con:
    st.error("Database connection failed. Please ensure ingestion is complete.")
    st.stop()

# 3. Sidebar - Global Filters
st.sidebar.image("https://upload.wikimedia.org/wikipedia/en/thumb/8/87/KNUST_Emblem.png/220px-KNUST_Emblem.png", use_container_width=True)
st.sidebar.header("Global Filters")

# Helper to get valid options
def get_options(query):
    try:
        return con.execute(query).df().iloc[:, 0].dropna().tolist()
    except:
        return []

# Year Filter
years = get_options("SELECT DISTINCT academic_year FROM course_summary ORDER BY academic_year")
selected_year = st.sidebar.selectbox("Academic Year", ["All Years"] + years)

# Faculty Filter (Cascading)
fac_q = "SELECT DISTINCT faculty FROM course_summary"
if selected_year != "All Years":
    fac_q += f" WHERE academic_year = '{selected_year}'"
fac_q += " ORDER BY faculty"
faculties = get_options(fac_q)
selected_faculty = st.sidebar.selectbox("Faculty", ["All Faculties"] + faculties)

# Department Filter
dept_q = "SELECT DISTINCT department FROM course_summary WHERE 1=1"
# Program Filter
prog_q = "SELECT DISTINCT program FROM course_summary WHERE 1=1"
if selected_year != "All Years": prog_q += f" AND academic_year = '{selected_year}'"
if selected_faculty != "All Faculties": prog_q += f" AND faculty = '{selected_faculty}'"
prog_q += " ORDER BY program"
programs = get_options(prog_q)
selected_prog = st.sidebar.selectbox("Program", ["All Programs"] + programs)

# Course Filter
course_q = "SELECT DISTINCT course_code FROM course_summary WHERE 1=1"
if selected_year != "All Years": course_q += f" AND academic_year = '{selected_year}'"
if selected_faculty != "All Faculties": course_q += f" AND faculty = '{selected_faculty}'"
if selected_prog != "All Programs": course_q += f" AND program = '{selected_prog}'"
course_q = f"SELECT DISTINCT REPLACE(course_code, '\n', ' ') as clean_code FROM ({course_q.replace('SELECT DISTINCT course_code', 'SELECT course_code')}) ORDER BY clean_code"
courses = [x[0] for x in con.execute(course_q).fetchall() if x[0]]
selected_course = st.sidebar.selectbox("Course", ["All Courses"] + courses)

# Filter Construction
def build_where(alias=""):
    prefix = f"{alias}." if alias else ""
    clauses = ["1=1"]
    if selected_year != "All Years": clauses.append(f"{prefix}academic_year = '{selected_year}'")
    if selected_faculty != "All Faculties": clauses.append(f"{prefix}faculty = '{selected_faculty}'")
    if selected_prog != "All Programs": clauses.append(f"{prefix}program = '{selected_prog}'")
    if selected_course != "All Courses": clauses.append(f"REPLACE({prefix}course_code, '\n', ' ') = '{selected_course}'")
    return " AND ".join(clauses)

where_clause = build_where()

st.sidebar.divider()
st.sidebar.markdown("**Note:** Data based on ingested excel files.")

# 4. Main Tabs
tab_course, tab_cohort, tab_retain, tab_demo, tab_sem, tab_overview = st.tabs(["Course Performance", "Cohort Analysis", "Retention & Attrition", "Demographics", "Semester Analysis", "Course Overview"])


with tab_course:
    st.subheader("Course Performance Analysis")
    
    # ORDER:
    # 1. Peak & Dip
    # 2. Top 10 Table
    # 3. Difficulty Matrix
    # 4. Problem Courses
    # 5. Viz Switcher
    # 6. Slump & Service

    # --- 1. PEAK & DIP ---
    try:
        q_sem = f"""
            SELECT academic_year, semester, AVG(avg_mark) as val 
            FROM course_summary 
            WHERE {where_clause} 
            GROUP BY academic_year, semester 
            ORDER BY val DESC
        """
        df_sem = con.execute(q_sem).df()
        
        col1, col2 = st.columns(2)
        if not df_sem.empty:
            best = df_sem.iloc[0]
            worst = df_sem.iloc[-1]
            with col1:
                st.metric("Highest Average", f"{best['academic_year']} Sem {best['semester']}", f"{best['val']:.2f}")
            with col2:
                st.metric("Lowest Average", f"{worst['academic_year']} Sem {worst['semester']}", f"{worst['val']:.2f}", delta_color="inverse")
    except:
        st.info("Not enough data for Peak & Dip.")

    st.divider()

    # --- 2. TOP 10 TABLES ---
    st.markdown("#### Course Pass Rate Analysis")
    
    # Calculate DF first (needed for both sections)
    q_diff = f"""
        SELECT REPLACE(course_code, '\n', ' ') as clean_code, 
               MAX(course_name) as course_name,
               AVG(avg_mark) as mean_mark, 
               AVG((num_passed * 100.0) / NULLIF((num_passed + num_trailed), 0)) as pass_rate
        FROM course_summary
        WHERE {where_clause.replace('course_code', "REPLACE(course_code, '\n', ' ')")}
        GROUP BY clean_code
    """
    df_diff = con.execute(q_diff).df()
    
    if not df_diff.empty:
        df_diff['fail_rate'] = 100 - df_diff['pass_rate']
        
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown("##### Highest Pass Rates")
            top_easy = df_diff.sort_values(by=['pass_rate', 'mean_mark'], ascending=[False, False]).head(10)
            
            top_easy_display = top_easy[['clean_code', 'course_name', 'mean_mark', 'pass_rate', 'fail_rate']].rename(columns={
                'clean_code': 'Code',
                'course_name': 'Name',
                'mean_mark': 'Avg', 
                'pass_rate': 'Pass%',
                'fail_rate': 'Fail%'
            })
            top_easy_display.index = range(1, len(top_easy_display) + 1)
            st.dataframe(top_easy_display.style.format({'Avg': '{:.1f}', 'Pass%': '{:.1f}', 'Fail%': '{:.1f}'}), use_container_width=True)

        with c2:
            st.markdown("##### Lowest Pass Rates")
            top_diff = df_diff.sort_values(by=['pass_rate', 'mean_mark'], ascending=[True, True]).head(10)
            
            top_diff_display = top_diff[['clean_code', 'course_name', 'mean_mark', 'pass_rate', 'fail_rate']].rename(columns={
                'clean_code': 'Code',
                'course_name': 'Name',
                'mean_mark': 'Avg', 
                'pass_rate': 'Pass%',
                'fail_rate': 'Fail%'
            })
            top_diff_display.index = range(1, len(top_diff_display) + 1)
            st.dataframe(top_diff_display.style.format({'Avg': '{:.1f}', 'Pass%': '{:.1f}', 'Fail%': '{:.1f}'}), use_container_width=True)
    
        st.divider()

        # --- 3. DIFFICULTY MATRIX (Plot) ---
        st.markdown("#### Pass Rate vs Average Mark Matrix")
        
        # Highlight specific course - MOVED HERE
        hl_course = st.selectbox("Highlight Course (Optional):", ["None"] + courses)

        # Define Highlight Logic
        # 1. Size (Larger for highlighted)
        df_diff['size'] = df_diff['clean_code'].apply(lambda x: 20 if x == hl_course else 8)
        # 2. Border Width (Black border for highlighted)
        df_diff['line_width'] = df_diff['clean_code'].apply(lambda x: 2 if x == hl_course else 0)
        # 3. Sort Order (Highlighted last = drawn on top)
        df_diff['is_hl'] = df_diff['clean_code'] == hl_course
        df_diff = df_diff.sort_values('is_hl')

        fig_diff = px.scatter(df_diff, x='mean_mark', y='pass_rate', hover_name='clean_code',
                              title="Course Performance Matrix",
                              labels={'mean_mark': 'Average Mark', 'pass_rate': 'Pass Rate (%)'},
                              color='pass_rate', size='size', 
                              color_continuous_scale='RdYlGn',
                              range_color=[0, 100])
        
        # Apply custom marker styling (Border) using the columns we created
        fig_diff.update_traces(marker=dict(line=dict(width=df_diff['line_width'], color='Black')))
        
        # Add reference lines - THRESHOLD 40
        fig_diff.add_hline(y=40, line_dash="dash", annotation_text="Pass Threshold (40%)")
        fig_diff.add_vline(x=60, line_dash="dash", annotation_text="Avg Threshold")
        fig_diff.update_xaxes(range=[0, 100])
        fig_diff.update_yaxes(range=[0, 100])
        # Hide the 'size' legend if it appears (color bar remains)
        fig_diff.update_layout(showlegend=False)
        
        st.plotly_chart(fig_diff, use_container_width=True)

    st.divider()

    # --- 4. COMPARATIVE TREND ANALYSIS ---
    st.markdown("#### Comparative Trend Analysis")
    st.write("Select multiple courses to compare their historical performance trends side-by-side.")

    # Multiselect for courses
    cmp_courses = st.multiselect("Select Courses to Compare", courses, default=[])

    if cmp_courses:
        cmp_sql = ",".join([f"'{c}'" for c in cmp_courses])
        
        q_cmp = f"""
            SELECT academic_year, REPLACE(course_code, '\n', ' ') as clean_code, 
                   AVG(avg_mark) as avg_mark
            FROM course_summary
            WHERE REPLACE(course_code, '\n', ' ') IN ({cmp_sql})
            AND {where_clause.replace('course_code', "REPLACE(course_code, '\n', ' ')")}
            GROUP BY academic_year, clean_code
            ORDER BY academic_year
        """
        df_cmp = con.execute(q_cmp).df()
        
        if not df_cmp.empty:
            # Enforce chronological order for X-axis
            sorted_years = sorted(df_cmp['academic_year'].unique())

            fig_cmp = px.line(df_cmp, x='academic_year', y='avg_mark', color='clean_code', markers=True,
                              title="Comparative Trend Analysis",
                              labels={'avg_mark': 'Average Mark', 'clean_code': 'Course'},
                              category_orders={'academic_year': sorted_years})
            fig_cmp.update_yaxes(range=[0, 100])
            st.plotly_chart(fig_cmp, use_container_width=True)
        else:
            st.info("No data found for the selected courses.")
    else:
        st.info("Please select at least one course.")

    st.divider()
    
    # --- 5. HISTORICAL PERFORMANCE EXPLORER (Viz Switcher) ---
    st.markdown("#### Historical Performance Trends")
    viz_type = st.radio("Select Visualization:", ["Line Chart (Trends)", "Box Plot (Spread)", "Histogram (Distribution)"], horizontal=True)
    
    if viz_type == "Line Chart (Trends)":
        q_trend = f"SELECT academic_year, AVG(mark) as val FROM student_performance WHERE {where_clause} GROUP BY academic_year ORDER BY academic_year"
        df_t = con.execute(q_trend).df()
        if not df_t.empty:
            st.plotly_chart(px.line(df_t, x='academic_year', y='val', markers=True, title="Average Marks Trend"), use_container_width=True)
            
    elif viz_type == "Box Plot (Spread)":
        q_box = f"SELECT academic_year, mark FROM student_performance WHERE {where_clause} USING SAMPLE 10%"
        df_b = con.execute(q_box).df()
        if not df_b.empty:
            st.plotly_chart(px.box(df_b.sort_values('academic_year'), x='academic_year', y='mark', title="Mark Distribution by Year"), use_container_width=True)
            
    elif viz_type == "Histogram (Distribution)":
        q_hist = f"SELECT mark FROM student_performance WHERE {where_clause} USING SAMPLE 10%"
        df_h = con.execute(q_hist).df()
        if not df_h.empty:
            st.plotly_chart(px.histogram(df_h, x='mark', nbins=20, title="Overall Grade Distribution"), use_container_width=True)

    st.divider()





# --- MODULE B: RETENTION & ATTRITION ---
with tab_retain:
    st.subheader("Retention & Attrition Insights")
    
    # 1. ATTRITION (Withdrawn)
    st.warning("Note: 'Withdrawn' status not explicitly tracked. Showing Failure/Trail Rates as integrity proxy.")
    
    q_risk = f"""
        SELECT academic_year, SUM(num_trailed) as trails, SUM(num_passed) as passes
        FROM course_summary 
        WHERE {where_clause}
        GROUP BY academic_year
        ORDER BY academic_year
    """
    df_risk = con.execute(q_risk).df()
    if not df_risk.empty:
        df_risk['total'] = df_risk['trails'] + df_risk['passes']
        df_risk['trail_rate'] = (df_risk['trails'] / df_risk['total']) * 100
        st.plotly_chart(px.bar(df_risk, x='academic_year', y='trail_rate', title="Trail Rate (Percentage of Fails) per Year"), use_container_width=True)

    # 2. STICKY TRAIL (Binned Bar Chart)
    st.markdown("#### Impact of Trailed Courses on CWA")
    st.write("Average CWA for students grouped by number of trailed courses.")
    
    # Threshold 40
    q_sticky = f"""
        WITH trail_counts AS (
            SELECT student_id, AVG(cwa) as avg_cwa, 
                   COUNT(CASE WHEN mark < 40 THEN 1 END) as num_trails
            FROM student_performance
            WHERE {where_clause} AND cwa IS NOT NULL
            GROUP BY student_id
        ),
        binned_trails AS (
            SELECT avg_cwa, 
                   CASE 
                       WHEN num_trails = 0 THEN '0 Trails'
                       WHEN num_trails BETWEEN 1 AND 2 THEN '1-2 Trails'
                       WHEN num_trails BETWEEN 3 AND 5 THEN '3-5 Trails'
                       ELSE '6+ Trails'
                   END as trail_group,
                   CASE 
                       WHEN num_trails = 0 THEN 0
                       WHEN num_trails BETWEEN 1 AND 2 THEN 1
                       WHEN num_trails BETWEEN 3 AND 5 THEN 2
                       ELSE 3
                   END as sort_order
            FROM trail_counts
        )
        SELECT trail_group, AVG(avg_cwa) as mean_cwa, sort_order
        FROM binned_trails
        GROUP BY trail_group, sort_order
        ORDER BY sort_order
    """
    df_sticky = con.execute(q_sticky).df()
    
    if not df_sticky.empty:
        fig_stick = px.bar(df_sticky, x='trail_group', y='mean_cwa',
                               title="Average CWA by Trail Count Group",
                               labels={'trail_group': 'Trail Category', 'mean_cwa': 'Average CWA'},
                               text_auto='.2f', color='trail_group',
                               color_discrete_sequence=px.colors.qualitative.Prism)
        fig_stick.update_yaxes(range=[0, 100])
        fig_stick.update_layout(showlegend=False)
        st.plotly_chart(fig_stick, use_container_width=True)
    else:
        st.info("Not enough data with CWA and Trails.")

    # --- 3. OVERLOAD TRAP (Removed) ---


# --- MODULE C: DEMOGRAPHICS ---
with tab_demo:
    st.subheader("Demographic Analysis")
    
    # GENDER GAP in Gatekeepers
    st.markdown("#### Gender Gap Analysis")
    st.write("Compare Male vs Female performance in specific courses.")
    
    default_gk = ['MATH 151', 'EE 151', 'COE 181', 'ME 161']
    # Filter default to available courses
    avail_defaults = [c for c in default_gk if c in courses]
    
    selected_gk = st.multiselect("Select Courses to Analyze", courses, default=avail_defaults)
    
    if selected_gk:
        gk_sql = ",".join([f"'{t}'" for t in selected_gk])
        q_gender = f"""
            SELECT REPLACE(course_code, '\n', ' ') as clean_code, gender, mark
            FROM student_performance
            WHERE REPLACE(course_code, '\n', ' ') IN ({gk_sql}) AND gender IN ('Male', 'Female')
            AND {where_clause.replace('course_code', "REPLACE(course_code, '\n', ' ')")}
        """
        df_gender = con.execute(q_gender).df()
        
        if not df_gender.empty:
            fig_gen = px.box(df_gender, x='clean_code', y='mark', color='gender',
                             title="Performance Distribution by Gender",
                             color_discrete_map={'Male': '#1f77b4', 'Female': '#e377c2'})
            fig_gen.update_yaxes(range=[0, 100])
            st.plotly_chart(fig_gen, use_container_width=True)
        else:
            st.info("No gender data found for these courses.")
    else:
        st.warning("Please select at least one course.")

# --- MODULE E: DETAILED SEMESTER ANALYSIS ---
with tab_sem:
    st.subheader("Detailed Semester Analysis")
    st.write("Deep dive into specific Course trends and Semester Trajectories.")
    
    # 1. COURSE HISTORY EXPLORER
    st.divider()
    st.markdown("#### Course History Explorer")
    st.write("Select a course to see how it has performed over differrent Academic Years.")
    
    hist_course = st.selectbox("Select Course for History", courses)
    
    if hist_course:
        q_hist = f"""
            SELECT academic_year, mark
            FROM student_performance
            WHERE REPLACE(course_code, '\n', ' ') = '{hist_course}'
            AND {where_clause.replace('course_code', "REPLACE(course_code, '\n', ' ')")}
            ORDER BY academic_year
        """
        df_hist = con.execute(q_hist).df()
        
        if not df_hist.empty:
            # Calculate Year Coverage
            total_years = len(years) # 'years' list from sidebar
            course_years = df_hist['academic_year'].nunique()
            
            # Display Metrics
            m1, m2 = st.columns(2)
            m1.metric("Data Available For", f"{course_years} / {total_years} Years")
            m2.metric("Missing Years", total_years - course_years)
        
            # Box plot to show distribution over time (Years)
            fig_hist = px.box(df_hist, x='academic_year', y='mark', 
                              title=f"Historical Performance Distribution: {hist_course}",
                              labels={'academic_year': 'Academic Year', 'mark': 'Mark'})
            fig_hist.update_yaxes(range=[0, 100])
            st.plotly_chart(fig_hist, use_container_width=True)
            
            # Avg Trend Line
            avg_hist = df_hist.groupby('academic_year')['mark'].mean().reset_index()
            fig_trend = px.line(avg_hist, x='academic_year', y='mark', markers=True,
                                title=f"Average Mark Trend: {hist_course}")
            fig_trend.update_yaxes(range=[0, 100])
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info(f"No historical data found for {hist_course}")

    # 2. SEMESTER IMPACT ANALYSIS
    st.divider()
    st.markdown("#### Semester Impact Trajectory")
    st.write("How does student performance evolve from Year 1 Sem 1 to Year 4 Sem 2?")
    
    q_traj = f"""
        WITH sem_perf AS (
            SELECT 
                CASE 
                    WHEN REPLACE(course_code, '\n', ' ') LIKE '%1__' THEN 'Year 1'
                    WHEN REPLACE(course_code, '\n', ' ') LIKE '%2__' THEN 'Year 2'
                    WHEN REPLACE(course_code, '\n', ' ') LIKE '%3__' THEN 'Year 3'
                    WHEN REPLACE(course_code, '\n', ' ') LIKE '%4__' THEN 'Year 4'
                    ELSE 'Other'
                END as level_est,
                semester,
                avg_mark as swa_proxy
            FROM course_summary
            WHERE {where_clause.replace('course_code', "REPLACE(course_code, '\n', ' ')")}
        )
        SELECT level_est, semester, AVG(swa_proxy) as mean_perf
        FROM sem_perf
        WHERE level_est != 'Other'
        GROUP BY level_est, semester
        ORDER BY level_est, semester
    """
    df_traj = con.execute(q_traj).df()
    
    if not df_traj.empty:
        df_traj['sem_label'] = df_traj['level_est'] + " Sem " + df_traj['semester'].astype(str)
        
        fig_traj = px.line(df_traj, x='sem_label', y='mean_perf', markers=True,
                           title="Cohort Performance Trajectory (Est. by Course Levels)",
                           labels={'sem_label': 'Semester Step', 'mean_perf': 'Average Performance'},
                           text='mean_perf')
        fig_traj.update_traces(textposition="bottom right")
        fig_traj.update_yaxes(range=[40, 80]) # Zoom in on passing range
        st.plotly_chart(fig_traj, use_container_width=True)
    else:
        st.info("Insufficient data for Trajectory Analysis.")

# --- MODULE 0: COURSE OVERVIEW ---
with tab_overview:
    st.subheader("Course Overview")
    st.write("Browse courses and see which programs offer them.")
    
    # Search Feature
    search_term = st.text_input("Search Course Code or Name", "")

    # Query to get Course Details
    # We group by Course Code and Name, and aggregate Programs and Years
    q_overview = f"""
        SELECT 
            REPLACE(course_code, '\n', ' ') as clean_code, 
            MAX(course_name) as course_name,
            LIST(DISTINCT program) as programs,
            LIST(DISTINCT academic_year) as years
        FROM course_summary
        WHERE {where_clause.replace('course_code', "REPLACE(course_code, '\n', ' ')")}
        GROUP BY clean_code
        ORDER BY clean_code
    """
    df_overview = con.execute(q_overview).df()
    
    if not df_overview.empty:
        # Convert lists to string for display if needed, or keep as list for standard dataframe
        # Streamlit dataframe handles lists nicely now, but string is safer for search
        df_overview['programs'] = df_overview['programs'].apply(lambda x: ", ".join(sorted(x)) if x is not None and len(x) > 0 else "")
        df_overview['years'] = df_overview['years'].apply(lambda x: ", ".join(sorted(x)) if x is not None and len(x) > 0 else "")
        
        # Renaissance Columns
        df_display = df_overview.rename(columns={
            'clean_code': 'Course Code',
            'course_name': 'Course Name',
            'programs': 'Offered In',
            'years': 'Academic Years'
        })
        
        # Apply Search Filter
        if search_term:
            df_display = df_display[
                df_display['Course Code'].str.contains(search_term, case=False) | 
                df_display['Course Name'].str.contains(search_term, case=False)
            ]

        st.dataframe(
            df_display,
            column_config={
                "Course Code": st.column_config.TextColumn("Course Code", width="small"),
                "Course Name": st.column_config.TextColumn("Course Name", width="medium"),
                "Offered In": st.column_config.TextColumn("Offered In Programs", width="large", help="The programs that include this course."),
                "Academic Years": st.column_config.TextColumn("Academic Years", width="medium", help="Years for which data is available.")
            },
            use_container_width=False,
            hide_index=True
        )
        st.info("No courses found matching the selected filters.")

# --- MODULE F: COHORT ANALYSIS ---
with tab_cohort:
    st.subheader("Cohort Progress Analysis")
    st.write("Analyze the progression of a specific cohort (students who entered in the same year) from entry to completion.")
    
    # 1. Cohort Selection
    # Define Cohort: Students who took Level 100 courses in a specific year
    q_cohort_years = """
        SELECT DISTINCT academic_year 
        FROM student_performance 
        WHERE REPLACE(course_code, '\n', ' ') LIKE '%1__'
        ORDER BY academic_year
    """
    try:
        cohort_years = [x[0] for x in con.execute(q_cohort_years).fetchall() if x[0]]
    except:
        cohort_years = []
        
    col_c1, col_c2 = st.columns([1, 3])
    with col_c1:
        sel_cohort = st.selectbox("Select Cohort (Entry Year)", cohort_years)
    
    if sel_cohort:
        # Build Filter for Cohort (Respects Faculty/Dept/Prog but IGNORES Global Year)
        # We redefine the filter logic locally
        cohort_clauses = ["1=1"]
        if selected_faculty != "All Faculties": cohort_clauses.append(f"faculty = '{selected_faculty}'")
        if selected_prog != "All Programs": cohort_clauses.append(f"program = '{selected_prog}'")
        cohort_where = " AND ".join(cohort_clauses)
        
        # 2. Identify Cohort Students
        # Students who started in sel_cohort AND match other filters
        # EXCLUDING students who have records in prior years (to filter out trailers)
        q_ids = f"""
            WITH starters AS (
                SELECT DISTINCT student_id 
                FROM student_performance
                WHERE academic_year = '{sel_cohort}'
                AND REPLACE(course_code, '\n', ' ') LIKE '%1__'
            ),
            matches AS (
                SELECT DISTINCT student_id 
                FROM student_performance 
                WHERE {cohort_where}
            ),
            priors AS (
                SELECT DISTINCT student_id
                FROM student_performance
                WHERE academic_year < '{sel_cohort}'
            )
            SELECT s.student_id 
            FROM starters s
            JOIN matches m ON s.student_id = m.student_id
            WHERE s.student_id NOT IN (SELECT student_id FROM priors)
        """
        
        # 3. Aggregated Progress Data
        # We pull data for these students across ALL years
        q_prog = f"""
            WITH cohort_list AS ({q_ids})
            SELECT 
                academic_year,
                COUNT(DISTINCT student_id) as active_students,
                AVG(mark) as avg_mark,
                AVG(cwa) as avg_cwa,
                COUNT(CASE WHEN num_trailed > 0 THEN 1 END) as students_trailing_courses
            FROM (
                -- Join performance with summary to get num_trailed if needed, 
                -- or just compute from raw marks if simpler. 
                -- For simplicity, let's use student_performance directly.
                SELECT sp.student_id, sp.academic_year, sp.mark, sp.cwa, 0 as num_trailed
                FROM student_performance sp
                JOIN cohort_list cl ON sp.student_id = cl.student_id
            )
            GROUP BY academic_year
            ORDER BY academic_year
        """
        
        df_cohort = con.execute(q_prog).df()
        
        if not df_cohort.empty:
            # Display Key Metrics (Current Status vs Entry)
            initial_count = df_cohort.iloc[0]['active_students']
            current_count = df_cohort.iloc[-1]['active_students']
            retention_rate = (current_count / initial_count) * 100 if initial_count > 0 else 0
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Cohort Size (Year 1)", initial_count)
            m2.metric("Current active / Recorded", current_count)
            m3.metric("Retention/Data Rate", f"{retention_rate:.1f}%")
            
            st.divider()
            
            # Visualizations
            c1, c2 = st.columns(2)
            
            with c1:
                # Retention Funnel (Bar Chart)
                fig_ret = px.bar(df_cohort, x='academic_year', y='active_students',
                                 title=f"Cohort Retention: {sel_cohort} Intake",
                                 labels={'active_students': 'Number of Students', 'academic_year': 'Academic Year'},
                                 text='active_students')
                fig_ret.update_traces(textposition='outside')
                st.plotly_chart(fig_ret, use_container_width=True)
                
            with c2:
                # Performance Trajectory
                fig_perf = px.line(df_cohort, x='academic_year', y='avg_mark', markers=True,
                                   title=f"Performance Trajectory: {sel_cohort} Intake",
                                   labels={'avg_mark': 'Average Mark', 'academic_year': 'Academic Year'})
                fig_perf.update_yaxes(range=[40, 80])
                st.plotly_chart(fig_perf, use_container_width=True)

            # --- 4. SEMESTER BREAKDOWN ---
            st.markdown("#### Semester-by-Semester Progression")
            st.write("Average performance broken down by specific semesters (e.g., Year 1 Sem 1, Year 1 Sem 2).")
            
            q_sem_prog = f"""
                WITH cohort_list AS ({q_ids})
                SELECT 
                    sp.academic_year,
                    sp.semester,
                    AVG(sp.mark) as avg_mark
                FROM student_performance sp
                JOIN cohort_list cl ON sp.student_id = cl.student_id
                GROUP BY sp.academic_year, sp.semester
                ORDER BY sp.academic_year, sp.semester
            """
            df_sem_prog = con.execute(q_sem_prog).df()
            
            if not df_sem_prog.empty:
                # Create a readable label
                df_sem_prog['sem_label'] = df_sem_prog['academic_year'] + " Sem " + df_sem_prog['semester'].astype(str)
                
                fig_sem = px.line(df_sem_prog, x='sem_label', y='avg_mark', markers=True,
                                  title=f"Semester-by-Semester Performance: {sel_cohort} Intake",
                                  labels={'sem_label': 'Semester', 'avg_mark': 'Average Mark'})
                fig_sem.update_yaxes(range=[40, 80])
                st.plotly_chart(fig_sem, use_container_width=True)
            else:
                st.info("No detailed semester data available for this cohort.")

        else:
            st.warning("No data found for this cohort with the selected filters.")
    else:
        st.info("Please select a cohort year.")
