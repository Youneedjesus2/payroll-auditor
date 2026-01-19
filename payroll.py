"""
CORNERSTONE CARE PAYROLL AUDITOR

A comprehensive payroll management system that:
1. Manages staff, clients, and assignments
2. Processes EVV reports and compares against POS limits
3. Handles private clients via manual entry (photo AI coming soon)
4. Calculates accurate payable hours based on assignments

Run with: streamlit run payroll.py
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO

import database as db

# =============================================================================
# PAGE CONFIG
# =============================================================================

st.set_page_config(
    page_title="Cornerstone Payroll",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# CUSTOM STYLES
# =============================================================================

st.markdown("""
<style>
    .block-container { padding-top: 2rem; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px;
        border-radius: 4px;
    }
    div[data-testid="stMetricValue"] { font-size: 1.8rem; }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        margin: 1rem 0;
    }
    .warning-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #fff3cd;
        border: 1px solid #ffeeba;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# SESSION STATE INITIALIZATION
# =============================================================================

def init_session_state():
    """Initialize all session state variables."""
    defaults = {
        # Navigation
        'current_page': 'payroll',

        # Week 1 data
        'week1_evv_data': None,
        'week1_claims_data': None,
        'week1_results': None,

        # Week 2 data
        'week2_evv_data': None,
        'week2_claims_data': None,
        'week2_results': None,

        # Combined results
        'combined_results': None,

        # Admin UI state
        'show_add_staff': False,
        'show_add_client': False,
        'show_add_assignment': False,
        'show_add_family_group': False,
        'editing_staff_id': None,
        'editing_client_id': None,
        'editing_assignment_id': None,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# =============================================================================
# FILE READING FUNCTIONS
# =============================================================================

def read_evv_report(uploaded_file):
    """Read EVV Services Rendered Report."""
    try:
        df_raw = pd.read_excel(uploaded_file, sheet_name='EVV SRR-SA Detail Comments')

        # Find the header row
        header_row = None
        for idx, row in df_raw.iterrows():
            if 'Service Date' in str(row.iloc[0]):
                header_row = idx
                break

        if header_row is None:
            st.error("Could not find header row in EVV file")
            return None

        # Read with proper headers
        headers = df_raw.iloc[header_row].tolist()
        df = pd.read_excel(
            uploaded_file,
            sheet_name='EVV SRR-SA Detail Comments',
            skiprows=header_row + 1,
            names=headers
        )

        # Filter to data rows
        df = df[df['Service Date'].notna()].copy()

        # Convert to numeric
        df['Service Duration (hours)'] = pd.to_numeric(df['Service Duration (hours)'], errors='coerce')
        df['Units'] = pd.to_numeric(df['Units'], errors='coerce')

        # Weekly POS Hours might not exist in all files
        if 'Weekly POS Hours' in df.columns:
            df['Weekly POS Hours'] = pd.to_numeric(df['Weekly POS Hours'], errors='coerce')

        return df

    except Exception as e:
        st.error(f"Error reading EVV file: {str(e)}")
        return None


def read_claims_report(uploaded_file):
    """Read Provider Portal Claims Report."""
    try:
        df_raw = pd.read_excel(uploaded_file, sheet_name='Provider Claims Report')

        # Find header row
        header_row = None
        for idx, row in df_raw.iterrows():
            if 'Service Date' in str(row.iloc[0]):
                header_row = idx
                break

        if header_row is None:
            st.error("Could not find header row in Claims file")
            return None

        # Read with proper headers
        headers = df_raw.iloc[header_row].tolist()
        df = pd.read_excel(
            uploaded_file,
            sheet_name='Provider Claims Report',
            skiprows=header_row + 1,
            names=headers
        )

        # Filter to data rows
        df = df[df['Service Date'].notna()].copy()

        # Convert to numeric
        df['Net Units'] = pd.to_numeric(df['Net Units'], errors='coerce')

        return df

    except Exception as e:
        st.error(f"Error reading Claims file: {str(e)}")
        return None


# =============================================================================
# PAYROLL CALCULATION LOGIC (NEW - ASSIGNMENT BASED)
# =============================================================================

def calculate_payroll_with_assignments(evv_df):
    """
    Calculate payroll using the assignment-based logic:

    1. For each client, sum all staff hours worked
    2. Get client's POS from database
    3. If total <= POS: pay everyone as worked
    4. If total > POS: find who exceeded their assignment, cap them

    Returns:
        - staff_client_details: DataFrame with per-client breakdown
        - staff_summary: DataFrame with totals per staff
        - issues: List of any issues found (missing assignments, etc.)
    """

    # Get reference data from database
    client_pos_map = db.get_client_pos_map()
    assignment_map = db.get_assignment_map()

    # Group EVV by client and staff
    grouped = evv_df.groupby(['Client Name', 'Staff Name']).agg({
        'Service Duration (hours)': 'sum',
        'Units': 'sum'
    }).reset_index()

    grouped.columns = ['Client Name', 'Staff Name', 'Hours Worked', 'Units Worked']
    grouped['Hours Worked'] = grouped['Hours Worked'].round(2)

    results = []
    issues = []

    # Process each client
    for client_name in grouped['Client Name'].unique():
        client_data = grouped[grouped['Client Name'] == client_name].copy()
        total_worked = client_data['Hours Worked'].sum()

        # Get POS from database, fallback to 0 (no limit) if not found
        pos_limit = client_pos_map.get(client_name, 0)

        if pos_limit == 0:
            issues.append(f"Client '{client_name}' not found in database or has no POS set")
            # If no POS defined, pay as worked
            for _, row in client_data.iterrows():
                results.append({
                    'Client Name': client_name,
                    'Staff Name': row['Staff Name'],
                    'POS Limit': pos_limit,
                    'Assigned Hours': None,
                    'Hours Worked': row['Hours Worked'],
                    'Payable Hours': row['Hours Worked'],
                    'Hours Reduced': 0,
                    'Status': 'No POS Set'
                })
            continue

        # Check if under POS
        if total_worked <= pos_limit:
            # Under or at POS - pay everyone as worked
            for _, row in client_data.iterrows():
                assigned = assignment_map.get((row['Staff Name'], client_name))
                results.append({
                    'Client Name': client_name,
                    'Staff Name': row['Staff Name'],
                    'POS Limit': pos_limit,
                    'Assigned Hours': assigned,
                    'Hours Worked': row['Hours Worked'],
                    'Payable Hours': row['Hours Worked'],
                    'Hours Reduced': 0,
                    'Status': 'OK'
                })
        else:
            # Over POS - need to find who overworked and cap them
            over_by = total_worked - pos_limit

            for _, row in client_data.iterrows():
                staff_name = row['Staff Name']
                hours_worked = row['Hours Worked']
                assigned = assignment_map.get((staff_name, client_name))

                if assigned is None:
                    # No assignment found - flag it
                    issues.append(f"No assignment found for '{staff_name}' on client '{client_name}'")
                    # Default: cap at their proportion of POS
                    proportion = hours_worked / total_worked
                    payable = round(pos_limit * proportion, 2)
                    status = 'No Assignment - Prorated'
                else:
                    # Has assignment - check if they overworked it
                    if hours_worked > assigned:
                        # Overworked - cap at assignment
                        payable = assigned
                        status = 'Capped at Assignment'
                    else:
                        # Didn't overwork - pay as worked
                        payable = hours_worked
                        status = 'OK'

                results.append({
                    'Client Name': client_name,
                    'Staff Name': staff_name,
                    'POS Limit': pos_limit,
                    'Assigned Hours': assigned,
                    'Hours Worked': hours_worked,
                    'Payable Hours': payable,
                    'Hours Reduced': round(hours_worked - payable, 2),
                    'Status': status
                })

    # Create results DataFrame
    staff_client_details = pd.DataFrame(results)

    if len(staff_client_details) == 0:
        return pd.DataFrame(), pd.DataFrame(), issues

    # Create staff summary
    staff_summary = staff_client_details.groupby('Staff Name').agg({
        'Hours Worked': 'sum',
        'Payable Hours': 'sum',
        'Hours Reduced': 'sum'
    }).reset_index()

    staff_summary.columns = ['Staff Name', 'Total Hours Worked', 'Total Hours Payable', 'Total Hours Reduced']
    staff_summary = staff_summary.round(2)
    staff_summary = staff_summary.sort_values('Total Hours Payable', ascending=False)

    return staff_client_details, staff_summary, issues


def analyze_week(evv_df):
    """Analyze a single week's EVV data."""
    details, summary, issues = calculate_payroll_with_assignments(evv_df)
    return {
        'details': details,
        'summary': summary,
        'issues': issues,
        'total_worked': summary['Total Hours Worked'].sum() if len(summary) > 0 else 0,
        'total_payable': summary['Total Hours Payable'].sum() if len(summary) > 0 else 0,
        'total_reduced': summary['Total Hours Reduced'].sum() if len(summary) > 0 else 0,
    }


# =============================================================================
# ADMIN UI - STAFF MANAGEMENT
# =============================================================================

def render_staff_management():
    """Render the staff management UI."""
    st.subheader("Staff Management")

    # Add new staff form
    with st.expander("Add New Staff Member", expanded=st.session_state.show_add_staff):
        with st.form("add_staff_form"):
            new_name = st.text_input("Staff Name")
            submitted = st.form_submit_button("Add Staff", type="primary")

            if submitted and new_name:
                try:
                    db.add_staff(new_name)
                    st.success(f"Added '{new_name}' to staff")
                    st.rerun()
                except Exception as e:
                    if "UNIQUE constraint" in str(e):
                        st.error(f"Staff member '{new_name}' already exists")
                    else:
                        st.error(f"Error: {str(e)}")

    # List existing staff
    staff_list = db.get_all_staff(active_only=False)

    if not staff_list:
        st.info("No staff members yet. Add your first one above.")
        return

    st.markdown(f"**Total: {len(staff_list)} staff members**")

    # Display as editable table
    for staff in staff_list:
        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])

        with col1:
            if staff['is_active']:
                st.write(f"**{staff['name']}**")
            else:
                st.write(f"~~{staff['name']}~~ (inactive)")

        with col2:
            # Show assignment count
            assignments = db.get_assignments_by_staff(staff['id'])
            st.caption(f"{len(assignments)} assignments")

        with col3:
            if staff['is_active']:
                if st.button("Deactivate", key=f"deact_staff_{staff['id']}", type="secondary"):
                    db.update_staff(staff['id'], is_active=False)
                    st.rerun()
            else:
                if st.button("Activate", key=f"act_staff_{staff['id']}", type="primary"):
                    db.update_staff(staff['id'], is_active=True)
                    st.rerun()

        with col4:
            if st.button("Delete", key=f"del_staff_{staff['id']}", type="secondary"):
                db.delete_staff(staff['id'])
                st.rerun()


# =============================================================================
# ADMIN UI - CLIENT MANAGEMENT
# =============================================================================

def render_client_management():
    """Render the client management UI."""
    st.subheader("Client Management")

    # Get family groups for dropdown
    family_groups = db.get_all_family_groups()
    family_group_options = {fg['name']: fg['id'] for fg in family_groups}
    family_group_options['None'] = 0

    # Add new family group
    with st.expander("Create Family Group (for siblings)"):
        with st.form("add_family_group_form"):
            fg_name = st.text_input("Family Group Name", placeholder="e.g., 'Smith Family'")
            fg_notes = st.text_input("Notes (optional)")
            fg_submitted = st.form_submit_button("Create Family Group")

            if fg_submitted and fg_name:
                db.add_family_group(fg_name, fg_notes)
                st.success(f"Created family group '{fg_name}'")
                st.rerun()

    # Add new client form
    with st.expander("Add New Client", expanded=st.session_state.show_add_client):
        with st.form("add_client_form"):
            col1, col2 = st.columns(2)

            with col1:
                client_name = st.text_input("Client Name")
                pos_hours = st.number_input("Weekly POS Hours", min_value=0.0, step=0.5)

            with col2:
                is_private = st.checkbox("Private Client (no EVV)")
                family_group = st.selectbox(
                    "Family Group (optional)",
                    options=list(family_group_options.keys()),
                    index=0
                )

            notes = st.text_input("Notes (optional)")
            submitted = st.form_submit_button("Add Client", type="primary")

            if submitted and client_name:
                try:
                    fg_id = family_group_options.get(family_group, 0)
                    db.add_client(
                        client_name,
                        pos_hours,
                        is_private,
                        fg_id if fg_id > 0 else None,
                        notes
                    )
                    st.success(f"Added client '{client_name}' with {pos_hours} POS hours")
                    st.rerun()
                except Exception as e:
                    if "UNIQUE constraint" in str(e):
                        st.error(f"Client '{client_name}' already exists")
                    else:
                        st.error(f"Error: {str(e)}")

    # List existing clients
    clients = db.get_all_clients(active_only=False)

    if not clients:
        st.info("No clients yet. Add your first one above.")
        return

    st.markdown(f"**Total: {len(clients)} clients**")

    # Group by family if applicable
    for client in clients:
        col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])

        with col1:
            name_display = f"**{client['name']}**"
            if not client['is_active']:
                name_display = f"~~{client['name']}~~ (inactive)"
            if client['is_private']:
                name_display += " [Private]"
            if client.get('family_group_name'):
                name_display += f" ({client['family_group_name']})"
            st.write(name_display)

        with col2:
            st.caption(f"POS: {client['pos_hours']}h")

        with col3:
            # Quick edit POS
            new_pos = st.number_input(
                "POS",
                value=float(client['pos_hours']),
                min_value=0.0,
                step=0.5,
                key=f"pos_{client['id']}",
                label_visibility="collapsed"
            )
            if new_pos != client['pos_hours']:
                db.update_client(client['id'], pos_hours=new_pos)
                st.rerun()

        with col4:
            if client['is_active']:
                if st.button("Deactivate", key=f"deact_client_{client['id']}"):
                    db.update_client(client['id'], is_active=False)
                    st.rerun()
            else:
                if st.button("Activate", key=f"act_client_{client['id']}"):
                    db.update_client(client['id'], is_active=True)
                    st.rerun()

        with col5:
            private_toggle = st.checkbox(
                "Private",
                value=bool(client['is_private']),
                key=f"private_{client['id']}",
                label_visibility="collapsed"
            )
            if private_toggle != bool(client['is_private']):
                db.update_client(client['id'], is_private=private_toggle)
                st.rerun()


# =============================================================================
# ADMIN UI - ASSIGNMENTS MANAGEMENT
# =============================================================================

def render_assignments_management():
    """Render the assignments management UI."""
    st.subheader("Staff Assignments")
    st.caption("Define which staff work for which clients and their assigned hours")

    # Get data for dropdowns
    staff_list = db.get_all_staff()
    clients_list = db.get_all_clients()

    if not staff_list:
        st.warning("Add staff members first in the Staff tab")
        return

    if not clients_list:
        st.warning("Add clients first in the Clients tab")
        return

    staff_options = {s['name']: s['id'] for s in staff_list}
    client_options = {c['name']: c['id'] for c in clients_list}
    client_pos = {c['name']: c['pos_hours'] for c in clients_list}

    # Add new assignment form
    with st.expander("Add New Assignment", expanded=True):
        with st.form("add_assignment_form"):
            col1, col2, col3 = st.columns(3)

            with col1:
                selected_staff = st.selectbox("Staff Member", options=list(staff_options.keys()))

            with col2:
                selected_client = st.selectbox("Client", options=list(client_options.keys()))
                if selected_client:
                    st.caption(f"Client POS: {client_pos.get(selected_client, 0)} hours")

            with col3:
                assigned_hours = st.number_input("Assigned Hours (per week)", min_value=0.0, step=0.5)

            col4, col5 = st.columns(2)
            with col4:
                is_permanent = st.checkbox("Permanent Assignment", value=True)
            with col5:
                notes = st.text_input("Notes (optional)")

            submitted = st.form_submit_button("Add Assignment", type="primary")

            if submitted and selected_staff and selected_client and assigned_hours > 0:
                try:
                    db.upsert_assignment(
                        staff_options[selected_staff],
                        client_options[selected_client],
                        assigned_hours,
                        is_permanent,
                        notes
                    )
                    st.success(f"Assigned {selected_staff} to {selected_client} for {assigned_hours} hours/week")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {str(e)}")

    # View assignments by client
    st.divider()
    st.markdown("### Current Assignments by Client")

    for client in clients_list:
        assignments = db.get_assignments_by_client(client['id'])
        total_assigned = sum(a['assigned_hours'] for a in assignments)

        status_icon = ""
        if total_assigned == client['pos_hours']:
            status_icon = "✅"
        elif total_assigned > client['pos_hours']:
            status_icon = "⚠️"
        elif total_assigned < client['pos_hours'] and total_assigned > 0:
            status_icon = "📝"

        with st.expander(f"{status_icon} {client['name']} - POS: {client['pos_hours']}h | Assigned: {total_assigned}h"):
            if not assignments:
                st.info("No staff assigned to this client yet")
            else:
                for a in assignments:
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

                    with col1:
                        perm_badge = "🔒" if a['is_permanent'] else "📅"
                        st.write(f"{perm_badge} **{a['staff_name']}**")

                    with col2:
                        new_hours = st.number_input(
                            "Hours",
                            value=float(a['assigned_hours']),
                            min_value=0.0,
                            step=0.5,
                            key=f"asn_hrs_{a['id']}",
                            label_visibility="collapsed"
                        )
                        if new_hours != a['assigned_hours']:
                            db.update_assignment(a['id'], assigned_hours=new_hours)
                            st.rerun()

                    with col3:
                        st.caption(f"{a['assigned_hours']}h / week")

                    with col4:
                        if st.button("Remove", key=f"del_asn_{a['id']}"):
                            db.delete_assignment(a['id'])
                            st.rerun()

            # Show remaining hours
            remaining = client['pos_hours'] - total_assigned
            if remaining > 0:
                st.caption(f"⏳ {remaining} hours still unassigned for this client")
            elif remaining < 0:
                st.warning(f"⚠️ Over-assigned by {abs(remaining)} hours!")


# =============================================================================
# ADMIN UI - IMPORT FROM EVV
# =============================================================================

def render_import_section():
    """Render the import from EVV section."""
    st.subheader("Import from EVV File")
    st.caption("Automatically import staff and client names from an EVV file")

    uploaded_file = st.file_uploader(
        "Upload EVV Excel file",
        type=['xlsx'],
        key='import_evv_file'
    )

    if uploaded_file:
        evv_df = read_evv_report(uploaded_file)

        if evv_df is not None:
            # Extract unique names
            staff_names = evv_df['Staff Name'].dropna().unique().tolist()
            client_names = evv_df['Client Name'].dropna().unique().tolist()

            col1, col2 = st.columns(2)

            with col1:
                st.markdown(f"**Found {len(staff_names)} staff members**")

                # Check which are new
                existing_staff = {s['name'].lower() for s in db.get_all_staff(active_only=False)}
                new_staff = [n for n in staff_names if n.lower() not in existing_staff]

                if new_staff:
                    st.write(f"New: {len(new_staff)}")
                    with st.expander("View new staff"):
                        for name in new_staff:
                            st.write(f"• {name}")

                    if st.button("Import Staff", type="primary"):
                        db.import_staff_from_names(staff_names)
                        st.success(f"Imported {len(new_staff)} new staff members")
                        st.rerun()
                else:
                    st.success("All staff already in database")

            with col2:
                st.markdown(f"**Found {len(client_names)} clients**")

                # Check which are new
                existing_clients = {c['name'].lower() for c in db.get_all_clients(active_only=False)}
                new_clients = [n for n in client_names if n.lower() not in existing_clients]

                if new_clients:
                    st.write(f"New: {len(new_clients)}")
                    with st.expander("View new clients"):
                        for name in new_clients:
                            st.write(f"• {name}")

                    if st.button("Import Clients", type="primary"):
                        db.import_clients_from_names(client_names, default_pos=0)
                        st.success(f"Imported {len(new_clients)} new clients (POS set to 0 - update manually)")
                        st.rerun()
                else:
                    st.success("All clients already in database")


# =============================================================================
# PAYROLL UI - WEEK ANALYSIS
# =============================================================================

def render_week_tab(week_num: int):
    """Render a week's upload and analysis UI."""
    week_key = f'week{week_num}'

    st.subheader(f"Week {week_num} Data Upload")

    # File upload
    st.markdown("**Upload EVV Services Rendered Report**")
    evv_file = st.file_uploader(
        f"Upload Week {week_num} EVV Excel file",
        type=['xlsx'],
        key=f'{week_key}_evv_uploader'
    )

    if evv_file:
        with st.spinner("Reading file..."):
            evv_df = read_evv_report(evv_file)

            if evv_df is not None:
                st.session_state[f'{week_key}_evv_data'] = evv_df
                st.success(f"Loaded {len(evv_df)} service records")

                # Preview
                with st.expander("Preview Data"):
                    preview_cols = ['Staff Name', 'Client Name', 'Service Duration (hours)']
                    if 'Units' in evv_df.columns:
                        preview_cols.append('Units')
                    st.dataframe(evv_df[preview_cols].head(10), use_container_width=True)

    # Analyze button
    if st.session_state.get(f'{week_key}_evv_data') is not None:
        if st.button(f"Calculate Week {week_num} Payroll", type="primary", key=f'analyze_{week_key}'):
            with st.spinner("Calculating..."):
                results = analyze_week(st.session_state[f'{week_key}_evv_data'])
                st.session_state[f'{week_key}_results'] = results
                st.success("Calculation complete!")

        # Show results
        if st.session_state.get(f'{week_key}_results'):
            results = st.session_state[f'{week_key}_results']

            st.divider()

            # Show any issues
            if results['issues']:
                with st.expander(f"⚠️ {len(results['issues'])} Issues Found", expanded=True):
                    for issue in results['issues']:
                        st.warning(issue)

            # Metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Hours Worked", f"{results['total_worked']:.2f}")
            with col2:
                st.metric("Hours Payable", f"{results['total_payable']:.2f}")
            with col3:
                delta = -results['total_reduced'] if results['total_reduced'] > 0 else 0
                st.metric("Hours Reduced", f"{results['total_reduced']:.2f}", delta=f"{delta:.2f}")

            # Staff summary
            st.markdown("### Staff Payroll Summary")
            if len(results['summary']) > 0:
                st.dataframe(results['summary'], use_container_width=True, hide_index=True)

            # Detailed breakdown
            with st.expander("View Detailed Breakdown by Client"):
                if len(results['details']) > 0:
                    st.dataframe(results['details'], use_container_width=True, hide_index=True)


# =============================================================================
# PAYROLL UI - SUMMARY
# =============================================================================

def render_payroll_summary():
    """Render the bi-weekly payroll summary."""
    st.subheader("Bi-Weekly Payroll Summary")

    week1_results = st.session_state.get('week1_results')
    week2_results = st.session_state.get('week2_results')

    if not week1_results and not week2_results:
        st.info("Upload and analyze data in Week 1 and/or Week 2 tabs first")
        return

    # Combine summaries
    if week1_results and week2_results:
        w1 = week1_results['summary'].copy()
        w2 = week2_results['summary'].copy()

        combined = pd.merge(
            w1,
            w2,
            on='Staff Name',
            how='outer',
            suffixes=(' W1', ' W2')
        ).fillna(0)

        combined['Total Hours Worked'] = (
            combined['Total Hours Worked W1'] + combined['Total Hours Worked W2']
        ).round(2)
        combined['Total Hours Payable'] = (
            combined['Total Hours Payable W1'] + combined['Total Hours Payable W2']
        ).round(2)
        combined['Total Hours Reduced'] = (
            combined['Total Hours Reduced W1'] + combined['Total Hours Reduced W2']
        ).round(2)

    elif week1_results:
        combined = week1_results['summary'].copy()
    else:
        combined = week2_results['summary'].copy()

    combined = combined.sort_values('Total Hours Payable', ascending=False)

    # Metrics
    st.markdown("### Pay Period Totals")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Staff", len(combined))
    with col2:
        st.metric("Total Hours Worked", f"{combined['Total Hours Worked'].sum():.2f}")
    with col3:
        st.metric("Total Hours Payable", f"{combined['Total Hours Payable'].sum():.2f}")
    with col4:
        reduced = combined['Total Hours Reduced'].sum()
        st.metric("Total Hours Reduced", f"{reduced:.2f}", delta=f"-{reduced:.2f}" if reduced > 0 else None)

    st.divider()

    # Staff with reductions warning
    staff_with_reductions = combined[combined['Total Hours Reduced'] > 0]
    if len(staff_with_reductions) > 0:
        st.warning(f"⚠️ {len(staff_with_reductions)} staff had hours reduced due to POS overages")

    # Summary table
    st.markdown("### Staff Payroll - Use 'Total Hours Payable' for payment")

    # Simplified view for payroll
    payroll_view = combined[['Staff Name', 'Total Hours Worked', 'Total Hours Payable', 'Total Hours Reduced']].copy()
    st.dataframe(payroll_view, use_container_width=True, hide_index=True)

    # Export
    st.divider()

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        payroll_view.to_excel(writer, index=False, sheet_name='Payroll Summary')

        if week1_results:
            week1_results['summary'].to_excel(writer, index=False, sheet_name='Week 1 Summary')
            week1_results['details'].to_excel(writer, index=False, sheet_name='Week 1 Details')

        if week2_results:
            week2_results['summary'].to_excel(writer, index=False, sheet_name='Week 2 Summary')
            week2_results['details'].to_excel(writer, index=False, sheet_name='Week 2 Details')

    st.download_button(
        label="Download Payroll Report (Excel)",
        data=buffer.getvalue(),
        file_name=f"payroll_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )


# =============================================================================
# MAIN APP
# =============================================================================

def main():
    init_session_state()

    # Sidebar navigation
    with st.sidebar:
        st.title("💰 Cornerstone Payroll")
        st.divider()

        page = st.radio(
            "Navigation",
            options=['Payroll', 'Admin'],
            label_visibility="collapsed"
        )

        st.session_state.current_page = page.lower()

        st.divider()

        # Quick stats
        staff_count = len(db.get_all_staff())
        client_count = len(db.get_all_clients())
        assignment_count = len(db.get_all_assignments())

        st.caption("Database Stats")
        st.write(f"👤 {staff_count} Staff")
        st.write(f"🏠 {client_count} Clients")
        st.write(f"📋 {assignment_count} Assignments")

    # Main content
    if st.session_state.current_page == 'payroll':
        st.title("Payroll Calculator")

        tab1, tab2, tab3 = st.tabs(["Week 1", "Week 2", "Summary"])

        with tab1:
            render_week_tab(1)

        with tab2:
            render_week_tab(2)

        with tab3:
            render_payroll_summary()

    elif st.session_state.current_page == 'admin':
        st.title("Admin Panel")

        tab1, tab2, tab3, tab4 = st.tabs(["Staff", "Clients", "Assignments", "Import"])

        with tab1:
            render_staff_management()

        with tab2:
            render_client_management()

        with tab3:
            render_assignments_management()

        with tab4:
            render_import_section()


if __name__ == "__main__":
    main()
