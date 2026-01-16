"""
CORNERSTONE CARE PAYROLL AUDITOR

Compares EVV (what was worked) vs Provider Portal Claims (what got paid)
to identify staff who overworked client POS limits.

Run with: streamlit run payroll_app_v2.py
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO


st.set_page_config(
    page_title="Payroll Hour Auditor",
    page_icon="🔍",
    layout="wide"
)


# ============================================================================
# FILE READING FUNCTIONS
# ============================================================================

def read_evv_report(uploaded_file):
    """Read EVV Services Rendered Report."""
    try:
        # Get headers from row with "Service Date"
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
        df['Weekly POS Hours'] = pd.to_numeric(df['Weekly POS Hours'], errors='coerce')
        
        return df
        
    except Exception as e:
        st.error(f"Error reading EVV file: {str(e)}")
        return None


def read_claims_report(uploaded_file):
    """Read Provider Portal Claims Report."""
    try:
        # Get headers
        df_raw = pd.read_excel(uploaded_file, sheet_name='Provider Claims Report')
        
        # Find header row (contains "Service Date")
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


# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================

def analyze_evv_by_client_and_staff(evv_df):
    """
    Group EVV data by Client + Staff to see who worked how much per client.
    
    Returns DataFrame with columns:
    - Client Name
    - Staff Name
    - Weekly POS Hours (should be same for all staff on that client)
    - Total Hours Worked (by this staff for this client)
    - Total Units (by this staff for this client)
    """
    
    grouped = evv_df.groupby(['Client Name', 'Staff Name']).agg({
        'Service Duration (hours)': 'sum',
        'Units': 'sum',
        'Weekly POS Hours': 'first'  # POS is same across all rows for a client
    }).reset_index()
    
    grouped.columns = ['Client Name', 'Staff Name', 'Hours Worked', 'Units Worked', 'Weekly POS Hours']
    
    # Round
    grouped['Hours Worked'] = grouped['Hours Worked'].round(2)
    grouped['Units Worked'] = grouped['Units Worked'].astype(int)
    
    return grouped


def analyze_claims_by_client(claims_df):
    """
    Group Claims data by Client to see what actually got paid.
    
    Returns DataFrame with columns:
    - Client Name
    - Total Net Units (what Medicaid approved)
    - Total Net Hours (units / 4)
    - Claim Comments (any adjustments)
    """
    
    grouped = claims_df.groupby('Client Name').agg({
        'Net Units': 'sum',
        'Claim Comments': lambda x: ' | '.join([str(c) for c in x if pd.notna(c)])
    }).reset_index()
    
    grouped.columns = ['Client Name', 'Net Units Paid', 'Claim Comments']
    
    # Convert units to hours
    grouped['Net Hours Paid'] = (grouped['Net Units Paid'] / 4).round(2)
    
    # Clean up empty comments
    grouped['Claim Comments'] = grouped['Claim Comments'].replace('', 'No adjustments')
    
    return grouped


def compare_evv_vs_claims(evv_by_client_staff, claims_by_client):
    """
    Compare EVV (worked) vs Claims (paid) to identify discrepancies.
    
    Returns:
    - comparison_df: Detailed comparison by client
    - staff_adjustments_df: What each staff member should be paid
    """
    
    # Group EVV by client only (sum all staff)
    evv_by_client = evv_by_client_staff.groupby('Client Name').agg({
        'Hours Worked': 'sum',
        'Units Worked': 'sum',
        'Weekly POS Hours': 'first'
    }).reset_index()
    
    # Merge EVV with Claims
    comparison = pd.merge(
        evv_by_client,
        claims_by_client,
        on='Client Name',
        how='outer'
    )
    
    comparison = comparison.fillna(0)
    
    # Calculate differences
    comparison['Hours Difference'] = (comparison['Hours Worked'] - comparison['Net Hours Paid']).round(2)
    comparison['Units Difference'] = comparison['Units Worked'] - comparison['Net Units Paid']
    
    # Flag issues
    comparison['Has Discrepancy'] = comparison['Units Difference'] != 0
    comparison['Over POS Limit'] = comparison['Hours Worked'] > comparison['Weekly POS Hours']
    
    # Now calculate staff adjustments
    # For each staff on each client, determine their payable hours
    staff_adjustments = evv_by_client_staff.copy()
    
    # Merge with comparison to get discrepancy info
    staff_adjustments = pd.merge(
        staff_adjustments,
        comparison[['Client Name', 'Net Hours Paid', 'Hours Difference', 'Has Discrepancy', 'Claim Comments']],
        on='Client Name',
        how='left'
    )
    
    # Calculate what each staff should be paid
    # If there's a discrepancy, prorate the approved hours based on what each staff worked
    
    def calculate_payable_hours(row):
        if not row['Has Discrepancy']:
            # No discrepancy - pay what they worked
            return row['Hours Worked']
        else:
            # Discrepancy exists - prorate based on their share of work
            # Get total hours worked on this client by ALL staff
            client_total = evv_by_client[evv_by_client['Client Name'] == row['Client Name']]['Hours Worked'].iloc[0]
            
            if client_total > 0:
                # Their percentage of work
                staff_percentage = row['Hours Worked'] / client_total
                # Apply to approved hours
                payable = row['Net Hours Paid'] * staff_percentage
                return round(payable, 2)
            else:
                return 0
    
    staff_adjustments['Payable Hours'] = staff_adjustments.apply(calculate_payable_hours, axis=1)
    staff_adjustments['Hours Reduced'] = (staff_adjustments['Hours Worked'] - staff_adjustments['Payable Hours']).round(2)
    staff_adjustments['Needs Adjustment'] = staff_adjustments['Hours Reduced'] > 0
    
    return comparison, staff_adjustments


def create_staff_payroll_summary(staff_adjustments_df):
    """
    Create final payroll summary by staff member.
    
    Shows:
    - Total hours worked across all clients
    - Total payable hours (after POS adjustments)
    - Total hours reduced
    """
    
    summary = staff_adjustments_df.groupby('Staff Name').agg({
        'Hours Worked': 'sum',
        'Payable Hours': 'sum',
        'Hours Reduced': 'sum'
    }).reset_index()
    
    summary.columns = ['Staff Name', 'Hours Worked', 'Hours Payable', 'Hours Reduced']
    
    summary = summary.round(2)
    
    # Sort by hours reduced (highest first)
    summary = summary.sort_values('Hours Reduced', ascending=False)
    
    return summary


# ============================================================================
# EXCEL EXPORT
# ============================================================================

def create_excel_export(comparison_df, staff_adjustments_df, staff_summary_df):
    """Create multi-sheet Excel workbook."""
    
    buffer = BytesIO()
    
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        # Sheet 1: Staff Payroll Summary
        staff_summary_df.to_excel(writer, index=False, sheet_name='Staff Payroll Summary')
        
        # Sheet 2: Client Comparison (EVV vs Claims)
        comparison_df.to_excel(writer, index=False, sheet_name='Client Comparison')
        
        # Sheet 3: Detailed Adjustments (by staff and client)
        staff_adjustments_df.to_excel(writer, index=False, sheet_name='Staff-Client Details')
        
        # Sheet 4: Issues Only
        issues = staff_adjustments_df[staff_adjustments_df['Needs Adjustment']]
        if len(issues) > 0:
            issues.to_excel(writer, index=False, sheet_name='Needs Adjustment')
    
    return buffer.getvalue()


# ============================================================================
# STREAMLIT UI
# ============================================================================

def initialize_session_state():
    """Initialize session state."""
    defaults = {
        # Week 1 data
        'week1_evv_data': None,
        'week1_claims_data': None,
        'week1_staff_summary': None,
        
        # Week 2 data
        'week2_evv_data': None,
        'week2_claims_data': None,
        'week2_staff_summary': None,
        
        # Combined results
        'combined_staff_summary': None,
        'comparison_week1': None,
        'comparison_week2': None,
        'staff_adjustments_week1': None,
        'staff_adjustments_week2': None
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def main():
    initialize_session_state()
    
    st.title("🔍 Payroll Hour Auditor")
    st.markdown("Compare EVV hours worked vs Provider Portal Claims for bi-weekly payroll.")
    
    st.divider()
    
    # Create tabs for Week 1, Week 2, and Summary
    tab1, tab2, tab3 = st.tabs(["📅 Week 1", "📅 Week 2", "📊 Payroll Summary"])
    
    # ========================================================================
    # WEEK 1 TAB
    # ========================================================================
    with tab1:
        st.subheader("Week 1 Data Upload")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**📄 EVV Services Rendered Report**")
            week1_evv_file = st.file_uploader(
                "Upload Week 1 EVV Excel file",
                type=['xlsx'],
                key='week1_evv_uploader'
            )
            
            if week1_evv_file:
                with st.spinner("Reading Week 1 EVV file..."):
                    evv_df = read_evv_report(week1_evv_file)
                    
                    if evv_df is not None:
                        st.session_state.week1_evv_data = evv_df
                        st.success(f"✓ Loaded {len(evv_df)} service records")
                        
                        with st.expander("Preview"):
                            st.dataframe(
                                evv_df[['Staff Name', 'Client Name', 'Weekly POS Hours', 
                                       'Service Duration (hours)', 'Units']].head(5),
                                use_container_width=True
                            )
        
        with col2:
            st.markdown("**💰 Provider Portal Claims Report**")
            week1_claims_file = st.file_uploader(
                "Upload Week 1 Claims Excel file",
                type=['xlsx'],
                key='week1_claims_uploader'
            )
            
            if week1_claims_file:
                with st.spinner("Reading Week 1 Claims file..."):
                    claims_df = read_claims_report(week1_claims_file)
                    
                    if claims_df is not None:
                        st.session_state.week1_claims_data = claims_df
                        st.success(f"✓ Loaded {len(claims_df)} claim records")
                        
                        with st.expander("Preview"):
                            st.dataframe(
                                claims_df[['Client Name', 'Net Units', 'Claim Comments']].head(5),
                                use_container_width=True
                            )
        
        # Analyze Week 1 button
        if st.session_state.week1_evv_data is not None and st.session_state.week1_claims_data is not None:
            if st.button("🔍 Analyze Week 1", type="primary", key='analyze_week1'):
                with st.spinner("Analyzing Week 1..."):
                    evv_by_client_staff = analyze_evv_by_client_and_staff(st.session_state.week1_evv_data)
                    claims_by_client = analyze_claims_by_client(st.session_state.week1_claims_data)
                    comparison, staff_adjustments = compare_evv_vs_claims(evv_by_client_staff, claims_by_client)
                    staff_summary = create_staff_payroll_summary(staff_adjustments)
                    
                    st.session_state.week1_staff_summary = staff_summary
                    st.session_state.comparison_week1 = comparison
                    st.session_state.staff_adjustments_week1 = staff_adjustments
                    
                    st.success("✓ Week 1 analysis complete!")
            
            # Show Week 1 results if available
            if st.session_state.week1_staff_summary is not None:
                st.divider()
                st.subheader("Week 1 Results")
                
                summary = st.session_state.week1_staff_summary
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Hours Worked", f"{summary['Hours Worked'].sum():.2f}")
                with col2:
                    st.metric("Hours Payable", f"{summary['Hours Payable'].sum():.2f}")
                with col3:
                    st.metric("Hours Reduced", f"{summary['Hours Reduced'].sum():.2f}")
                
                st.dataframe(summary, use_container_width=True, hide_index=True)
    
    # ========================================================================
    # WEEK 2 TAB
    # ========================================================================
    with tab2:
        st.subheader("Week 2 Data Upload")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**📄 EVV Services Rendered Report**")
            week2_evv_file = st.file_uploader(
                "Upload Week 2 EVV Excel file",
                type=['xlsx'],
                key='week2_evv_uploader'
            )
            
            if week2_evv_file:
                with st.spinner("Reading Week 2 EVV file..."):
                    evv_df = read_evv_report(week2_evv_file)
                    
                    if evv_df is not None:
                        st.session_state.week2_evv_data = evv_df
                        st.success(f"✓ Loaded {len(evv_df)} service records")
                        
                        with st.expander("Preview"):
                            st.dataframe(
                                evv_df[['Staff Name', 'Client Name', 'Weekly POS Hours', 
                                       'Service Duration (hours)', 'Units']].head(5),
                                use_container_width=True
                            )
        
        with col2:
            st.markdown("**💰 Provider Portal Claims Report**")
            week2_claims_file = st.file_uploader(
                "Upload Week 2 Claims Excel file",
                type=['xlsx'],
                key='week2_claims_uploader'
            )
            
            if week2_claims_file:
                with st.spinner("Reading Week 2 Claims file..."):
                    claims_df = read_claims_report(week2_claims_file)
                    
                    if claims_df is not None:
                        st.session_state.week2_claims_data = claims_df
                        st.success(f"✓ Loaded {len(claims_df)} claim records")
                        
                        with st.expander("Preview"):
                            st.dataframe(
                                claims_df[['Client Name', 'Net Units', 'Claim Comments']].head(5),
                                use_container_width=True
                            )
        
        # Analyze Week 2 button
        if st.session_state.week2_evv_data is not None and st.session_state.week2_claims_data is not None:
            if st.button("🔍 Analyze Week 2", type="primary", key='analyze_week2'):
                with st.spinner("Analyzing Week 2..."):
                    evv_by_client_staff = analyze_evv_by_client_and_staff(st.session_state.week2_evv_data)
                    claims_by_client = analyze_claims_by_client(st.session_state.week2_claims_data)
                    comparison, staff_adjustments = compare_evv_vs_claims(evv_by_client_staff, claims_by_client)
                    staff_summary = create_staff_payroll_summary(staff_adjustments)
                    
                    st.session_state.week2_staff_summary = staff_summary
                    st.session_state.comparison_week2 = comparison
                    st.session_state.staff_adjustments_week2 = staff_adjustments
                    
                    st.success("✓ Week 2 analysis complete!")
            
            # Show Week 2 results if available
            if st.session_state.week2_staff_summary is not None:
                st.divider()
                st.subheader("Week 2 Results")
                
                summary = st.session_state.week2_staff_summary
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Hours Worked", f"{summary['Hours Worked'].sum():.2f}")
                with col2:
                    st.metric("Hours Payable", f"{summary['Hours Payable'].sum():.2f}")
                with col3:
                    st.metric("Hours Reduced", f"{summary['Hours Reduced'].sum():.2f}")
                
                st.dataframe(summary, use_container_width=True, hide_index=True)
    
    # ========================================================================
    # SUMMARY TAB (Combined Results)
    # ========================================================================
    with tab3:
        st.subheader("Bi-Weekly Payroll Summary")
        
        # Check if both weeks are analyzed
        if st.session_state.week1_staff_summary is not None or st.session_state.week2_staff_summary is not None:
            
            # Combine Week 1 and Week 2 results
            week1 = st.session_state.week1_staff_summary
            week2 = st.session_state.week2_staff_summary
            
            # Merge both weeks
            if week1 is not None and week2 is not None:
                combined = pd.merge(
                    week1[['Staff Name', 'Hours Worked', 'Hours Payable', 'Hours Reduced']],
                    week2[['Staff Name', 'Hours Worked', 'Hours Payable', 'Hours Reduced']],
                    on='Staff Name',
                    how='outer',
                    suffixes=(' Week 1', ' Week 2')
                )
                combined = combined.fillna(0)
                
                # Calculate totals
                combined['Total Hours Worked'] = (
                    combined['Hours Worked Week 1'] + combined['Hours Worked Week 2']
                ).round(2)
                combined['Total Hours Payable'] = (
                    combined['Hours Payable Week 1'] + combined['Hours Payable Week 2']
                ).round(2)
                combined['Total Hours Reduced'] = (
                    combined['Hours Reduced Week 1'] + combined['Hours Reduced Week 2']
                ).round(2)
                
            elif week1 is not None:
                combined = week1.copy()
                combined.columns = ['Staff Name', 'Total Hours Worked', 'Total Hours Payable', 'Total Hours Reduced']
            else:
                combined = week2.copy()
                combined.columns = ['Staff Name', 'Total Hours Worked', 'Total Hours Payable', 'Total Hours Reduced']
            
            # Sort by total payable hours
            combined = combined.sort_values('Total Hours Payable', ascending=False)
            
            st.session_state.combined_staff_summary = combined
            
            # Display metrics
            st.markdown("### 💰 Bi-Weekly Pay Period Totals")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Staff", len(combined))
            
            with col2:
                st.metric(
                    "Total Hours Worked",
                    f"{combined['Total Hours Worked'].sum():.2f}"
                )
            
            with col3:
                st.metric(
                    "Total Hours Payable",
                    f"{combined['Total Hours Payable'].sum():.2f}"
                )
            
            with col4:
                st.metric(
                    "Total Hours Reduced",
                    f"{combined['Total Hours Reduced'].sum():.2f}",
                    delta=f"-{combined['Total Hours Reduced'].sum():.2f}",
                    delta_color="inverse"
                )
            
            st.divider()
            
            # Show summary table
            st.markdown("### 👥 Staff Payroll Summary")
            st.markdown("**Use this for payroll - 'Total Hours Payable' is what each staff gets paid**")
            
            # Highlight staff with reductions
            staff_with_reductions = combined[combined['Total Hours Reduced'] > 0]
            if len(staff_with_reductions) > 0:
                st.warning(f"⚠️ {len(staff_with_reductions)} staff had hours reduced due to POS overages")
            
            # Show the table
            st.dataframe(
                combined,
                use_container_width=True,
                hide_index=True
            )
            
            # Download button
            st.divider()
            
            # Create Excel export with all sheets
            buffer = BytesIO()
            
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                # Combined summary (main sheet)
                combined.to_excel(writer, index=False, sheet_name='Payroll Summary')
                
                # Week 1 details
                if st.session_state.week1_staff_summary is not None:
                    st.session_state.week1_staff_summary.to_excel(writer, index=False, sheet_name='Week 1 Summary')
                    if st.session_state.comparison_week1 is not None:
                        st.session_state.comparison_week1.to_excel(writer, index=False, sheet_name='Week 1 Clients')
                    if st.session_state.staff_adjustments_week1 is not None:
                        st.session_state.staff_adjustments_week1.to_excel(writer, index=False, sheet_name='Week 1 Details')
                
                # Week 2 details
                if st.session_state.week2_staff_summary is not None:
                    st.session_state.week2_staff_summary.to_excel(writer, index=False, sheet_name='Week 2 Summary')
                    if st.session_state.comparison_week2 is not None:
                        st.session_state.comparison_week2.to_excel(writer, index=False, sheet_name='Week 2 Clients')
                    if st.session_state.staff_adjustments_week2 is not None:
                        st.session_state.staff_adjustments_week2.to_excel(writer, index=False, sheet_name='Week 2 Details')
            
            excel_bytes = buffer.getvalue()
            
            st.download_button(
                label="💾 Download Bi-Weekly Payroll Report (Excel)",
                data=excel_bytes,
                file_name=f"payroll_biweekly_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        
        else:
            st.info("👆 Upload and analyze files in Week 1 and/or Week 2 tabs first")


if __name__ == "__main__":
    main()