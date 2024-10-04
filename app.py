import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime
import base64
from io import BytesIO

# Function to fetch distinct values for dropdowns
def fetch_dropdown_values(city_code):
    DB_HOST = '34.100.223.97'
    DB_PORT = 5432
    DB_NAME = 'trips'
    DB_USER = 'postgres'
    DB_PASSWORD = 'theimm0rtaL'

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    if city_code and city_code != 'All':
        site_query = f"SELECT DISTINCT client_office FROM etms_trips WHERE lower(city_code) = '{city_code.lower()}' ORDER BY client_office"
        sp_query = f"SELECT DISTINCT driver_lithium_id FROM etms_trips WHERE lower(city_code) = '{city_code.lower()}' ORDER BY driver_lithium_id"
        vehicle_query = f"SELECT DISTINCT vehicle_reg_no FROM etms_trips WHERE lower(city_code) = '{city_code.lower()}' ORDER BY vehicle_reg_no"
    else:
        site_query = "SELECT DISTINCT client_office FROM etms_trips ORDER BY client_office"
        sp_query = "SELECT DISTINCT driver_lithium_id FROM etms_trips ORDER BY driver_lithium_id"
        vehicle_query = "SELECT DISTINCT vehicle_reg_no FROM etms_trips ORDER BY vehicle_reg_no"

    site_names = pd.read_sql_query(site_query, conn)['client_office'].tolist()
    sp_lithium_ids = pd.read_sql_query(sp_query, conn)['driver_lithium_id'].tolist()
    vehicle_reg_nos = pd.read_sql_query(vehicle_query, conn)['vehicle_reg_no'].tolist()

    conn.close()

    return site_names, sp_lithium_ids, vehicle_reg_nos

# Function to fetch data from PostgreSQL
def fetch_data(start_date, end_date, city_code, site_name, sp_lithium_id, vehicle_reg_no):
    DB_HOST = '34.100.223.97'
    DB_PORT = 5432
    DB_NAME = 'trips'
    DB_USER = 'postgres'
    DB_PASSWORD = 'theimm0rtaL'

    sql_query = """
    SELECT 
        id,
        city_code,
        client,
        client_office,
        billing_model,
        to_char(trip_date::timestamp,'DD/MM/YYYY') as trip_date,
        trip_type,
        shift_time,
        client_trip_id,
        vehicle_reg_no,
        driver_lithium_id,
        driver_name,
        driver_contact_no,
        planned_pax,
        actual_pax,
        is_escort,
        to_char(client_trip_allocation_time::timestamp, 'DD/MM/YYYY HH24:MI') as client_trip_allocation_time,
        to_char(lithium_cab_allocation_time::timestamp, 'DD/MM/YYYY HH24:MI') as lithium_cab_allocation_time,
        delay_reason,
        ow_planned_distance,
        ow_actual_distance,
        two_way_actual_distance,
        employee_location::jsonb ->>'emp1location' as emp1location,
        employee_location::jsonb ->>'emp2location' as emp2location,
        employee_location::jsonb ->>'emp3location' as emp3location,
        employee_location::jsonb ->>'emp4location' as emp4location,
        to_char(first_pickup_driver_reportin_time::timestamp, 'DD/MM/YYYY HH24:MI') as first_pickup_driver_reportin_time,
        to_char(first_emp_planned_pick_time::timestamp, 'DD/MM/YYYY HH24:MI') as first_emp_planned_pick_time,
        to_char(first_emp_actual_pick_time::timestamp, 'DD/MM/YYYY HH24:MI') as first_emp_actual_pick_time,
        to_char(last_emp_planned_drop_time::timestamp, 'DD/MM/YYYY HH24:MI') as last_emp_planned_drop_time,
        to_char(last_emp_actual_drop_time::timestamp, 'DD/MM/YYYY HH24:MI') as last_emp_actual_drop_time,
        to_char(branch_office_reporting_time::timestamp, 'DD/MM/YYYY HH24:MI') as branch_office_reporting_time,
        isb2b,
        b2b_trip_id,
        to_char(planned_start_time::timestamp, 'DD/MM/YYYY HH24:MI') as planned_start_time,
        to_char(actual_start_time::timestamp, 'DD/MM/YYYY HH24:MI') as actual_start_time,
        to_char(planned_end_time::timestamp, 'DD/MM/YYYY HH24:MI') as planned_end_time,
        to_char(actual_end_time::timestamp, 'DD/MM/YYYY HH24:MI') as actual_end_time,
        trip_status,
        remarks
    FROM 
        etms_trips
    WHERE 
        trip_date >= %s AND trip_date <= %s 
        AND (%s = '' OR lower(city_code) = %s)
        AND (%s = '' OR lower(client_office) = %s)
        AND (%s = '' OR lower(driver_lithium_id) = %s)
        AND (%s = '' OR lower(vehicle_reg_no) = %s)
    """

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    df = pd.read_sql_query(sql_query, conn, params=(start_date, end_date, city_code, city_code, site_name, site_name, sp_lithium_id, sp_lithium_id, vehicle_reg_no, vehicle_reg_no))

    conn.close()

    return df

# Streamlit app
def main():
    st.title('Trip Data Analysis')

    # Date range filter
    start_date = st.date_input('Start Date')
    end_date = st.date_input('End Date')

    # City filter
    city_options = ['All', 'BLR', 'CHN', 'NCR-Nodia', 'NCR-GGN', 'HYD','PUN','MUM']  # Add more cities as needed
    selected_city = st.selectbox('Select City', city_options)

    # Fetch dropdown values for site name, SP lithium ID, and vehicle reg number
    site_names, sp_lithium_ids, vehicle_reg_nos = fetch_dropdown_values(selected_city)

    # Site Name filter
    site_name_options = ['All'] + site_names
    selected_site_name = st.selectbox('Select Site Name', site_name_options)

    # SP Lithium ID filter
    sp_lithium_id_options = ['All'] + sp_lithium_ids
    selected_sp_lithium_id = st.selectbox('Select SP Lithium ID', sp_lithium_id_options)

    # Vehicle Registration Number filter
    vehicle_reg_no_options = ['All'] + vehicle_reg_nos
    selected_vehicle_reg_no = st.selectbox('Select Vehicle Registration Number', vehicle_reg_no_options)

    # Display SQL query dynamically
    if start_date and end_date:
        sql_query = f"""
        SELECT 
        id, city_code, client, client_office, billing_model, 
        to_char(trip_date::timestamp,'DD/MM/YYYY') as trip_date, trip_type, shift_time, 
        client_trip_id, vehicle_reg_no, driver_lithium_id, driver_name, driver_contact_no, 
        planned_pax, actual_pax, is_escort, 
        to_char(client_trip_allocation_time::timestamp, 'DD/MM/YYYY HH24:MI') as client_trip_allocation_time, 
        to_char(lithium_cab_allocation_time::timestamp, 'DD/MM/YYYY HH24:MI') as lithium_cab_allocation_time, 
        delay_reason, ow_planned_distance, ow_actual_distance, two_way_actual_distance, 
        employee_location::jsonb ->>'emp1location' as emp1location,
        employee_location::jsonb ->>'emp2location' as emp2location,
        employee_location::jsonb ->>'emp3location' as emp3location,
        employee_location::jsonb ->>'emp4location' as emp4location, 
        to_char(first_pickup_driver_reportin_time::timestamp, 'DD/MM/YYYY HH24:MI') as first_pickup_driver_reportin_time, 
        to_char(first_emp_planned_pick_time::timestamp, 'DD/MM/YYYY HH24:MI') as first_emp_planned_pick_time, 
        to_char(first_emp_actual_pick_time::timestamp, 'DD/MM/YYYY HH24:MI') as first_emp_actual_pick_time, 
        to_char(last_emp_planned_drop_time::timestamp, 'DD/MM/YYYY HH24:MI') as last_emp_planned_drop_time, 
        to_char(last_emp_actual_drop_time::timestamp, 'DD/MM/YYYY HH24:MI') as last_emp_actual_drop_time, 
        to_char(branch_office_reporting_time::timestamp, 'DD/MM/YYYY HH24:MI') as branch_office_reporting_time, 
        isb2b, b2b_trip_id, 
        to_char(planned_start_time::timestamp, 'DD/MM/YYYY HH24:MI') as planned_start_time, 
        to_char(actual_start_time::timestamp, 'DD/MM/YYYY HH24:MI') as actual_start_time, 
        to_char(planned_end_time::timestamp, 'DD/MM/YYYY HH24:MI') as planned_end_time, 
        to_char(actual_end_time::timestamp, 'DD/MM/YYYY HH24:MI') as actual_end_time, 
        trip_status, remarks
        FROM etms_trips
        WHERE trip_date >= '{start_date}' AND trip_date <= '{end_date}' 
        AND ('{selected_city}' = 'All' OR lower(city_code) = lower('{selected_city}')) 
        AND ('{selected_site_name}' = 'All' OR lower(client_office) = lower('{selected_site_name}')) 
        AND ('{selected_sp_lithium_id}' = 'All' OR lower(driver_lithium_id) = lower('{selected_sp_lithium_id}')) 
        AND ('{selected_vehicle_reg_no}' = 'All' OR lower(vehicle_reg_no) = lower('{selected_vehicle_reg_no}'));
        """
        st.code(sql_query)

    if st.button('Fetch Data'):
        if start_date and end_date:
            df = fetch_data(start_date, end_date, selected_city, selected_site_name, selected_sp_lithium_id, selected_vehicle_reg_no)
            if not df.empty:
                st.dataframe(df)

                # Option to download the data
                csv = df.to_csv(index=False).encode('utf-8')
                b64 = base64.b64encode(csv).decode()
                link = f'<a href="data:file/csv;base64,{b64}" download="trip_data.csv">Download CSV</a>'
                st.markdown(link, unsafe_allow_html=True)
            else:
                st.warning('No data found for the selected filters.')

if __name__ == '__main__':
    main()
