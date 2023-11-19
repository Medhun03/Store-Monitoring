from flask import Flask, request, jsonify
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

app = Flask(__name__)

# Load data from CSVs
store_status_df = pd.read_csv("store_status.csv")
menu_hours_df = pd.read_csv("menu_hours.csv")
store_timezone_df = pd.read_csv("store_timezone.csv")

# Create SQLite database and store data
engine = create_engine('sqlite:///:memory:')
store_status_df.to_sql('store_status', con=engine, index=False)
menu_hours_df.to_sql('menu_hours', con=engine, index=False)
store_timezone_df.to_sql('store_timezone', con=engine, index=False)

def get_menu_hours(store_id):
    # Function to get business hours for a store
    # Returns a tuple (start_time, end_time)
    menu_hours = menu_hours_df[menu_hours_df['store_id'] == store_id]
    if menu_hours.empty:
        return (datetime.min.time(), datetime.max.time())
    else:
        return (
            datetime.strptime(menu_hours['start_time_local'].values[0], '%H:%M:%S').time(),
            datetime.strptime(menu_hours['end_time_local'].values[0], '%H:%M:%S').time()
        )
def calculate_uptime_downtime(store_id, start_time, end_time):
    # Function to calculate uptime and downtime for a store within given time range
    # Returns a tuple (uptime, downtime) in minutes
    start_time = pd.Timestamp(start_time)
    end_time = pd.Timestamp(end_time)
    
    store_status_df['timestamp_utc'] = pd.to_datetime(store_status_df['timestamp_utc'])

    query_condition = f"store_id == {store_id} and {start_time} <= timestamp_utc <= {end_time}"

    uptime = store_status_df.query(f"{query_condition} and status == 'active'")['timestamp_utc'].count() * 60
    downtime = store_status_df.query(f"{query_condition} and status == 'inactive'")['timestamp_utc'].count() * 60
    
    return uptime, downtime


def generate_report(store_id):
    # Function to generate the report for a store
    # Returns a dictionary with the report data
    current_timestamp = store_status_df['timestamp_utc'].max()
    #current_timestamp_utc = datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S.%f %Z')
    current_timestamp_utc = datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S.%f %Z')

    # Assuming we are running the report for the last week
    start_time = current_timestamp_utc - timedelta(days=7)
    end_time = current_timestamp_utc

    start_time = pd.Timestamp(start_time)
    end_time = pd.Timestamp(end_time)
    
    business_start_time, business_end_time = get_menu_hours(store_id)
    intervals = pd.date_range(start=start_time, end=end_time, freq='1H')
    
    report_data = {
        'store_id': store_id,
        'uptime_last_hour': 0,
        'uptime_last_day': 0,
        'uptime_last_week': 0,
        'downtime_last_hour': 0,
        'downtime_last_day': 0,
        'downtime_last_week': 0,
    }
    
    for interval_start in intervals:
        interval_end = interval_start + timedelta(hours=1)
        
        business_start_datetime = datetime.combine(interval_start.date(), business_start_time)
        business_end_datetime = datetime.combine(interval_start.date(), business_end_time)
        
        if business_start_datetime <= interval_start <= business_end_datetime:
            # Inside business hours
            uptime, downtime = calculate_uptime_downtime(store_id, interval_start, interval_end)
            report_data['uptime_last_week'] += uptime
            report_data['downtime_last_week'] += downtime
            
            if interval_start >= current_timestamp_utc - timedelta(hours=1):
                report_data['uptime_last_hour'] += uptime
                report_data['downtime_last_hour'] += downtime
                
            if interval_start >= current_timestamp_utc - timedelta(days=1):
                report_data['uptime_last_day'] += uptime
                report_data['downtime_last_day'] += downtime
    
    return report_data

# API endpoints
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    # This endpoint triggers the report generation
    # Returns a report_id
    # In a real-world scenario, this would be an asynchronous task
    store_id = request.json.get('store_id')
    report_id = generate_report(store_id)
    return jsonify({'report_id': report_id})

@app.route('/get_report', methods=['GET'])
def get_report():
    # This endpoint returns the status of the report or the CSV
    # Returns 'Running' if the report generation is not complete
    # Returns 'Complete' along with the CSV file if the report generation is complete
    report_id = request.args.get('report_id')
    # In a real-world scenario, you would check the status of the report based on the report_id
    # and return the CSV file if the report is complete
    return jsonify({'status': 'Complete', 'report_data': generate_report(1)})

if __name__ == '__main__':
    app.run(debug=True)
