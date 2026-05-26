import time
import json
import base64
from datetime import datetime, timedelta
import gspread
from google.cloud import pubsub_v1
from oauth2client.service_account import ServiceAccountCredentials

#constans defined for communicating with Google Sheets & GCP
GDRIVE_SCOPE = 'https://www.googleapis.com/auth/drive' 
GSPREAD_SCOPE = 'https://spreadsheets.google.com/feeds'
SCOPES = [GSPREAD_SCOPE, GDRIVE_SCOPE]

GDRIVE_CREDS_JSON = 'attend1-gdrive-creds.json'
CREDS = ServiceAccountCredentials.from_json_keyfile_name(GDRIVE_CREDS_JSON,
                                                         SCOPES)

#constants defined for accessing the worksheets in the Attendance Review spreadsheet
WEEKLY_SUMMARY = 'Weekly Summary'
DAILY_ATTENDANCE = 'Daily Attendance'
RAW_DATA_SHEET_NAME = 'Raw Data - {}'
RAW_DATA_SHEET_STAFF = 'Staff'
RAW_DATA_SHEET_COLUMNS = 4

#contstants defined for accessing the Review Periods Google Sheet 
WORKSHEET_CUT_OFF_TIMES = 'Cut Off Times'
SHEET_ID_REVIEW_PERIODS = '1gwCUph21sKUTEmI1Wxk2l34hq00iuPNLyEPlF_8DxDI'

#constants defined for the fomulae to be added to the Attendance Review Sheet template
CHECK_OUT_CELL = 'INDIRECT(ADDRESS(ROW()-1, COLUMN()))'
CHECK_IN_CELL = 'INDIRECT(ADDRESS(ROW()-2, COLUMN()))'
FORMULA_HOURS_WORKED = '=MINUS({}, {})'.format(CHECK_OUT_CELL, CHECK_IN_CELL)

CUT_OFF_TIME_CELL = 'INDIRECT(ADDRESS(ROW() - 2, 2))'
CHECK_IN_AVERAGE_CELL = 'INDIRECT(ADDRESS(ROW() - 2, COLUMN()))'
COMPARISON_CONDITION = 'LTE({}, {})'.format(CHECK_IN_AVERAGE_CELL,
                                            CUT_OFF_TIME_CELL)
FORMULA_WEEKLY_PENALTY = '=IF({}, "No", "Yes")'.format(COMPARISON_CONDITION)

#constant defined for storing the weekly stats
TIME_DURATION_FORMAT = '{:02d}:{:02d}:{:02d}.000'

#constants for GCP pub/sub
PROJECT_ID = 'attend1'
TOPIC_LAST_STORED_TIMESTAMP = 'last_stored_timestamp'

#constants for console messages
WORKSHEET_NOT_FOUND_EXCEPTION = 'ERROR: Unable to find {} worksheet in the {} spreadsheet.'

def pub_last_stored_timestamp(device_id, timestamp):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_LAST_STORED_TIMESTAMP)

    last_stored_record = {}
    last_stored_record['device_id'] = device_id
    last_stored_record['timestamp'] = timestamp
 
    #convert the review_sheet_info object to a utf-8 encoded json string
    payload = json.dumps(last_stored_record)
    future_response = publisher.publish(topic_path,
                                        payload.encode('utf-8'),
                                        location=device_id)
    
    #the script execution will get blocked on the following call until
    #a message has been successfully published
    message_id = future_response.result()
    log_msg = 'message id: {}, payload: {}, published to {} topic' 
    print(log_msg.format(message_id, payload, TOPIC_LAST_STORED_TIMESTAMP))

def generate_query_params(value_ip_option,
                          insert_data_option=None):
    query_params = {}
    query_params['valueInputOption'] = value_ip_option
    
    if insert_data_option is not None:
        query_params['insertDataOption'] = insert_data_option
    
    return query_params

def generate_request_body(data_range, major_dimensions, values):
    request_body = {}
    request_body['majorDimension'] = major_dimensions
    request_body['values'] = values
    return request_body

def compute_data_range(worksheet_name,
                       elements_per_row,
                       first_row=1,
                       last_row=1):
    last_column = 'A' * (elements_per_row // 26)
    ascii_val = (elements_per_row % 26) + 64
    last_column += chr(ascii_val)
    return '{}!A{}:{}{}'.format(worksheet_name,
                                first_row,
                                last_column,
                                last_row)

def store_attendance_records(review_sheet_id,
                             review_start_date,
                             worksheet_suffix,
                             attendance_records):
    """
    filters and stores the raw attendance records for Tintash employees
    in the 'Raw Data - XX' worksheet (where XX corresponds to worksheet
    suffix) in the Attendance Review Sheet
    """
    client = gspread.authorize(CREDS)
    spreadsheet = client.open_by_key(review_sheet_id)
    
    worksheet_name = RAW_DATA_SHEET_NAME.format(worksheet_suffix)
    worksheet = spreadsheet.worksheet(worksheet_name)
    stored_records = worksheet.get_all_records()
    
    recent_time = None 
    if len(stored_records) is 0:
        recent_time = review_start_date
    else:
        recent_time = datetime.strptime(stored_records[-1]['Timestamp'],
                                        '%d-%m-%Y %H:%M:%S')

    last_stored_timestamp = recent_time.strftime('%d-%m-%Y %H:%M:%S')
    records_to_store = []
    for record in attendance_records:
        entry_timestamp = datetime.strptime(record['timestamp'],
                                            '%d-%m-%Y %H:%M:%S')
        if entry_timestamp > recent_time:
            last_stored_timestamp =  record['timestamp']
            records_to_store.append([record['timestamp'],
                                  record['username'],
                                  record['device'],
                                  record['entry']])

    #batch write all the raw attendance records to avoid exceeding
    #the quota limit of 100 write requests per user per 100 seconds
    #for Google Sheets
    data_range = compute_data_range(worksheet_name,
                                    RAW_DATA_SHEET_COLUMNS)
    query_params = generate_query_params('USER_ENTERED', 'OVERWRITE')
    request_body = generate_request_body(data_range,
                                         'ROWS',
                                         records_to_store)
    spreadsheet.values_append(data_range,
                              query_params,
                              request_body)

    return last_stored_timestamp

def filter_and_store_attendance(review_sheet_id,
                                review_start_date,
                                device_location,
                                raw_attendance_records):
    """
    filters staff and employee attendance records and stores them
    separately in their respective worksheets, and returns the timestamp
    of the last stored attendance record
    """
    #filter staff and employee raw attendance and store them separately
    print('storing attendance records of staff...')
    staff_records = [record for record in raw_attendance_records
                     if ' ' in record['username']]
    store_attendance_records(review_sheet_id,
                             review_start_date,
                             RAW_DATA_SHEET_STAFF,
                             staff_records)
    print('storing attendance records of employees...')
    employee_records = [record for record in raw_attendance_records
                        if ' ' not in record['username']]
    last_stored_timestamp = store_attendance_records(review_sheet_id,
                                                     review_start_date,
                                                     device_location,
                                                     employee_records)
    #format the raw data for computing and storing the daily attendance 
    print('last stored attendance record\'s timestamp: ' + 
          last_stored_timestamp)

    return last_stored_timestamp

def load_raw_data(review_sheet_id, start_date, device_id):
    """
    loads all the raw attendance records stored in the 'Raw Data - XX'
    (where XX corresponds to location postfix for the input device_id)
    worksheet in the Attendance Review Sheet
    """
    client = gspread.authorize(CREDS)
    sheet = client.open_by_key(review_sheet_id)
    
    worksheet_name = RAW_DATA_SHEET_NAME.format(device_id)
    worksheet = sheet.worksheet(worksheet_name)
    stored_records = worksheet.get_all_records()
    
    attendance_records = []
    for record in stored_records:
        entry_timestamp = datetime.strptime(record['Timestamp'],
                                            '%d-%m-%Y %H:%M:%S')
        if entry_timestamp > start_date:
            attendance_records.append(record)
    
    return attendance_records

def format_raw_data(raw_attendance_records):
    """
    compiles the raw data in a formatted manner on a per employee basis
    which makes reading single entries time efficient [O(1)] while 
    updating the Daily Attendance worksheet
    """
    formatted_data = {}

    for record in raw_attendance_records:
        employee = record['Employee']
        if employee not in formatted_data:
            formatted_data[employee] = {}
        
        tokens = record['Timestamp'].split(' ')
        entry_date = tokens[0]
        entry_time = tokens[1]
        if entry_date not in formatted_data[employee]:
            formatted_data[employee][entry_date] = {}
        if record['Entry'] == 'Check In':
            if 'Check In' not in formatted_data[employee][entry_date]:
                formatted_data[employee][entry_date]['Check In'] = entry_time
            else:
                formatted_data[employee][entry_date]['Check Out'] = entry_time
        if record['Entry'] == 'Check Out':
            formatted_data[employee][entry_date]['Check Out'] = entry_time
        
    return formatted_data

def format_attendance_records(review_sheet_id, start_date, device_location):
    """
    loads and formats the raw attendance records from Raw Data worksheet (in
    the Attendance Review Sheet) since the 'start_date' received from the 
    biometric device at 'device_location' 
    """
    raw_attendance_data = load_raw_data(review_sheet_id,
                                        start_date,
                                        device_location)
    log_msg = 'formatting {} raw attendance records...'
    print(log_msg.format(len(raw_attendance_data)))
    formatted_data = format_raw_data(raw_attendance_data)

    return formatted_data

def load_existing_employees(review_sheet_id):
    """
    loads the employees that are already listed in the Attendance
    Review Sheet for the current review period
    """
    client = gspread.authorize(CREDS)
    spreadsheet = client.open_by_key(review_sheet_id)
    try:
        existing_employees = []
        worksheet = spreadsheet.worksheet(WEEKLY_SUMMARY)
        weekly_summary_data = worksheet.get_all_values()
        for row in range(1, len(weekly_summary_data), 3):
            existing_employees.append(weekly_summary_data[row][0])
        
        print('found {} existing employees'.format(len(existing_employees)))
    except gspread.exceptions.WorksheetNotFound as e:
        print (WORKSHEET_NOT_FOUND_EXCEPTION.format(WEEKLY_SUMMARY,
                                                    spreadsheet.title))
    except Exception as e:
        print ('Runtime exception encountered: {}'.format(e))
    finally:
        return existing_employees

def mark_unknown_employees(existing_employees, formatted_attendance_records):
    """
    identifies and returns the list of unknown employees who are not listed 
    in the attendance review sheet template for the running attendance review 
    period
    """
    unknown_employees = []

    for employee in formatted_attendance_records:
        if employee not in existing_employees:
            unknown_employees.append(employee)
    
    return unknown_employees

def load_cut_offs(unknown_employees):
    """
    loads the cut off times of the unknown employees from the 'Check In Times'
    worksheet (in the Review Periods spreadsheet)
    """
    print ('loading cut off times for the new hires...')

    #open the sheet containing employee names, emails and cut off times
    client = gspread.authorize(CREDS)
    spreadsheet = client.open_by_key(SHEET_ID_REVIEW_PERIODS)

    employees_info = {}
    try:
        #read the employees' info from the worksheet
        worksheet = spreadsheet.worksheet(WORKSHEET_CUT_OFF_TIMES)
        employees_data = worksheet.get_all_records()
        
        for record in employees_data: 
            user_id = record['Email'].split('@')[0]
            if user_id in unknown_employees:
                #the unknown employee is actually a new hire!
                print('copying info of {}'.format(user_id))
                employees_info[user_id] = {}
                employees_info[user_id]['cut-off'] = record['Cut Off Time']
        
    except gspread.exceptions.WorksheetNotFound as e:
        print (WORKSHEET_NOT_FOUND_EXCEPTION.format(WORKSHEET_CUT_OFF_TIMES,
                                                    spreadsheet.title))
    except Exception as e:
        print ('Runtime exception encountered: {}'.format(e))
    finally:
        return employees_info

def append_to_daily_attendance_sheet(spreadsheet, new_employees): 
    """
    updates the Daily Attendance worksheet in the Attendance Review
    Sheet by adding entries for the new employees at the bottom of
    the worksheet
    """
    worksheet = spreadsheet.worksheet(DAILY_ATTENDANCE)
    records = worksheet.get_all_values()
    total_cols = len(records[0])
    
    col_day = []
    col_entry = []
    col_employee = []
    print('adding {} employees to {}'.format(len(new_employees), worksheet.title))
    for employee in new_employees:
        col_employee.extend([employee, '', ''])
        col_entry.extend(['Check In', 'Check Out', 'Hours Worked'])
        col_day.extend(['', '', FORMULA_HOURS_WORKED])
    
    values = [col_employee, col_entry]
    for i in range(total_cols - 2):
        values.append(col_day)
    
    data_range = compute_data_range(worksheet.title, total_cols)
    query_params = generate_query_params('USER_ENTERED', 'OVERWRITE')
    request_body = generate_request_body(data_range, 'COLUMNS', values)
    spreadsheet.values_append(data_range,
                              query_params,
                              request_body)
    
    print('new hires added to {} sheet'.format(worksheet.title))

def append_to_weekly_summary_sheet(spreadsheet, new_employees):
    """
    updates the Weekly Summary worksheet in the Attendance Review
    Sheet by adding entries for the new employees at the bottom of
    the worksheet
    """
    worksheet = spreadsheet.worksheet(WEEKLY_SUMMARY)
    records = worksheet.get_all_values()
    total_cols = len(records[0])
    
    col_week = []
    col_details = []
    col_cut_off = []
    col_employee = []

    for employee in new_employees:
        employee_info = new_employees[employee]
        col_employee.extend([employee, '', ''])
        col_cut_off.extend([employee_info['cut-off'], '', ''])
        col_details.extend(['Weekly Average', 'Hours Worked', 'Penalties'])
        col_week.extend(['', '', FORMULA_WEEKLY_PENALTY])
    
    values = [col_employee, col_cut_off, col_details]
    for i in range(total_cols - 3):
        values.append(col_week)
    
    data_range = compute_data_range(worksheet.title, total_cols)
    query_params = generate_query_params('USER_ENTERED', 'OVERWRITE')
    request_body = generate_request_body(data_range, 'COLUMNS', values)
    spreadsheet.values_append(data_range,
                              query_params,
                              request_body)
    
    print('new hires added to {} sheet'.format(worksheet.title))

def add_employees_to_review_sheet(review_sheet_id, employees_info):
    """
    adds the new employees to the Daily Attendance and Weekly 
    Summary worksheets in the Attendance Review spreadsheet for
    the running review period
    """

    client = gspread.authorize(CREDS)
    spreadsheet = client.open_by_key(review_sheet_id)
    print ('adding new hires to the {}...'.format(spreadsheet.title))

    try:
        append_to_daily_attendance_sheet(spreadsheet, employees_info)
        append_to_weekly_summary_sheet(spreadsheet, employees_info)

    except Exception as e:
        print ('Runtime exception encountered: {}'.format(e))

def identify_and_add_new_hires(review_sheet_id,
                               device_location,
                               formatted_attendance):
    """
    identifies unknown users, if any, and adds their information to the 
    Attendance Review Sheet template for this attendance review period 
    if they are newly hired employees
    """
    #identify any unknown employees that do not exist in the
    #Attendance Review Sheet template
    print('looking for any unknown employees...')
    old_employees = load_existing_employees(review_sheet_id)
    unknown_employees = mark_unknown_employees(old_employees,
                                               formatted_attendance)
    
    new_hires_info = {}
    if len(unknown_employees) > 0: 
        #The attedance records include data of unknown employees. if these
        #employees are new hires, we need to update our template for the
        #Attendance Review Sheet to include their data as well.
        print ('attendance records received for unkown employees...')
        new_hires_info = load_cut_offs(unknown_employees)
        if len(new_hires_info) > 0:
            #new employees were hired and their users were created on the
            #biometric device after the commencement of this Attendance Review
            #period
            print('{} new employees have been hired.', len(new_hires_info))
            add_employees_to_review_sheet(review_sheet_id, new_hires_info)
        else:
            #the unknown employees represent the employees who left Tintash
            #after the start of this attendance review period. the People Ops
            #team deleted their user from the biometric device before the 
            #completion of the running Attendance Review period.
            log_msg = 'unknown employees correspond to ex-employees removed from the {} biometric device.'
            print(log_msg.format(device_location))
    else:
        print('no attendance records received for any unknown employees')

def update_entry_timestamp(stored_timestamp, new_timestamp, entry_type):
    """
    returns whether the new timestamp should replace the previously 
    stored timestamp (in the daily attendance sheet) for a given 
    entry type or not 
    """
    update_flag = stored_timestamp is ''
    if update_flag is False:
        old_entry = datetime.strptime(stored_timestamp, '%H:%M:%S')
        new_entry = datetime.strptime(new_timestamp, '%H:%M:%S')
        if entry_type == 'Check In' and new_entry < old_entry:
            update_flag = True
        elif entry_type == 'Check Out' and new_entry > old_entry:
            update_flag = True
    
    return update_flag

def compile_daily_attendance(worksheet, formatted_attendance):
    """
    compiles the daily attendance data by consolidating the 
    stored daily attendance and the input formatted attendance 
    """
    compiled_attendance = worksheet.get_all_values()
    
    for column in range(2, len(compiled_attendance[0])):
        calendar_date = compiled_attendance[0][column]
        for row in range(2, len(compiled_attendance)):
            employee = compiled_attendance[row][0]
            entry_type = compiled_attendance[row][1]
            if entry_type == 'Hours Worked':
                compiled_attendance[row][column] = FORMULA_HOURS_WORKED
            else:
                row_idx = row if entry_type == 'Check In' else row - 1
                employee = compiled_attendance[row_idx][0]
                if employee in formatted_attendance:
                    employee_attendance = formatted_attendance[employee]
                    if calendar_date in employee_attendance:
                        daily_attendance = employee_attendance[calendar_date]
                        if entry_type in daily_attendance:
                            timestamp = daily_attendance[entry_type]
                            stored_timestamp = compiled_attendance[row][column]
                            ret_val = update_entry_timestamp(stored_timestamp,
                                                             timestamp,
                                                             entry_type)
                            if ret_val is True:
                                compiled_attendance[row][column] = timestamp

    return compiled_attendance

def update_daily_attendance(review_sheet_id, formatted_attendance_data):
    """
    updates the attendence entries in Daily Attendance worksheet in the
    Attendance Review spreadsheet based on the received input data
    """
    client = gspread.authorize(CREDS)
    spreadsheet = client.open_by_key(review_sheet_id)
    worksheet = spreadsheet.worksheet(DAILY_ATTENDANCE)
    compiled_attendance = compile_daily_attendance(worksheet,
                                                   formatted_attendance_data)

    #batch write the compiled daily attendance to avoid exceeding the quota
    #limit of 100 write requests per user per 100 seconds for Google Sheets
    data_range = compute_data_range(worksheet.title, 
                                   len(compiled_attendance[0]),
                                   last_row=len(compiled_attendance))
    query_params = generate_query_params('USER_ENTERED')
    request_body = generate_request_body(data_range,
                                         'ROWS',
                                         compiled_attendance)
    spreadsheet.values_update(data_range,
                              query_params,
                              request_body)

def calculate_weekly_stats(cumulative_checkins,
                           days_worked,
                           total_work_duration):
    """
    calculates and returns the average check-in time and the total hours
    worked in a week for compiling the weekly summary
    """
    hours = minutes = seconds = 0
    worked_hours = worked_minutes = worked_seconds = 0
    
    if days_worked > 0:
        check_in_avg = cumulative_checkins/days_worked
        hours, remainder = divmod(check_in_avg.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        worked_hours, remainder = divmod(total_work_duration.seconds, 3600)
        worked_minutes, worked_seconds= divmod(remainder, 60)
        worked_hours = total_work_duration.days * 24 + worked_hours
        
    weekly_check_in_avg = TIME_DURATION_FORMAT.format(hours,
                                                      minutes,
                                                      seconds)
    weekly_hours_worked = TIME_DURATION_FORMAT.format(worked_hours,
                                                      worked_minutes,
                                                      worked_seconds)
     
    return weekly_check_in_avg, weekly_hours_worked

def compile_weekly_stats(review_sheet_id):
    """
    compiles and returns the weekly summary stats based on the attendance
    data in the Daily Attendance worksheet in the Attendance Review Sheet.
    """
    client = gspread.authorize(CREDS)
    sheet = client.open_by_key(review_sheet_id)
    worksheet = sheet.worksheet(DAILY_ATTENDANCE)
    daily_attendance_data = worksheet.get_all_values()

    weekly_summary_data = {}
    for row in range(2, len(daily_attendance_data), 3):
        employee = daily_attendance_data[row][0]
        employee_weekly_summary = weekly_summary_data[employee] = {}

        week = 1
        days_worked = 0
        sum_hours_worked = timedelta()
        sum_check_in_time = timedelta()
        for column in range(2, len(daily_attendance_data[0])):
            timestamp = daily_attendance_data[row][column]

            if timestamp != '': #ensure that the employee checked in on this date
                days_worked += 1
                check_in = datetime.strptime(timestamp, '%H:%M:%S')
                daily_hours_worked = daily_attendance_data[row+2][column]
                hours_worked = datetime.strptime(daily_hours_worked,
                                                 '%H:%M:%S')
                
                sum_check_in_time += timedelta(hours=check_in.hour,
                                               minutes=check_in.minute,
                                               seconds=check_in.second) 
                sum_hours_worked += timedelta(hours=hours_worked.hour,
                                              minutes=hours_worked.minute,
                                              seconds=hours_worked.second)
            
            calendar_date = datetime.strptime(daily_attendance_data[0][column],
                                              '%d-%m-%Y').date()
            if calendar_date.isoweekday() == 7: #denotes Sunday, marks end of the week
                if days_worked > 0:
                    #calculate the weekly stats based on the total number
                    #of days the employee worked during this week 
                    avg_check_in, weekly_hours = calculate_weekly_stats(sum_check_in_time,
                                                                        days_worked,
                                                                        sum_hours_worked)
                    review_week = 'Week {}'.format(week)
                    weekly_stats = employee_weekly_summary[review_week] = {}
                    weekly_stats['Weekly Average'] = avg_check_in
                    weekly_stats['Hours Worked'] = weekly_hours
                #reset variables before moving on to the next week
                week += 1
                days_worked = 0
                sum_hours_worked = timedelta()
                sum_check_in_time = timedelta()
    
    return weekly_summary_data

def compile_weekly_summary(worksheet, compiled_weekly_stats):
    """
    compiles the weekly summary data by consolidating the 
    stored weekly summary and the input weekly stats
    """
    weekly_summary_data = worksheet.get_all_values()
    
    for column in range(3, len(weekly_summary_data[0])):
        review_week = weekly_summary_data[0][column]
        for row in range(1, len(weekly_summary_data)):
            weekly_stat = weekly_summary_data[row][2]
            if weekly_stat == 'Penalties':
                weekly_summary_data[row][column] = FORMULA_WEEKLY_PENALTY
            else:
                row_idx = row if weekly_stat == 'Weekly Average' else row - 1
                employee = weekly_summary_data[row_idx][0]
                if employee in compiled_weekly_stats:
                    employee_weekly_stats = compiled_weekly_stats[employee]
                    if review_week in employee_weekly_stats:
                        review_week_stats = employee_weekly_stats[review_week]
                        if weekly_stat in review_week_stats:
                            compiled_stat = review_week_stats[weekly_stat]
                            stored_stat = weekly_summary_data[row][column]
                            if stored_stat != compiled_stat:
                                weekly_summary_data[row][column] = compiled_stat
    
    return weekly_summary_data

def update_weekly_summary(review_sheet_id):
    """
    updates the weekly stats in Weekly Summary worksheet (in the
    Attendance Review spreadsheet) based on the data in the Daily
    Attendance worksheet
    """
    compiled_weekly_stats = compile_weekly_stats(review_sheet_id)
    
    client = gspread.authorize(CREDS)
    spreadsheet = client.open_by_key(review_sheet_id)
    worksheet = spreadsheet.worksheet(WEEKLY_SUMMARY)

    weekly_summary_data = compile_weekly_summary(worksheet,
                                                 compiled_weekly_stats)
    
    #batch write the compiled weekly summary to avoid exceeding the quota
    #limit of 100 write requests per user per 100 seconds for Google Sheets
    data_range = compute_data_range(worksheet.title, 
                                   len(weekly_summary_data[0]),
                                   last_row=len(weekly_summary_data))
    query_params = generate_query_params('USER_ENTERED')
    request_body = generate_request_body(data_range,
                                         'ROWS',
                                         weekly_summary_data)
    spreadsheet.values_update(data_range,
                              query_params,
                              request_body)

def entry_point(event, context):
    """
    Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    print('parsing request params...')
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    query_params = json.loads(pubsub_message)

    review_sheet_id = query_params['sheet_id']
    device_location = query_params['device_id']
    start_date = query_params['start_date']
    review_start_date = datetime.strptime(start_date, '%m/%d/%Y')
    raw_attendance_records = query_params['records']
    log_msg = '{} attendance records received'
    print(log_msg.format(len(raw_attendance_records)))
    
    #store attendance records and compute daily attendance and weekly summary
    last_stored_timestamp = filter_and_store_attendance(review_sheet_id,
                                                        review_start_date,
                                                        device_location,
                                                        raw_attendance_records)
    formatted_attendance = format_attendance_records(review_sheet_id,
                                                     review_start_date,
                                                     device_location)
    new_hires_info = identify_and_add_new_hires(review_sheet_id,
                                                device_location,
                                                formatted_attendance)
    
    print('updating {} sheet...'.format(DAILY_ATTENDANCE))            
    update_daily_attendance(review_sheet_id, formatted_attendance)
    print('updating {} sheet...'.format(WEEKLY_SUMMARY))
    update_weekly_summary(review_sheet_id)

    #publish the last stored timestamp to the GCP Pub/Sub topic
    pub_last_stored_timestamp(device_location, last_stored_timestamp)

