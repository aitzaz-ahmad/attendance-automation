import time
import json
import base64
from datetime import datetime, timedelta, date
import gspread
from google.cloud import pubsub_v1
from oauth2client.service_account import ServiceAccountCredentials

#constans defining information to communicate with Google Sheets
GDRIVE_SCOPE = 'https://www.googleapis.com/auth/drive' 
GSPREAD_SCOPE = 'https://spreadsheets.google.com/feeds'
SCOPES = [GSPREAD_SCOPE, GDRIVE_SCOPE]

GDRIVE_CREDS_JSON = 'attend1-gdrive-creds.json'
CREDS = ServiceAccountCredentials.from_json_keyfile_name(GDRIVE_CREDS_JSON,
                                                         SCOPES)

#constants defined for generating the Attendace Review Sheet template
SPREAD_SHEET_NAME = 'Attendance Review - {} {}'
RAW_DATA_DK = 'Raw Data - DK'
RAW_DATA_STAFF = 'Raw Data - Staff'
RAW_DATA_SHADMAN = 'Raw Data - Shadman'
DAILY_ATTENDANCE = 'Daily Attendance'
WEEKLY_SUMMARY = 'Weekly Summary'

MINIMUM_COLS = '10'
DEFAULT_COLS = '50'
DEFAULT_ROWS = '1000'
ROLE_WRITER = 'writer'
ROLE_READER = 'reader'
PER_LEVEL_USER = 'user'

#email ids of the personnel with whom the attendance review sheet is supposed to be shared
SARAH_ANWAR = 'sarah.anwar@tintash.com'
RAFIA_MASUD = 'rafia.masud@tintash.com'
IQRA_AMJAD = 'iqra.amjad@tintash.com'
MUDDASSIR_MUNEER = 'muddassir.muneer@tintash.com'
SELF = 'aitzaz@tintash.com'
INTERNAL_TOOLS = 'internaltools@tintash.com'

#constants defined for the fomulae to be added to the Attendance Review Sheet template
CHECK_OUT_CELL = 'INDIRECT(ADDRESS(ROW()-1, COLUMN()))'
CHECK_IN_CELL = 'INDIRECT(ADDRESS(ROW()-2, COLUMN()))'
FORMULA_HOURS_WORKED = '=MINUS({}, {})'.format(CHECK_OUT_CELL, CHECK_IN_CELL)

CUT_OFF_TIME_CELL = 'INDIRECT(ADDRESS(ROW() - 2, 2))'
CHECK_IN_AVERAGE_CELL = 'INDIRECT(ADDRESS(ROW() - 2, COLUMN()))'
COMPARISON_CONDITION = 'LTE({}, {})'.format(CHECK_IN_AVERAGE_CELL,
                                            CUT_OFF_TIME_CELL)
FORMULA_WEEKLY_PENALTY = '=IF({}, "No", "Yes")'.format(COMPARISON_CONDITION)

#constants defined for accessing the 'Review Periods' sheet
WORKSHEET_CUT_OFF_TIMES = 'Cut Off Times'
SHEET_ID_REVIEW_PERIODS = '1gwCUph21sKUTEmI1Wxk2l34hq00iuPNLyEPlF_8DxDI'

#constants for GCP pub/sub
PROJECT_ID = 'attend1'
TOPIC_NEW_REVIEW_SHEET = 'new_review_sheet'

#constants for console messages
RUNTIME_EXCEPTION = 'Runtime exception encountered by {}.\nException: {}'
WORKSHEET_NOT_FOUND_EXCEPTION = 'ERROR: Unable to find {} worksheet in the {} spreadsheet.'
SPREADSHEET_NOT_FOUND_EXCEPTION = 'ERROR: Unable to find an attendance review sheet with the title {}'

def load_employees():
    employees_info = {}

    #open and read the sheet for containing employee names, emails and cut off times
    client = gspread.authorize(CREDS)
    spreadsheet = client.open_by_key(SHEET_ID_REVIEW_PERIODS)

    try:
        #read the review period entries from the sheet
        worksheet = spreadsheet.worksheet(WORKSHEET_CUT_OFF_TIMES)
        employees_data = worksheet.get_all_records()

        print('found data for {} employees'.format(len(employees_data)))

        for record in employees_data: 
            user_id = record['Email'].split('@')[0]
            
            employees_info[user_id] = {}
            employees_info[user_id]['name'] = record['Name']
            employees_info[user_id]['email'] = record['Email']
            employees_info[user_id]['cut-off'] = record['Cut Off Time']
        
    except gspread.exceptions.WorksheetNotFound as ex:
        print (WORKSHEET_NOT_FOUND_EXCEPTION.format(WORKSHEET_CUT_OFF_TIMES,
                                                    spreadsheet.title))
    except Exception as ex:
        print (RUNTIME_EXCEPTION.format('load_employees', ex))
    finally:
        return employees_info

def review_spreadsheet_exists(review_month, review_year, force_remove=False):
    ss_name = SPREAD_SHEET_NAME.format(review_month, review_year)
    client = gspread.authorize(CREDS)
    spreadsheet = None

    try:
        spreadsheet = client.open(ss_name)
        
        if force_remove:
            client.del_spreadsheet(spreadsheet.id) 
    except gspread.exceptions.SpreadsheetNotFound as ex:
        print(SPREADSHEET_NOT_FOUND_EXCEPTION.format(ss_name))
    except Exception as ex:
        print (RUNTIME_EXCEPTION.format('review_spreadsheet_exists', ex))
    finally:
        return spreadsheet

def create_spreadsheet(review_month, review_year, employees, start_date, end_date):
    #generate the name of the attendance sheet for the review period
    ss_name = SPREAD_SHEET_NAME.format(review_month, review_year)
    
    client = gspread.authorize(CREDS)

    try:
        spreadsheet = client.create(ss_name)
        
        #add the required worksheets for the attendance review process
        raw_data_staff = spreadsheet.add_worksheet(title=RAW_DATA_STAFF,
                                                   rows=DEFAULT_ROWS,
                                                   cols=DEFAULT_COLS)
        raw_data_dk = spreadsheet.add_worksheet(title=RAW_DATA_DK,
                                                rows=DEFAULT_ROWS,
                                                cols=DEFAULT_COLS)
        raw_data_sh = spreadsheet.add_worksheet(title=RAW_DATA_SHADMAN, 
                                                rows=DEFAULT_ROWS,
                                                cols=DEFAULT_COLS)
        daily_attendance = spreadsheet.add_worksheet(title=DAILY_ATTENDANCE,
                                                     rows=len(employees)*3+2,
                                                     cols=DEFAULT_COLS)
        weekly_summary = spreadsheet.add_worksheet(title=WEEKLY_SUMMARY,
                                                   rows=len(employees)*3+2,
                                                   cols=MINIMUM_COLS)
        
        #delete the worksheet added by default (via the create spreadsheet call)
        spreadsheet.del_worksheet(spreadsheet.sheet1)
        
        #populate the worksheet templates
        create_raw_data_template(raw_data_staff)
        create_raw_data_template(raw_data_dk)
        create_raw_data_template(raw_data_sh)
        create_daily_attendance_template(spreadsheet,
                                         DAILY_ATTENDANCE,
                                         start_date,
                                         end_date,
                                         employees)
        create_weekly_summary_template(spreadsheet,
                                       WEEKLY_SUMMARY,
                                       start_date,
                                       end_date,
                                       employees)
        
        #share the spreadsheet with the appropriate personnel
        spreadsheet.share(INTERNAL_TOOLS, perm_type=PER_LEVEL_USER,
                          role=ROLE_WRITER)
        spreadsheet.share(SARAH_ANWAR, perm_type=PER_LEVEL_USER,
                          role=ROLE_WRITER)
        spreadsheet.share(RAFIA_MASUD, perm_type=PER_LEVEL_USER,
                          role=ROLE_WRITER)
        spreadsheet.share(IQRA_AMJAD, perm_type=PER_LEVEL_USER,
                          role=ROLE_WRITER)
        spreadsheet.share(MUDDASSIR_MUNEER, perm_type=PER_LEVEL_USER,
                          role=ROLE_WRITER)
        spreadsheet.share(SELF, perm_type=PER_LEVEL_USER,
                          role=ROLE_READER)
        
    except Exception as ex:
        print(RUNTIME_EXCEPTION.format('create_spreadsheet', ex))
        
        if spreadsheet is not None:
            #delete the spreadsheet because the template creation didn't
            #complete successfully
            client.del_spreadsheet(spreadsheet.id)
            spreadsheet = None
    finally:
        return spreadsheet

def generate_query_params(value_ip_option, insert_data_option):
    query_params = {}
    query_params['valueInputOption'] = value_ip_option
    query_params['insertDataOption'] = insert_data_option
    return query_params

def generate_request_body(data_range, major_dimensions, values):
    request_body = {}
    request_body['majorDimension'] = major_dimensions
    request_body['values'] = values
    return request_body

def compute_data_range(worksheet_name, elements_per_row):
    last_column = 'A' * (elements_per_row // 26)
    ascii_val = (elements_per_row % 26) + 64
    last_column += chr(ascii_val)
    return '{}!A1:{}1'.format(worksheet_name, last_column)

def create_raw_data_template(worksheet):
    worksheet.append_row(['Timestamp', 'Employee', 'Location', 'Entry'])

def create_daily_attendance_template(spreadsheet, worksheet_name, start_date,
                                     end_date, employees):
    print('creating daily attendance template...')
    
    row_title = ['Employee', 'Entry']
    row_days  = ['', '']

    #find the Sunday of the week with the end_date for the review period
    week_end = None
    if end_date.weekday() == 6:
        week_end = end_date
    else:
        week_end = end_date + timedelta(days=6-end_date.weekday())

    date = start_date
    while date <= week_end:
        row_days.append(date.strftime('%A'))
        row_title.append(date.strftime('%d-%m-%Y'))
        date += timedelta(days=1)
    
    total_cols = len(row_title)
    data_range = compute_data_range(worksheet_name, total_cols)
    query_params = generate_query_params('USER_ENTERED', 'OVERWRITE')
    request_body = generate_request_body(data_range,
                                         'ROWS',
                                         [row_title, row_days])
    spreadsheet.values_append(data_range,
                              query_params,
                              request_body)
    
    col_day = []
    col_entry = []
    col_employee = []

    for employee in employees:
        col_employee.extend([employee, '', ''])
        col_entry.extend(['Check In', 'Check Out', 'Hours Worked'])
        col_day.extend(['', '', FORMULA_HOURS_WORKED])

    values = [col_employee, col_entry]
    for i in range(total_cols - 2):
        values.append(col_day)
    
    request_body = generate_request_body(data_range, 'COLUMNS', values)
    spreadsheet.values_append(data_range,
                              query_params,
                              request_body)

def create_weekly_summary_template(spreadsheet, worksheet_name, start_date,
                                   end_date, employees):
    print('creating weekly summary template...')
    first_monday = (start_date - timedelta(days=start_date.weekday()))
    last_monday = (end_date - timedelta(days=end_date.weekday()))
    #the difference of the Mondays alone does not factor in the last week
    total_weeks = 1 + (last_monday - first_monday).days // 7 
    log_msg = 'review period details:-\nstart date: {}\nend date: {}\ntotal weeks: {}'
    print(log_msg.format(start_date, end_date, total_weeks))
    
    row_title = ['Employee', 'Cut-Off Time', 'Details']
    for count in range(total_weeks):
        row_title.append('Week {}'.format(count+1))
    
    total_cols = len(row_title)
    data_range = compute_data_range(worksheet_name, total_cols)
    query_params = generate_query_params('USER_ENTERED', 'OVERWRITE')
    request_body = generate_request_body(data_range, 'ROWS', [row_title])
    spreadsheet.values_append(data_range,
                              query_params,
                              request_body)    
    
    col_week = []
    col_details = []
    col_cut_off = []
    col_employee = []

    for employee in employees:
        employee_info = employees[employee]
        col_employee.extend([employee, '', ''])
        col_cut_off.extend([employee_info['cut-off'], '', ''])
        col_details.extend(['Weekly Average', 'Hours Worked', 'Penalties'])
        col_week.extend(['', '', FORMULA_WEEKLY_PENALTY])
    
    values = [col_employee, col_cut_off, col_details]
    for i in range(total_cols - 3):
        values.append(col_week)
    
    request_body = generate_request_body(data_range, 'COLUMNS', values)
    spreadsheet.values_append(data_range,
                              query_params,
                              request_body)

def pub_review_sheet_info(sheet_id, sheet_name):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NEW_REVIEW_SHEET)

    review_sheet_info = {}
    review_sheet_info['id'] = sheet_id
    review_sheet_info['name'] = sheet_name
 
    #convert the review_sheet_info object to a utf-8 encoded json string
    payload = json.dumps(review_sheet_info)
    future_response = publisher.publish(topic_path, payload.encode('utf-8'))
    
    #the script execution will get blocked on the following call until a
    #message has been successfully published
    message_id = future_response.result()
    log_msg = 'message id: {}, payload: {}, published to {} topic.'
    print(log_msg.format(message_id, payload, TOPIC_NEW_REVIEW_SHEET))

def entry_point(event, context):
    """
    Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    query_params = json.loads(pubsub_message)
    end_date = datetime.strptime(query_params['end_date'], '%m/%d/%Y')
    start_date = datetime.strptime(query_params['start_date'],
                                   '%m/%d/%Y')
    review_year = query_params['sheetname']
    review_month = query_params['month']
    print('attendance review sheet request for {} {}'.format(review_month,
                                                             review_year))
    #proceed with the review sheet creation logic if and only if the review
    #period's spreadsheet does not exist
    attendance_review_sheet = review_spreadsheet_exists(review_month, review_year)
    if attendance_review_sheet is None:
        print('creating attendance review sheet...')
        #load the employees list and their cut off times
        employees_info = load_employees()
        attendance_review_sheet = create_spreadsheet(review_month,
                                                     review_year,
                                                     employees_info,
                                                     start_date,
                                                     end_date)
    
    review_sheet_id = attendance_review_sheet.id
    review_sheet_name = attendance_review_sheet.title
    log_msg = 'attendance review sheet id: {}\nattendance review sheet name: {}'
    print(log_msg.format(review_sheet_id, review_sheet_name))

    #publish the review sheet info to the GCP pub/sub topic
    pub_review_sheet_info(review_sheet_id, review_sheet_name)

