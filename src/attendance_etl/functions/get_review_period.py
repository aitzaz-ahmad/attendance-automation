import json
import base64
import gspread
from google.cloud import pubsub_v1
from datetime import datetime, timedelta, date
from oauth2client.service_account import ServiceAccountCredentials

#constans for communicating with Google Sheets
GDRIVE_SCOPE = 'https://www.googleapis.com/auth/drive' 
GSPREAD_SCOPE = 'https://spreadsheets.google.com/feeds'
SCOPES = [GSPREAD_SCOPE, GDRIVE_SCOPE]

GDRIVE_CREDS_JSON = 'attend1-gdrive-creds.json'
CREDS = ServiceAccountCredentials.from_json_keyfile_name(GDRIVE_CREDS_JSON,
                                                         SCOPES)

#constants for calculating the next review period
SHEET_ID_REVIEW_PERIODS = '1gwCUph21sKUTEmI1Wxk2l34hq00iuPNLyEPlF_8DxDI'
MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
          'August', 'September', 'October', 'November', 'December']

#constants for GCP pub/sub
PROJECT_ID = 'attend1'
TOPIC_NEW_REVIEW_PERIOD = 'new_review_period'

def pub_review_period(review_period):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NEW_REVIEW_PERIOD)

    #convert the review_period object to a utf-8 encoded json string 
    payload = json.dumps(review_period)
    future_response = publisher.publish(topic_path, payload.encode('utf-8'))
    
    #the script execution will get blocked on the following call until a
    #message has been successfully published
    message_id = future_response.result()
    log_msg = 'message id: {}, payload: {}, published to {} topic.'
    print(log_msg.format(message_id, payload, TOPIC_NEW_REVIEW_PERIOD))

def calculate_next_review_period(last_review_month, sheetname):
    """
    Calculates and returns the sheetname and the month for the next review
    period for which the data needs to be fetched from the 'Review Periods'
    google sheet.
    """
    review_month = MONTHS[(MONTHS.index(last_review_month) + 1) % len(MONTHS)]
    if review_month is 'January':
        #if the month is December for today set sheetname to year + 1, else
        #set the sheetname to the value of the year
        today = date.today()
        sheetname = str(today.year + 1) if today.month is 12 else str(today.year)

    return sheetname, review_month

def get_next_review_period(last_review_period):
    #compute the worksheet name and the review month based on the last 
    #review period
    last_review_month = last_review_period['month']
    review_period_sheet = last_review_period['sheetname']
    sheetname, review_month = calculate_next_review_period(last_review_month, 
                                                           review_period_sheet)
    print('next review month: {}\nsheet name: {}'.format(review_month,
                                                         sheetname))
    
    #open and read the sheet for containing review periods if it exist
    client = gspread.authorize(CREDS)
    spreadsheet = client.open_by_key(SHEET_ID_REVIEW_PERIODS)

    next_review_period = {}
    try:
        #read the review period entries from the sheet
        worksheet = spreadsheet.worksheet(sheetname)
        review_period_table = worksheet.get_all_records()
        
        for record in review_period_table:
            if review_month in record['Month']:
                #populate the next review period's json object to be returned
                next_review_period['month'] = review_month
                next_review_period['start_date'] = record['Start Date']
                next_review_period['end_date'] = record['End Date']
                next_review_period['duration_weeks'] = record['Duration (Weeks)']
                next_review_period['sheetname'] = sheetname
    except gspread.exceptions.WorksheetNotFound as e:
        print ('ERROR: Unable to find review periods for the year ' +
               sheetname + ' in the review period spreadsheet.')
    except Exception as e:
        print ('Runtime exception encountered: {}'.format(e))
    finally: 
        return next_review_period

def entry_point(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    query_params = json.loads(pubsub_message)
    next_review_period = get_next_review_period(query_params)
    print('next_review_period: ', next_review_period)
    
    #publish the next review period's info to the GCP pub/sub topic
    pub_review_period(next_review_period)

