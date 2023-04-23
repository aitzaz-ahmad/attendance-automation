import os
import time
import json
from zk import ZK, const
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
from google.protobuf import duration_pb2

#constant(s) for communicating with the zkteco biometric device
DEVICE_TIME_OUT = 10

#constants defined for delay/sleep intervals 
POLLING_DELAY = timedelta(minutes=15)
DEEP_SLEEP_DURATION = timedelta(hours=1)

#constants defined for on-disk files
SNAPSHOT_FILE = "snapshot.json"
REVIEW_PERIOD_JSON = 'review_period.json'
BIOMETRIC_DEVICE_CONFIG_FILE = 'biometric_device_config.json'

#constants defined for GCP pub/sub message exchange
PROJECT_ID = 'attend1'
GET_REVIEW_PERIOD_TOPIC = 'get_review_period'
NEW_REVIEW_PERIOD_TOPIC = 'new_review_period'
CREATE_REVIEW_SHEET_TOPIC = 'create_review_sheet'
NEW_REVIEW_SHEET_TOPIC = 'new_review_sheet'
STORE_ATTEND_RECORDS_TOPIC = 'store_attend_records'
LAST_STORED_TIMESTAMP_TOPIC = 'last_stored_timestamp'

SUBSCRIPTION_NAME = 'sub_{}_{}'
ACK_DEADLINE = 10 #lease time (in seconds) to acknowledge the receipt of a message
SUBSCRIPTION_TTL = 7776000 #90 days (in seconds)
MAX_LIMIT = 1
PULL_MSG_TIMEOUT = 30.0  #How long the subscriber should listen for messages in seconds

#constants defined for the states in the FSM
FETCH_REVIEW_PERIOD = 1
AWAIT_REVIEW_PERIOD = 2
RAISE_FINAL_ALARM = 3
NO_REVIEW_PERIOD = 4
REQUEST_REVIEW_SHEET = 5
AWAIT_REVIEW_SHEET = 6
RELAY_ATTENDANCE_RECORDS = 7
AWAIT_LAST_STORED_TIMESTAMP = 8
REVIEW_PERIOD_EXPIRED = 9

#constants defined for various system wide flags
FINAL_ALARM_RAISED = 1 #0x01
SEND_DAILY_WARNING = 1 << 1 #0x02 

#global variables
HANDLER_TABLE = {}
PI4_STATE = 0
SYSTEM_FLAGS = 0
DEVICE_INFO = None
REVIEW_PERIOD_INFO = None
REVIEW_SHEET_ID = None
LAST_STORED_TIMESTAMP = None

def clear_records_from_device(device_ip, comm_port):
    """
    interface for communicating with the ZKTeco biometric device to  
    clear all the attendance records from the machine 
    """
    conn = None
    zk = ZK(device_ip, port=comm_port, timeout=DEVICE_TIME_OUT,
            force_udp=False, ommit_ping=False)
    try:
        print('Connecting to device ...')
        conn = zk.connect()
        print('Disabling device ...')
        conn.disable_device()
        print('Firmware Version: : {}'.format(conn.get_firmware_version()))

        print('deleting all attendance records stored on the biometric device')
        conn.clear_attendance()
        
        print('Enabling device ...')
        conn.enable_device()
    except Exception as e:
        print("Process terminate : {}".format(e))
    finally:
        if conn:
            conn.disconnect()

def pull_records_from_device(device_ip, comm_port):
    """
    interface for communicating with the ZKTeco biometric device to fetch 
    the attendance records stored on the machine 
    """
    conn = None
    zk = ZK(device_ip, port=comm_port, timeout=DEVICE_TIME_OUT,
            force_udp=False, ommit_ping=False)
    try:
        users = []
        records = []

        print('Connecting to device ...')
        conn = zk.connect()
        print('Disabling device ...')
        conn.disable_device()
        print('Firmware Version: : {}'.format(conn.get_firmware_version()))

        print('Fetching list of users...')
        users = conn.get_users()
        print('Fetching attendance records...')
        records = conn.get_attendance()

        print('Enabling device ...')
        conn.enable_device()
    except Exception as e:
        print("Process terminate : {}".format(e))
    finally:
        if conn:
            conn.disconnect()

        return users, records

def convert_to_map(zk_users):
    """
    converts the list of users fetched from the biometric device
    to a user id vs name map to allow a O(1) lookup for generating
    a human readable equivalent of the attendance entries. 
    """
    print('Generating user id to name mapping for {} users...'.format(len(zk_users)))
    user_mapping = {}
    for zk_user in zk_users:
        user_mapping[zk_user.user_id] = zk_user.name

    return user_mapping

def convert_to_dict(attendance_record, user_mapping):
    """
    converts a ZKTeco attendance record to a dictionary that can
    be readily stored in the datastore.
    """
    name = user_mapping[attendance_record.user_id]
    timestamp = attendance_record.timestamp 
    entry_type = 'Check In' if attendance_record.punch == 0 else 'Check Out'

    record = {}
    record['username'] = name
    record['timestamp'] = timestamp.strftime('%d-%m-%Y %H:%M:%S')
    record['entry'] = entry_type
    
    return record

def decode_zk_format(attendance_records, user_mapping, device_tag):
    """
    decodes the attendance records from the biometric device to
    a python friendly list which can be dumped to the database.
    """
    decoded_records = []
    for entry in attendance_records:
        if entry.user_id in user_mapping:
            #decode attendance records only for the users that
            #have not been deleted from the biometric device
            record = convert_to_dict(entry, user_mapping)
            record['device'] = device_tag
            decoded_records.append(record)

    return decoded_records

def filter_records(records, from_timestamp, to_timestamp=None):
    """
    filters the attendance records fetched from the biometric
    device and returns the subset based on the from and to 
    timestamps. if the to_timestamp param is None, all records
    after the from_timestamp are returned.
    """
    filtered_records = []
    if to_timestamp is None:
        filtered_records = [record for record in records if record.timestamp > from_timestamp]
    elif to_timestamp <= from_timestamp:
        print('Error: Invalid input arguments - to_timstamp must be greater than from_timestamp')
    else:
        filtered_records = [record for record in records if (record.timestamp > from_timestamp and record.timestamp <= to_timestamp)]

    return filtered_records

def get_attendance_records(from_date, to_date=None):
    """
    pulls all attendance records from the biometric device, filters out the
    records based on the from_date and to_date time frame, and decodes the
    filtered records from the zkteco format before returning them
    """
    print('get_attendance_records invoked!')
    
    device_tag = DEVICE_INFO['identifier']
    print('fetching data from device {}'.format(device_tag))
    users, records = pull_records_from_device(DEVICE_INFO['ip'],
                                              DEVICE_INFO['port'])
    print('filtering attendance records since ',
          from_date.strftime('%d-%m-%Y %H:%M:%S'))
    unsaved_records = filter_records(records, from_date, to_date)
    print('{} unsaved attendance records found'.format(len(unsaved_records)))
    user_mapping = convert_to_map(users)
    print('decoding unsaved records from zkteco format...')
    decoded_records = decode_zk_format(unsaved_records,
                                       user_mapping,
                                       device_tag)

    print('{} attendance records decoded'.format(len(decoded_records)))
    
    return decoded_records

def review_period_expired():
    """
    returns True if the date of the system clock is greater than
    the date of the last day of the review period, False otherwise
    """
    last_day = datetime.strptime(REVIEW_PERIOD_INFO['end_date'],
                                        '%m/%d/%Y')
    if last_day.isoweekday() == 5: #denotes Friday
        #consider Sunday as the last day of this review period
        last_day += timedelta(days=2)

    return datetime.today().date() > last_day.date()

def wait_duration_before_next_pull():
    """
    returns the time duration to wait for before attempting to pull 
    the attendance records (from the zkteco device) the next time
    """
    now = datetime.today()
    last_day = datetime.strptime(REVIEW_PERIOD_INFO['end_date'],
                                        '%m/%d/%Y')
    if last_day.isoweekday() == 5: #denotes Friday
        #consider Sunday as the last day of this review period
        last_day += timedelta(days=2)

    wait_duration = POLLING_DELAY
    if now.date() == last_day.date():
        #today's the last day of this review period
        delta_midnight = last_day + timedelta(days=1) - now
        wait_duration = min(POLLING_DELAY, delta_midnight)    

    return wait_duration

def publish_message_to_topic(project_id, topic_name, data):
    """
    converts the input argument 'data' (a dictionary) to a byte array
    and publishes it as a message to the GCP pub/sub topic 'topic_name'
    created under the GCP project identified by 'project_id' 
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    #convert the data object to a utf-8 encoded json string
    payload = json.dumps(data)
    future_response = publisher.publish(topic_path, payload.encode('utf-8'))
    
    #the script execution will get blocked on the following call until a
    #message has been successfully published
    message_id = future_response.result()
    log_msg = 'message id: {}, payload: {}, published to {} topic.'
    print(log_msg.format(message_id, payload, topic_name))

def subscription_exists(topic_name, subscription_name):
    """
    checks whether the subscription exists against the input topic or not 
    """
    retVal = False
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)
    subscriptions = publisher.list_topic_subscriptions(topic_path)
    
    for subscription_path in subscriptions:
        if subscription_name in subscription_path:
            retVal = True
            break

    return retVal

def create_subscription(topic_name, subscription_name):
    """
    creates a gcp pub/sub subscription for the input topic name if it
    does not exist
    """
    already_exists = subscription_exists(topic_name,
                                         subscription_name)

    if already_exists is False:
        subscriber = pubsub_v1.SubscriberClient()
        sub_path = subscriber.subscription_path(PROJECT_ID, subscription_name)
        topic_path = subscriber.topic_path(PROJECT_ID, topic_name)
        ttl_duration = duration_pb2.Duration(seconds=SUBSCRIPTION_TTL)
        expiration_policy = pubsub_v1.types.ExpirationPolicy(ttl=ttl_duration)
        response = subscriber.create_subscription(name=sub_path,
                                                  topic=topic_path,
                                                  ack_deadline_seconds=ACK_DEADLINE,
                                                  expiration_policy=expiration_policy)

def is_directed_to_me(pubsub_message):
    """
    checks and returns whether or not a GCP pub/sub message is
    directed to this Rasberry Pi4 device
    """
    for_me = False
    attributes = pubsub_message.attributes
    if attributes:
        target_device = attributes['location']
        for_me = target_device == DEVICE_INFO['identifier']

    return for_me

def sync_pull_message(subscription_name):
    """
    synchronously pulls pub/sub messages from the input subscription_name
    until a message targeted for this Raspberry Pi4 device is received
    """
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        PROJECT_ID, subscription_name
    )
 
    msg_data = None
    msg_receipt = False
    #keep trying until the intended message is not received
    while msg_receipt == False:
        log_msg = 'waiting for message from {} subscription...'
        print(log_msg.format(subscription_name))
        response = subscriber.pull(subscription_path,
                                   max_messages=MAX_LIMIT,
                                   timeout=SUB_TIMEOUT)

        ack_ids = []
        for received_message in response.received_messages:
            ack_ids.append(received_message.ack_id)
            msg_receipt = is_directed_to_me(received_message.message)

            if msg_receipt:
                payload = received_message.message.data
                msg_data = json.loads(payload.decode('utf-8'))
                log_msg = 'message received from {} subscription successfully'
                print(log_msg.format(subscription_name))

        if len(ack_ids) > 0: #attempt to ACK iff messages were received
            #acknowledges the received messages so they will not be sent
            #again.
            subscriber.acknowledge(subscription_path, ack_ids)

    return msg_data

def save_review_period():
    """
    saves the review period json to the disk
    """
    with open(REVIEW_PERIOD_JSON, 'w') as json_file:
        json.dump(REVIEW_PERIOD_INFO, json_file)

def capture_snapshot():
    """
    Add annotation
    """
    snapshot = {}
    snapshot['pi4_state'] = PI4_STATE
    snapshot['sys_flags'] = SYSTEM_FLAGS
    snapshot['sheet_id'] = REVIEW_SHEET_ID
    snapshot['last_stored_timestamp'] = LAST_STORED_TIMESTAMP

    with open(SNAPSHOT_FILE, 'w') as json_file:
        json.dump(snapshot, json_file)

def transition_state(new_state):
    """
    updates the PI4_STATE to new_state and captures a snapshot of the globals
    at the time of the state change as well
    """
    global PI4_STATE
    PI4_STATE = new_state
    
    #it is important to ensure that checkpointing takes place at non-waiting
    #states only. given that the asynchronous nature of the message exchange
    #protocol between Rasberry Pi4 and the cloud backend, it is impossible to
    #guarantee that the Pi4 will not get "deadlocked" in the waiting state in
    #case of an attempted recovery from a shutdown.
    if (PI4_STATE != AWAIT_REVIEW_PERIOD and
        PI4_STATE != AWAIT_REVIEW_SHEET and
        PI4_STATE != AWAIT_LAST_STORED_TIMESTAMP):
        capture_snapshot()

def handler_fetch_review_period():
    """
    executes the logic for handling the FETCH_REVIEW_PERIOD state
    """
    #at this point the REVIEW_PERIOD_INFO object contains the details of
    #the attendance review period that just ended i.e. the previous review
    #period
    publish_message_to_topic(PROJECT_ID, 
                             GET_REVIEW_PERIOD_TOPIC,
                             REVIEW_PERIOD_INFO)

    transition_state(AWAIT_REVIEW_PERIOD) 

def handler_await_review_period():
    """
    executes the logic for handling the AWAIT_REVIEW_PERIOD state
    """
    subscription_name = SUBSCRIPTION_NAME.format(NEW_REVIEW_PERIOD_TOPIC,
                                                 DEVICE_INFO['identifier'])
    create_subscription(NEW_REVIEW_PERIOD_TOPIC, subscription_name)
    new_review_period = sync_pull_message(subscription_name)

    next_state = None
    if len(new_review_period) > 0:
        #clear the final alarm flag from sys flags
        SYSTEM_FLAGS &= (~FINAL_ALARM_RAISED)

        #update and save the new review period's info and transition 
        #to the next state
        REVIEW_PERIOD_INFO = new_review_period
        save_review_period()

        next_state = REQUEST_REVIEW_SHEET
    else: #denotes that the data for the next review period isn't available yet
        if SYSTEM_FLAGS & FINAL_ALARM_RAISED != 0:
            next_state = NO_REVIEW_PERIOD 
        else: #denotes that the final alarm hasn't been raised yet
            next_state = FINAL_ALARM_RAISED

    transition_state(next_state)
        
def handler_raise_final_alarm():
    """
    executes the logic for handling the RAISE_FINAL_ALARM state
    """
    #TODO: Fire an email to People Ops to report missing review period and 
    #      inform about entering deep sleep mode

    #set the FINAL_ALARM_RAISED flag on a successful dispatch of the email
    #notification to the People Ops team
    SYSTEM_FLAGS |= FINAL_ALARM_RAISED

    transition_state(NO_REVIEW_PERIOD)

def handler_no_review_period():
    """
    executes the logic for handling the NO_REVIEW_PERIOD state
    """
    #enter a deep sleep, re-check for the next review period on wake up!
    time.sleep(DEEP_SLEEP_DURATION.total_seconds())
    transition_state(FETCH_REVIEW_PERIOD)

def handler_request_review_sheet():
    """
    executes the logic for handling the REQUEST_REVIEW_SHEET state
    """
    #in this state the REVIEW_PERIOD_INFO object contains the information of
    #the new attendance review period that is about to begin
    publish_message_to_topic(PROJECT_ID,
                             CREATE_REVIEW_SHEET_TOPIC,
                             REVIEW_PERIOD_INFO)

    transition_state(AWAIT_REVIEW_SHEET)

def handler_await_review_sheet():
    """
    executes the logic for handling the AWAIT_REVIEW_SHEET state
    """
    subscription_name = SUBSCRIPTION_NAME.format(NEW_REVIEW_SHEET_TOPIC,
                                                 DEVICE_INFO['identifier'])
    create_subscription(NEW_REVIEW_SHEET_TOPIC, subscription_name)
    new_review_sheet = sync_pull_message(subscription_name)

    #store the review sheet id of the Attendance Review sheet for the new
    #review period 
    REVIEW_SHEET_ID = new_review_sheet['id']
    print('saving sheet id of {} sheet'.format(new_review_sheet['name']))

    transition_state(RELAY_ATTENDANCE_RECORDS)

def handler_relay_attendance_records():
    """
    executes the logic for handling the RELAY_ATTENDANCE_RECORDS state
    """
    #wait for wait_duration before attempting to pull the attendance records
    #from the zkteco biometric device
    wait_duration = wait_duration_before_next_pull()
    time.sleep(wait_duration.total_seconds())

    #from_timestamp should be the review_start_date at the start of the review
    #period, and then equal to the last stored timestamp returned by the GCP
    #Pub/Sub endpoint
    from_timestamp = None #where to start filtering attendance data from
    if LAST_STORED_TIMESTAMP is not None:
        from_timestamp = datetime.strptime(LAST_STORED_TIMESTAMP,
                                           '%d-%m-%Y %H:%M:%S')
    else:
        from_timestamp = datetime.strptime(REVIEW_PERIOD_INFO['start_date'],
                                           '%m/%d/%Y')
    new_records = get_attendance_records(from_timestamp)

    if len(new_records) > 0:
        #new (unsaved) attendance records are available on the device since
        #the last publish call to the cloud
        request_params = {}
        request_params['sheet_id'] = REVIEW_SHEET_ID
        request_params['device_id'] = DEVICE_INFO['identifier']
        request_params['start_date'] = REVIEW_PERIOD_INFO['start_date']
        request_params['records'] = new_records
        publish_message_to_topic(PROJECT_ID,
                                 STORE_ATTEND_RECORDS_TOPIC,
                                 request_params)
        transition_state(AWAIT_LAST_STORED_TIMESTAMP)

def handler_await_last_stored_timestamp():
    """
    executes the logic for handling the AWAIT_LAST_STORED_TIMESTAMP state
    """
    subscription_name = SUBSCRIPTION_NAME.format(LAST_STORED_TIMESTAMP_TOPIC,
                                                 DEVICE_INFO['identifier'])
    create_subscription(LAST_STORED_TIMESTAMP_TOPIC, subscription_name)
    last_stored_record = sync_pull_message(subscription_name)

    #update the timestamp of the most recent attendance record saved
    LAST_STORED_TIMESTAMP = last_stored_record['timestamp']

    if review_period_expired():
        #proceed with the clean up and maintenance because the review period
        #has expired 
        next_state = REVIEW_PERIOD_EXPIRED
    else:
        next_state = RELAY_ATTENDANCE_RECORDS

    transition_state(next_state)

def handler_review_period_expired():
    """
    executes the logic for handling the REVIEW_PERIOD_EXPIRED state
    """
    #check for any unsaved attendance records before attempting to delete
    #data from the zkteco biometric device
    new_records = get_attendance_records(from_timestamp)
    if len(new_records) == 0:
        #there are no unsaved attendance records on the biometric device,
        #therefore, it is safe to delete all attendance data from the device
        device_tag = DEVICE_INFO['identifier']
        print('deleting attendance records from device {}'.format(device_tag))
        clear_records_from_device(DEVICE_INFO['ip'], DEVICE_INFO['port'])
        
    global REVIEW_SHEET_ID, LAST_STORED_TIMESTAMP
    REVIEW_SHEET_ID = LAST_STORED_TIMESTAMP = None
    
    transition_state(FETCH_REVIEW_PERIOD)

def setup_device_info():
    """
    loads the configuration details of the attendance devices
    from the configuration file
    """
    global DEVICE_INFO
    with open(BIOMETRIC_DEVICE_CONFIG_FILE) as json_file:
        DEVICE_INFO = json.load(json_file)

    print ('device config: ', DEVICE_INFO)

def setup_review_period_info():
    """
    loads the review period information from the json file
    """
    global REVIEW_PERIOD_INFO
    with open(REVIEW_PERIOD_JSON) as json_file:
        REVIEW_PERIOD_INFO = json.load(json_file)

    print ('review period info: ', REVIEW_PERIOD_INFO)

def setup_handler_table():
    """
    populates the state handler table during bootstrapping sequence
    """
    global HANDLER_TABLE 
    HANDLER_TABLE[FETCH_REVIEW_PERIOD] = handler_fetch_review_period
    HANDLER_TABLE[AWAIT_REVIEW_PERIOD] = handler_await_review_period
    HANDLER_TABLE[RAISE_FINAL_ALARM] = handler_raise_final_alarm
    HANDLER_TABLE[NO_REVIEW_PERIOD] = handler_no_review_period
    HANDLER_TABLE[REQUEST_REVIEW_SHEET] = handler_request_review_sheet
    HANDLER_TABLE[AWAIT_REVIEW_SHEET] = handler_await_review_sheet
    HANDLER_TABLE[RELAY_ATTENDANCE_RECORDS] = handler_relay_attendance_records
    HANDLER_TABLE[AWAIT_LAST_STORED_TIMESTAMP] = handler_await_last_stored_timestamp
    HANDLER_TABLE[REVIEW_PERIOD_EXPIRED] = handler_review_period_expired

def load_snapshot():
    """
    loads the snapshot captured during checkpointing to resume operation
    without losing state information in case of a (device) shutdown
    """
    global PI4_STATE, REVIEW_SHEET_ID, LAST_STORED_TIMESTAMP
    with open(SNAPSHOT_FILE) as json_file:
        snapshot = json.load(json_file)
        PI4_STATE = snapshot['pi4_state']
        SYSTEM_FLAGS = snapshot['sys_flags']
        REVIEW_SHEET_ID = snapshot['sheet_id']
        LAST_STORED_TIMESTAMP = snapshot['last_stored_timestamp']

    msg = 'pi4 state = {}, review sheet id = {}, last stored timestamp = {}'
    print ('loaded snapshot: \n', msg.format(PI4_STATE,
                                             REVIEW_SHEET_ID,
                                             LAST_STORED_TIMESTAMP))

def bootstrap_pi4():
    """
    Add annotation
    """
    setup_device_info()
    setup_handler_table()
    setup_review_period_info()
    load_snapshot()
    
def run():
    """
    the main loop that continues to run endlessly on the Raspberry Pi 4 
    device(s) and is responsible for invoking the appropriate handler for
    the current state of the software
    """
    while True:
        HANDLER_TABLE[PI4_STATE]()

def main():
    """
    main method - marks the start of execution
    """
    bootstrap_pi4()
    run()
    
main()
