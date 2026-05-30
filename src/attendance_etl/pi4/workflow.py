import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, cast

from attendance_etl import config
from attendance_etl.logging_utils import get_logger
from attendance_etl.models import ReviewPeriod, RuntimeState
from attendance_etl.pi4.state import (
    AWAIT_LAST_STORED_TIMESTAMP,
    AWAIT_REVIEW_PERIOD,
    AWAIT_REVIEW_SHEET,
    FETCH_REVIEW_PERIOD,
    FINAL_ALARM_RAISED,
    NO_REVIEW_PERIOD,
    RAISE_FINAL_ALARM,
    RELAY_ATTENDANCE_RECORDS,
    REQUEST_REVIEW_SHEET,
    REVIEW_PERIOD_EXPIRED,
    WAITING_STATES,
)
from attendance_etl.storage.review_period import save_review_period
from attendance_etl.storage.snapshot import save_snapshot
from attendance_etl.transform.zkteco_records import convert_to_map, decode_zk_format, filter_records

logger = get_logger("Pi4Workflow")


class Pi4Workflow:
    def __init__(self, state, device, messenger):
        self.state = state
        self.device = device
        self.messenger = messenger
        self.handler_table = self.setup_handler_table()

    def setup_handler_table(self):
        """
        populates the state handler table during bootstrapping sequence
        """
        handler_table = {}
        handler_table[FETCH_REVIEW_PERIOD] = self.handler_fetch_review_period
        handler_table[AWAIT_REVIEW_PERIOD] = self.handler_await_review_period
        handler_table[RAISE_FINAL_ALARM] = self.handler_raise_final_alarm
        handler_table[NO_REVIEW_PERIOD] = self.handler_no_review_period
        handler_table[REQUEST_REVIEW_SHEET] = self.handler_request_review_sheet
        handler_table[AWAIT_REVIEW_SHEET] = self.handler_await_review_sheet
        handler_table[RELAY_ATTENDANCE_RECORDS] = self.handler_relay_attendance_records
        handler_table[AWAIT_LAST_STORED_TIMESTAMP] = self.handler_await_last_stored_timestamp
        handler_table[REVIEW_PERIOD_EXPIRED] = self.handler_review_period_expired
        return handler_table

    def device_info(self):
        return cast(Dict[str, Any], self.state.device_info)

    def review_period_info(self):
        return cast(Dict[str, Any], self.state.review_period_info)

    def get_attendance_records(self, from_date, to_date=None):
        """
        pulls all attendance records from the biometric device, filters out the
        records based on the from_date and to_date time frame, and decodes the
        filtered records from the zkteco format before returning them
        """
        logger.debug("get_attendance_records invoked")

        device_info = self.device_info()
        device_tag = device_info["identifier"]
        logger.info("fetching data from device %s", device_tag)
        users, records = self.device.pull_records()
        logger.info("filtering attendance records since %s", from_date.strftime("%d-%m-%Y %H:%M:%S"))
        unsaved_records = filter_records(records, from_date, to_date)
        logger.info("%s unsaved attendance records found", len(unsaved_records))
        user_mapping = convert_to_map(users)
        logger.debug("decoding unsaved records from zkteco format...")
        decoded_records = decode_zk_format(unsaved_records, user_mapping, device_tag)

        logger.info("%s attendance records decoded", len(decoded_records))

        return decoded_records

    def review_period_expired(self):
        """
        returns True if the date of the system clock is greater than
        the date of the last day of the review period, False otherwise
        """
        review_period_info = self.review_period_info()
        last_day = datetime.strptime(review_period_info["end_date"], "%m/%d/%Y")
        if last_day.isoweekday() == 5:  # denotes Friday
            # consider Sunday as the last day of this review period
            last_day += timedelta(days=2)

        return datetime.today().date() > last_day.date()

    def wait_duration_before_next_pull(self):
        """
        returns the time duration to wait for before attempting to pull
        the attendance records (from the zkteco device) the next time
        """
        review_period_info = self.review_period_info()
        now = datetime.today()
        last_day = datetime.strptime(review_period_info["end_date"], "%m/%d/%Y")
        if last_day.isoweekday() == 5:  # denotes Friday
            # consider Sunday as the last day of this review period
            last_day += timedelta(days=2)

        wait_duration = config.POLLING_DELAY
        if now.date() == last_day.date():
            # today's the last day of this review period
            delta_midnight = last_day + timedelta(days=1) - now
            wait_duration = min(config.POLLING_DELAY, delta_midnight)

        return wait_duration

    def capture_snapshot(self):
        """
        Add annotation
        """
        last_stored_timestamp = self.state.last_stored_timestamp
        if isinstance(last_stored_timestamp, str):
            last_stored_timestamp = datetime.strptime(last_stored_timestamp, "%d-%m-%Y %H:%M:%S")

        save_snapshot(
            RuntimeState(
                pi4_state=self.state.pi4_state,
                sys_flags=self.state.system_flags,
                sheet_id=self.state.review_sheet_id,
                last_stored_timestamp=last_stored_timestamp,
            )
        )

    def transition_state(self, new_state):
        """
        updates the PI4_STATE to new_state and captures a snapshot of the globals
        at the time of the state change as well
        """
        self.state.pi4_state = new_state

        # it is important to ensure that checkpointing takes place at non-waiting
        # states only. given that the asynchronous nature of the message exchange
        # protocol between Rasberry Pi4 and the cloud backend, it is impossible to
        # guarantee that the Pi4 will not get "deadlocked" in the waiting state in
        # case of an attempted recovery from a shutdown.
        if self.state.pi4_state not in WAITING_STATES:
            self.capture_snapshot()

    def handler_fetch_review_period(self):
        """
        executes the logic for handling the FETCH_REVIEW_PERIOD state
        """
        # at this point the REVIEW_PERIOD_INFO object contains the details of
        # the attendance review period that just ended i.e. the previous review
        # period
        self.messenger.publish_message_to_topic(config.GET_REVIEW_PERIOD_TOPIC, self.state.review_period_info)

        self.transition_state(AWAIT_REVIEW_PERIOD)

    def handler_await_review_period(self):
        """
        executes the logic for handling the AWAIT_REVIEW_PERIOD state
        """
        subscription_name = self.messenger.subscription_name(config.NEW_REVIEW_PERIOD_TOPIC)
        self.messenger.create_subscription(config.NEW_REVIEW_PERIOD_TOPIC, subscription_name)
        new_review_period = self.messenger.sync_pull_message(subscription_name)

        next_state = None
        if len(new_review_period) > 0:
            # clear the final alarm flag from sys flags
            self.state.system_flags &= ~FINAL_ALARM_RAISED

            # update and save the new review period's info and transition
            # to the next state
            self.state.review_period_info = new_review_period
            save_review_period(ReviewPeriod.from_dict(new_review_period))

            next_state = REQUEST_REVIEW_SHEET
        else:  # denotes that the data for the next review period isn't available yet
            if self.state.system_flags & FINAL_ALARM_RAISED != 0:
                next_state = NO_REVIEW_PERIOD
            else:  # denotes that the final alarm hasn't been raised yet
                # Preserve the legacy quirk: this assigns the flag value as a
                # state id, which equals FETCH_REVIEW_PERIOD.
                next_state = FINAL_ALARM_RAISED

        self.transition_state(next_state)

    def handler_raise_final_alarm(self):
        """
        executes the logic for handling the RAISE_FINAL_ALARM state
        """
        # TODO: Fire an email to People Ops to report missing review period and
        #      inform about entering deep sleep mode

        # set the FINAL_ALARM_RAISED flag on a successful dispatch of the email
        # notification to the People Ops team
        self.state.system_flags |= FINAL_ALARM_RAISED

        self.transition_state(NO_REVIEW_PERIOD)

    def handler_no_review_period(self):
        """
        executes the logic for handling the NO_REVIEW_PERIOD state
        """
        # enter a deep sleep, re-check for the next review period on wake up!
        time.sleep(config.DEEP_SLEEP_DURATION.total_seconds())
        self.transition_state(FETCH_REVIEW_PERIOD)

    def handler_request_review_sheet(self):
        """
        executes the logic for handling the REQUEST_REVIEW_SHEET state
        """
        # in this state the REVIEW_PERIOD_INFO object contains the information of
        # the new attendance review period that is about to begin
        self.messenger.publish_message_to_topic(config.CREATE_REVIEW_SHEET_TOPIC, self.state.review_period_info)

        self.transition_state(AWAIT_REVIEW_SHEET)

    def handler_await_review_sheet(self):
        """
        executes the logic for handling the AWAIT_REVIEW_SHEET state
        """
        subscription_name = self.messenger.subscription_name(config.NEW_REVIEW_SHEET_TOPIC)
        self.messenger.create_subscription(config.NEW_REVIEW_SHEET_TOPIC, subscription_name)
        new_review_sheet = self.messenger.sync_pull_message(subscription_name)

        # store the review sheet id of the Attendance Review sheet for the new
        # review period
        self.state.review_sheet_id = new_review_sheet["id"]
        logger.info("saving sheet id of %s sheet", new_review_sheet["name"])

        self.transition_state(RELAY_ATTENDANCE_RECORDS)

    def handler_relay_attendance_records(self):
        """
        executes the logic for handling the RELAY_ATTENDANCE_RECORDS state
        """
        # wait for wait_duration before attempting to pull the attendance records
        # from the zkteco biometric device
        wait_duration = self.wait_duration_before_next_pull()
        time.sleep(wait_duration.total_seconds())

        review_period_info = self.review_period_info()

        # from_timestamp should be the review_start_date at the start of the review
        # period, and then equal to the last stored timestamp returned by the GCP
        # Pub/Sub endpoint
        from_timestamp = None  # where to start filtering attendance data from
        if self.state.last_stored_timestamp is not None:
            from_timestamp = datetime.strptime(self.state.last_stored_timestamp, "%d-%m-%Y %H:%M:%S")
        else:
            from_timestamp = datetime.strptime(review_period_info["start_date"], "%m/%d/%Y")
        new_records = self.get_attendance_records(from_timestamp)

        if len(new_records) > 0:
            # new (unsaved) attendance records are available on the device since
            # the last publish call to the cloud
            request_params = {}
            request_params["sheet_id"] = self.state.review_sheet_id
            request_params["device_id"] = self.device_info()["identifier"]
            request_params["start_date"] = review_period_info["start_date"]
            request_params["records"] = new_records
            self.messenger.publish_message_to_topic(config.STORE_ATTEND_RECORDS_TOPIC, request_params)
            self.transition_state(AWAIT_LAST_STORED_TIMESTAMP)

    def handler_await_last_stored_timestamp(self):
        """
        executes the logic for handling the AWAIT_LAST_STORED_TIMESTAMP state
        """
        subscription_name = self.messenger.subscription_name(config.LAST_STORED_TIMESTAMP_TOPIC)
        self.messenger.create_subscription(config.LAST_STORED_TIMESTAMP_TOPIC, subscription_name)
        last_stored_record = self.messenger.sync_pull_message(subscription_name)

        # update the timestamp of the most recent attendance record saved
        self.state.last_stored_timestamp = last_stored_record["timestamp"]

        if self.review_period_expired():
            # proceed with the clean up and maintenance because the review period
            # has expired
            next_state = REVIEW_PERIOD_EXPIRED
        else:
            next_state = RELAY_ATTENDANCE_RECORDS

        self.transition_state(next_state)

    def handler_review_period_expired(self):
        """
        executes the logic for handling the REVIEW_PERIOD_EXPIRED state
        """
        review_period_info = self.review_period_info()

        # check for any unsaved attendance records before attempting to delete
        # data from the zkteco biometric device
        if self.state.last_stored_timestamp is not None:
            from_timestamp = datetime.strptime(self.state.last_stored_timestamp, "%d-%m-%Y %H:%M:%S")
        else:
            from_timestamp = datetime.strptime(review_period_info["start_date"], "%m/%d/%Y")

        new_records = self.get_attendance_records(from_timestamp)
        if len(new_records) == 0:
            # there are no unsaved attendance records on the biometric device,
            # therefore, it is safe to delete all attendance data from the device
            device_tag = self.device_info()["identifier"]
            logger.info("deleting attendance records from device %s", device_tag)
            self.device.clear_records()

        self.state.review_sheet_id = self.state.last_stored_timestamp = None

        self.transition_state(FETCH_REVIEW_PERIOD)

    def run_once(self):
        self.handler_table[self.state.pi4_state]()

    def run_forever(self):
        """
        the main loop that continues to run endlessly on the Raspberry Pi 4
        device(s) and is responsible for invoking the appropriate handler for
        the current state of the software
        """
        while True:
            self.run_once()


HandlerTable = Dict[int, Callable[[], None]]
