from attendance_etl.logging_utils import get_logger

logger = get_logger("RecordTransformer")


def convert_to_map(zk_users):
    """
    converts the list of users fetched from the biometric device
    to a user id vs name map to allow a O(1) lookup for generating
    a human readable equivalent of the attendance entries.
    """
    logger.debug("Generating user id to name mapping for %s users...", len(zk_users))
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
    entry_type = "Check In" if attendance_record.punch == 0 else "Check Out"

    record = {}
    record["username"] = name
    record["timestamp"] = timestamp.strftime("%d-%m-%Y %H:%M:%S")
    record["entry"] = entry_type

    return record


def decode_zk_format(attendance_records, user_mapping, device_tag):
    """
    decodes the attendance records from the biometric device to
    a python friendly list which can be dumped to the database.
    """
    decoded_records = []
    for entry in attendance_records:
        if entry.user_id in user_mapping:
            # decode attendance records only for the users that
            # have not been deleted from the biometric device
            record = convert_to_dict(entry, user_mapping)
            record["device"] = device_tag
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
        logger.warning("Error: Invalid input arguments - to_timstamp must be greater than from_timestamp")
    else:
        filtered_records = [
            record for record in records if (record.timestamp > from_timestamp and record.timestamp <= to_timestamp)
        ]

    return filtered_records
