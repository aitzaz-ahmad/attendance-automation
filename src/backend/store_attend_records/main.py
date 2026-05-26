"""Google Cloud Function entry point wrapper."""

from attendance_etl.functions.store_attend_records import entry_point as _entry_point


def entry_point(event, context):
    return _entry_point(event, context)
