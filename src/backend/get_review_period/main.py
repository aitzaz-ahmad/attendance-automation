"""Google Cloud Function entry point wrapper."""

from attendance_etl.functions.get_review_period import entry_point as _entry_point


def entry_point(event, context):
    return _entry_point(event, context)
