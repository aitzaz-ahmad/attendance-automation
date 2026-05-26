"""Google Cloud Function entry point wrapper."""

from attendance_etl.functions.create_review_sheet import entry_point as _entry_point


def entry_point(event, context):
    return _entry_point(event, context)
