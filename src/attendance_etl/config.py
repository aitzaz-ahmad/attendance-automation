"""Central configuration constants for the attendance ETL runtime."""

from datetime import timedelta

# Google Cloud project that owns the current Pub/Sub topics.
PROJECT_ID = "attend1"

# Pub/Sub topics used by the ingestion workflow and Cloud Function responses.
GET_REVIEW_PERIOD_TOPIC = "get_review_period"
NEW_REVIEW_PERIOD_TOPIC = "new_review_period"
CREATE_REVIEW_SHEET_TOPIC = "create_review_sheet"
NEW_REVIEW_SHEET_TOPIC = "new_review_sheet"
STORE_ATTEND_RECORDS_TOPIC = "store_attend_records"
LAST_STORED_TIMESTAMP_TOPIC = "last_stored_timestamp"

# Pub/Sub subscription naming and pull behavior.
SUBSCRIPTION_NAME = "sub_{}_{}"
ACK_DEADLINE = 10
SUBSCRIPTION_TTL = 7776000
MAX_LIMIT = 1
PULL_MSG_TIMEOUT = 30.0

# Local files used by the Raspberry Pi runtime.
SNAPSHOT_FILE = "snapshot.json"
REVIEW_PERIOD_JSON = "review_period.json"
BIOMETRIC_DEVICE_CONFIG_FILE = "biometric_device_config.json"

# Runtime sleep intervals for normal polling and missing-review-period recovery.
POLLING_DELAY = timedelta(minutes=15)
DEEP_SLEEP_DURATION = timedelta(hours=1)
