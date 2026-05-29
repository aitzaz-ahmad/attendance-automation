from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

# constants defined for the states in the FSM
FETCH_REVIEW_PERIOD = 1
AWAIT_REVIEW_PERIOD = 2
RAISE_FINAL_ALARM = 3
NO_REVIEW_PERIOD = 4
REQUEST_REVIEW_SHEET = 5
AWAIT_REVIEW_SHEET = 6
RELAY_ATTENDANCE_RECORDS = 7
AWAIT_LAST_STORED_TIMESTAMP = 8
REVIEW_PERIOD_EXPIRED = 9

# constants defined for various system wide flags
FINAL_ALARM_RAISED = 1  # 0x01
SEND_DAILY_WARNING = 1 << 1  # 0x02

WAITING_STATES: Tuple[int, int, int] = (
    AWAIT_REVIEW_PERIOD,
    AWAIT_REVIEW_SHEET,
    AWAIT_LAST_STORED_TIMESTAMP,
)


@dataclass
class Pi4RuntimeState:
    pi4_state: int = 0
    system_flags: int = 0
    device_info: Optional[Dict[str, Any]] = None
    review_period_info: Optional[Dict[str, Any]] = None
    review_sheet_id: Optional[str] = None
    last_stored_timestamp: Optional[str] = None
