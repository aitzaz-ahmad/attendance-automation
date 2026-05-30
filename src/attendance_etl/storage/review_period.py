import json

from attendance_etl import config
from attendance_etl.logging_utils import get_logger
from attendance_etl.models import ReviewPeriod

logger = get_logger("ReviewPeriod")


def load_review_period(path=config.REVIEW_PERIOD_JSON) -> ReviewPeriod:
    """
    loads the review period information as the shared domain model.
    """
    logger.debug("loading review period from %s", path)
    with open(path) as json_file:
        return ReviewPeriod.from_dict(json.load(json_file))


def save_review_period(review_period: ReviewPeriod, path=config.REVIEW_PERIOD_JSON) -> None:
    """
    saves the review period json to the disk
    """
    if not isinstance(review_period, ReviewPeriod):
        raise TypeError("save_review_period expects a ReviewPeriod instance")

    logger.debug("saving review period to %s", path)
    with open(path, "w") as json_file:
        json.dump(review_period.to_dict(), json_file)
