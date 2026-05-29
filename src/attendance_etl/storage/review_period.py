import json

# constants defined for on-disk files
REVIEW_PERIOD_JSON = "review_period.json"


class ReviewPeriodStore:
    def __init__(self, path=REVIEW_PERIOD_JSON):
        self.path = path

    def load(self):
        return load_review_period(self.path)

    def save(self, review_period_info):
        save_review_period(review_period_info, self.path)


def load_review_period(path=REVIEW_PERIOD_JSON):
    """
    loads the review period information from the json file
    """
    with open(path) as json_file:
        return json.load(json_file)


def save_review_period(review_period_info, path=REVIEW_PERIOD_JSON):
    """
    saves the review period json to the disk
    """
    with open(path, "w") as json_file:
        json.dump(review_period_info, json_file)
