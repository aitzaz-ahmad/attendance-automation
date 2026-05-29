import json

# constants defined for GCP pub/sub message exchange
PROJECT_ID = "attend1"
GET_REVIEW_PERIOD_TOPIC = "get_review_period"
NEW_REVIEW_PERIOD_TOPIC = "new_review_period"
CREATE_REVIEW_SHEET_TOPIC = "create_review_sheet"
NEW_REVIEW_SHEET_TOPIC = "new_review_sheet"
STORE_ATTEND_RECORDS_TOPIC = "store_attend_records"
LAST_STORED_TIMESTAMP_TOPIC = "last_stored_timestamp"

SUBSCRIPTION_NAME = "sub_{}_{}"
ACK_DEADLINE = 10  # lease time (in seconds) to acknowledge the receipt of a message
SUBSCRIPTION_TTL = 7776000  # 90 days (in seconds)
MAX_LIMIT = 1
PULL_MSG_TIMEOUT = 30.0  # How long the subscriber should listen for messages in seconds


class PubSubMessenger:
    def __init__(self, device_identifier, project_id=PROJECT_ID):
        self.device_identifier = device_identifier
        self.project_id = project_id

    def subscription_name(self, topic_name):
        return SUBSCRIPTION_NAME.format(topic_name, self.device_identifier)

    def publish_message_to_topic(self, topic_name, data):
        publish_message_to_topic(self.project_id, topic_name, data)

    def create_subscription(self, topic_name, subscription_name):
        create_subscription(topic_name, subscription_name, self.project_id)

    def sync_pull_message(self, subscription_name):
        return sync_pull_message(subscription_name, self.device_identifier, self.project_id)


def publish_message_to_topic(project_id, topic_name, data):
    """
    converts the input argument 'data' (a dictionary) to a byte array
    and publishes it as a message to the GCP pub/sub topic 'topic_name'
    created under the GCP project identified by 'project_id'
    """
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    # convert the data object to a utf-8 encoded json string
    payload = json.dumps(data)
    future_response = publisher.publish(topic_path, payload.encode("utf-8"))

    # the script execution will get blocked on the following call until a
    # message has been successfully published
    message_id = future_response.result()
    log_msg = "message id: {}, payload: {}, published to {} topic."
    print(log_msg.format(message_id, payload, topic_name))


def subscription_exists(topic_name, subscription_name, project_id=PROJECT_ID):
    """
    checks whether the subscription exists against the input topic or not
    """
    from google.cloud import pubsub_v1

    retVal = False
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    subscriptions = publisher.list_topic_subscriptions(topic_path)

    for subscription_path in subscriptions:
        if subscription_name in subscription_path:
            retVal = True
            break

    return retVal


def create_subscription(topic_name, subscription_name, project_id=PROJECT_ID):
    """
    creates a gcp pub/sub subscription for the input topic name if it
    does not exist
    """
    from google.cloud import pubsub_v1
    from google.protobuf import duration_pb2

    already_exists = subscription_exists(topic_name, subscription_name, project_id)

    if already_exists is False:
        subscriber = pubsub_v1.SubscriberClient()
        sub_path = subscriber.subscription_path(project_id, subscription_name)
        topic_path = subscriber.topic_path(project_id, topic_name)
        ttl_duration = duration_pb2.Duration(seconds=SUBSCRIPTION_TTL)
        expiration_policy = pubsub_v1.types.ExpirationPolicy(ttl=ttl_duration)
        subscriber.create_subscription(
            name=sub_path,
            topic=topic_path,
            ack_deadline_seconds=ACK_DEADLINE,
            expiration_policy=expiration_policy,
        )


def is_directed_to_device(pubsub_message, device_identifier):
    """
    checks and returns whether or not a GCP pub/sub message is
    directed to this Rasberry Pi4 device
    """
    for_me = False
    attributes = pubsub_message.attributes
    if attributes:
        target_device = attributes["location"]
        for_me = target_device == device_identifier

    return for_me


def sync_pull_message(subscription_name, device_identifier, project_id=PROJECT_ID):
    """
    synchronously pulls pub/sub messages from the input subscription_name
    until a message targeted for this Raspberry Pi4 device is received
    """
    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    msg_data = None
    msg_receipt = False
    # keep trying until the intended message is not received
    while not msg_receipt:
        log_msg = "waiting for message from {} subscription..."
        print(log_msg.format(subscription_name))
        response = subscriber.pull(subscription_path, max_messages=MAX_LIMIT, timeout=PULL_MSG_TIMEOUT)

        ack_ids = []
        for received_message in response.received_messages:
            ack_ids.append(received_message.ack_id)
            msg_receipt = is_directed_to_device(received_message.message, device_identifier)

            if msg_receipt:
                payload = received_message.message.data
                msg_data = json.loads(payload.decode("utf-8"))
                log_msg = "message received from {} subscription successfully"
                print(log_msg.format(subscription_name))

        if len(ack_ids) > 0:  # attempt to ACK iff messages were received
            # acknowledges the received messages so they will not be sent
            # again.
            subscriber.acknowledge(subscription_path, ack_ids)

    return msg_data
