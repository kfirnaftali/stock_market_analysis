import logging

from google.cloud import pubsub_v1
import apache_beam as beam


class publish_message_to_pub_sub(beam.DoFn):

    def __init__(self, project_id, topic_id):
        self.project = project_id
        self.topic_id = topic_id

    def process(self, message):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(self.project, self.topic_id)
        try:
            future = publisher.publish(topic_path, message, origin="stock_market_recommendation", username="gcp")
            res = future.result()
            logging.info(f'the LLM results published to pubsub:  {res}')
        except Exception as e:
            logging.info("An error occurred while trying to poublish resulst to pub/sup topic:", e)



