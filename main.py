import json
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from typing import Any
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON
from apache_beam.ml.inference.base import RunInference
from utilities import vertex_ai_search_and_conversation as vtx_srch_and_recmnd
from apache_beam.utils.timestamp import Duration
from utilities import write_to_pub_sub



def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", help="DirectRunner or DataflowRunner", type=str)
    parser.add_argument("--project", help="project", type=str)
    parser.add_argument("--region", help="the retion to run", type=str)
    parser.add_argument("--temp_location", help="gs location", type=str)
    parser.add_argument("--staging_location", help="gs location", type=str)
    parser.add_argument("--service_account", help="gs location", type=str)

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args,
                                       runner=known_args.runner,
                                       project=known_args.project,
                                       job_name='stock-market-analysis',
                                       temp_location=known_args.temp_location,
                                       region=known_args.region,
                                       service_account=known_args.service_account,
                                       allow_unsafe_triggers=True,
                                       enable_data_sampling=True
                                       )
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True  # Retain pipeline state

    # Define Pub/Sub subscription and BigQuery details
    subscription = "projects/demos-337108/subscriptions/expert_hup_stock_tickers-sub"
    results_pub_sub_topic = 'expert_hub_results'

    class LogWindowData(beam.DoFn):
        def process(self, element, window=beam.DoFn.WindowParam):
            from datetime import datetime
            window_start = window.start
            window_end = window.end
            logging.info(f' window start time:{datetime.fromtimestamp(window_start)}, window end time '
                         f'{datetime.fromtimestamp(window_end)}')
            logging.info(f'The window tiem and the element value  is : \n {element}')
            yield element

    class FormatAsPubsubMessage(beam.DoFn):

        """Validates if the element can be converted to JSON and performs the conversion."""

        def process(self, element):
            _, prediction_result = element  # Assuming your key '1' is not important
            # data = json.dumps(prediction_result.__dict__).encode('utf-8')  # Serialize result
            prompt = prediction_result.example.get('prompt')
            content = prediction_result.inference.get('content')
            string_the_results = f'Prompt: \n {prompt} \n Content: \n {content}'
            # json_string = '{"prompt": "%s", "content": "%s"}' % (prompt, content)
            bytestring = string_the_results.encode('utf-8')
            #
            #

            # # Optionally include attributes, e.g., for message filtering or routing
            # attributes = {
            #     'content_type': 'application/json',
            #     'source': ' stock-market-analysis'
            # }
            yield bytestring  # beam.io.WriteStringsToPubSub(bytestring)

    class GetTimestamp(beam.DoFn):
        def process(self, element):
            from datetime import datetime, timezone
            date_format = '%Y-%m-%dT%H:%M:%S.%f%z'
            the_time = str(element[1].get('timestamp'))
            # Parse the datetime string and assign timezone information directly
            time_obj = datetime.strptime(the_time, date_format).replace(tzinfo=timezone.utc)
            # Format the datetime object as needed (optional)
            dt_formatted = time_obj.strftime('%Y-%m-%dT%H:%M:%S')  # If you need the formatted string
            # Get the UTC epoch timestamp
            # epoch_timestamp = time_obj.timestamp()
            yield dt_formatted, element

    class GetKeyValuePair(beam.DoFn):
        def process(self, element, window=beam.DoFn.WindowParam):
            # window_start = window.start
            # window_end = window.end
            # print(f' window start time:{datetime.fromtimestamp(window_start)}, window end time \ {
            # datetime.fromtimestamp(window_end)}, element timestamp is {element.get("timestamp")}, passenger_count
            # for the ride is: {element.get("passenger_count")}')
            yield element.get('ticker'), element

    def parse_json_message(message: bytes) -> dict[str, Any]:
        """Parse the input json message """
        row = json.loads(message)
        return {
            "ticker": row["ticker_name"],
            "close": row["close"],
            "current_transaction_rate": row["current_transaction_rate"],
            "volume": row["volume"],
        }

    class CalcKPIsPerTicker(beam.DoFn):

        def process(self, data):
            if bool(data):

                ticker, transactions = data
                total_volume = sum(t['volume'] for t in transactions)
                current_rates = [t['current_transaction_rate'] for t in transactions]
                try:
                    min_rate = min([rate for rate in current_rates if rate is not None])
                    max_rate = max([rate for rate in current_rates if rate is not None])
                except:
                    min_rate = 155
                    max_rate = 155
                logging.info(
                    f'ticker: {ticker}, total_volume: {total_volume}, min_rate:{min_rate}, max_rate: {max_rate}')
                yield ticker, total_volume, min_rate, max_rate
            else:
                yield 'GOOG', 1, 1.1, 1.1

    class treshold_rate_test(beam.DoFn):
        def __init__(self):
            self.volume_threshold = 7000000  # 7000000

        def process(self, data):
            self.ticker = data[0]  # ticker
            self.total_volume = data[1]  # total_volume
            self.min_rate = data[2]  # min_rate
            self.max_rate = data[3]  # max_rate
            the_rate = ((self.max_rate - self.min_rate) / self.min_rate) * 100
            if (self.total_volume > self.volume_threshold) and ((the_rate < -20) or (the_rate > 5)):
                rate_trend = f'{the_rate}% decrease' if the_rate <= -5 else f'{the_rate}% increase'
                yield self.ticker, self.total_volume, self.min_rate, self.max_rate, rate_trend
            else:
                yield self.ticker, 0, 0.0, 0.0, ''

    parameters = {
        "temperature": 0.2, "maxOutputTokens": 1024, "topK": 40, "topP": 0.95
    }
    model_handler = VertexAIModelHandlerJSON(
        endpoint_id="8259920574969544704",
        project='demos-337108',
        location='us-central1',
    )

    class CostumedKeyedModelHandler(KeyedModelHandler):
        def batch_elements_kwargs(self):
            return {"max_batch_size": 1}

    class VertexAiSearchCall(beam.DoFn):

        def __init__(self):
            self.project_id = "demos-337108"
            self.location = "global"
            self.data_store_id = "expert-hub-stock-market_1706521538254"
            self.query = "google recommender buy or sell stock"
            self.rate_trend = '5% increase'

        def process(self, element):
            response = vtx_srch_and_recmnd.search_sample(self.project_id, self.location, self.data_store_id, self.query)
            """Extracts a list of snippets from the given response."""
            snippets = []
            snippet = ""
            results = ""

            for result in response.results:
                try:
                    snippet = result.document.derived_struct_data.get('snippets')[0].get('snippet')
                    logging.info(f'this is the snippet extracted from vertex ai search and conv: {snippet}')
                except Exception as e:
                    logging.info("An error occurred while trying get snippet:", e)
                    # return " "

                if snippet:  # Optional: Check if snippet exists before adding
                    results += snippet + ' '
            rate_trend = element[4]
            self.rate_trend = rate_trend if rate_trend is not None else self.rate_trend
            prompt = f'accoding to the following data: \n {results} \n and consider  {self.rate_trend} in GOOG stock, what do you' \
                     f' recommend to do with the stock: buy sell or hold? explain the reason for your recommendation'
            logging.info(prompt)
            yield prompt

    def filter_zero_values(element):
        ticker, total_volume, min_rate, max_rate = element
        if min_rate != 0.0:
            yield element

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read messages from Pub/Sub
        messages = (pipeline | "ReadFromPubSub" >> ReadFromPubSub(subscription=subscription)
                    # | 'json the data' >> beam.Map(json.loads)
                    | "Parse JSON messages" >> beam.Map(parse_json_message)
                    | beam.WindowInto(beam.window.FixedWindows(60),
                                      allowed_lateness=Duration(seconds=5 * 24 * 60 * 60),  # 5 days in seconds
                                      trigger=beam.trigger.Repeatedly(beam.trigger.AfterProcessingTime(60)),
                                      # trigger=beam.trigger.Repeatedly(
                                      #     beam.trigger.AfterWatermark(early=beam.trigger.AfterCount(1))),
                                      accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)

                    | 'Key Value Pair' >> beam.ParDo(GetKeyValuePair())
                    | 'group all the tickers by their symbols' >> beam.GroupByKey()
                    | "Calculate KPIs-Volume,Min-Max Rate" >> beam.ParDo(CalcKPIsPerTicker())
                    )
        prnt_stg = (messages | 'print to log' >> beam.ParDo(LogWindowData()))

        results = (messages | "threshold test" >> beam.ParDo(treshold_rate_test())
                   # x[2] = min_rate - filtering only those who pass the
                   | 'filtering irrelevant' >> beam.Filter(lambda x: x[2] != 0.0)
                   # threshold test
                   | 'call vertex ai search and conversation' >> beam.ParDo(VertexAiSearchCall())
                   | "Format prompt" >> beam.Map(lambda data: ("1", {"prompt": data}))
                   | "run inference with vertex " >> RunInference(CostumedKeyedModelHandler(model_handler))
                   | 'Format to PubSub message' >> beam.ParDo(FormatAsPubsubMessage())
                   | 'write to pub/sub' >> beam.ParDo(
                    write_to_pub_sub.publish_message_to_pub_sub(project_id=known_args.project,
                                                                topic_id=results_pub_sub_topic))
                   )
        # _ = results | 'print to log' >> beam.ParDo(LogWindowData())


if __name__ == "__main__":
    import logging

    logging.getLogger().setLevel(logging.INFO)
    main()
