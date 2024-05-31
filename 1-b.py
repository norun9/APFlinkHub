from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.datastream.functions import AggregateFunction
from pyflink.common.time import Time
from typing import Optional, Dict, Union


class MyAggregateFunction(AggregateFunction):

    def create_accumulator(self):
        return {
            'count': 0,
            'sum': 0.0,
            'min': float('inf'),
            'max': float('-inf'),
            'topic': None
        }

    def add(self, value: Dict[str, Union[str, float]], accumulator: Dict[str, Union[int, float, str]]):
        v = float(value['value'])
        accumulator['count'] += 1
        accumulator['sum'] += v
        accumulator['min'] = min(accumulator['min'], v)
        accumulator['max'] = max(accumulator['max'], v)
        if accumulator['topic'] is None:
            accumulator['topic'] = value['topic']
        return accumulator

    def get_result(self, accumulator: Dict[str, Union[int, float, str]]) -> Dict[str, Union[str, float]]:
        if accumulator['count'] == 0:
            return {
                'avg': 0,
                'min': float('inf'),
                'max': float('-inf'),
                'count': 0,
                'topic': accumulator['topic']
            }
        else:
            result = {
                'avg': round(accumulator['sum'] / accumulator['count'], 2),
                'min': round(accumulator['min'], 2),
                'max': round(accumulator['max'], 2),
                'count': accumulator['count'],
                'topic': accumulator['topic']
            }
            return result

    def merge(self, acc_a: Dict[str, Union[int, float, str]], acc_b: Dict[str, Union[int, float, str]]) -> Dict[str, Union[int, float, str]]:
        acc_a['count'] += acc_b['count']
        acc_a['sum'] += acc_b['sum']
        acc_a['min'] = min(acc_a['min'], acc_b['min'])
        acc_a['max'] = max(acc_a['max'], acc_b['max'])
        acc_a['topic'] = acc_b['topic'] if 'topic' not in acc_a else acc_a['topic']
        return acc_a


env = StreamExecutionEnvironment.get_execution_environment()


def create_kafka_producer(topic):
    return FlinkKafkaProducer(
        topic=topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': '150.65.230.59:9092'}
    )


consumer_topic = 'i483-allsensors'

kafka_consumer = FlinkKafkaConsumer(
    topics=consumer_topic,
    deserialization_schema=SimpleStringSchema(),
    properties={
        'bootstrap.servers': '150.65.230.59:9092',
        'group.id': f'flink-consumer-{consumer_topic}'
    }
)

kafka_consumer.set_start_from_latest()

data_stream = env.add_source(kafka_consumer)


def parse_message(message) -> Optional[Dict[str, Union[str, float]]]:
    # Example message: "i483-sensors-team1-SCD41-co2,1716958268066,566"
    parts = message.split(',')
    topic, _, value = parts
    if len(parts) == 3 and topic != 'i483-sensors-gen-data-increasing':
        try:
            value = float(value)
            return {'topic': topic, 'value': value}
        except ValueError:
            return None
    else:
        return None


parsed_stream = data_stream.map(parse_message).filter(lambda x: x is not None)

keyed_stream = parsed_stream.key_by(lambda x: x['topic'])

aggregated_stream = (
    keyed_stream
    .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(30)))
    .aggregate(MyAggregateFunction())
)


def print_result(result):
    topic = result['topic']
    min_val = str(result['min'])
    max_val = str(result['max'])
    avg_val = str(result['avg'])
    print(f'Topic: {topic}, Min Val: {min_val}, Max Val: {max_val}, Avg Val: {avg_val}')


aggregated_stream.map(print_result)


env.execute()
