from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import AggregateFunction, ProcessAllWindowFunction
from pyflink.common.time import Time
import re


class MyAggregateFunction(AggregateFunction):

    def create_accumulator(self):
        return {
            'count': 0,
            'sum': 0.0,
            'min': float('inf'),
            'max': float('-inf')
        }

    def add(self, value, accumulator):
        v = float(value)
        accumulator['count'] += 1
        accumulator['sum'] += v
        accumulator['min'] = min(accumulator['min'], v)
        accumulator['max'] = max(accumulator['max'], v)
        return accumulator

    def get_result(self, accumulator):
        if accumulator['count'] == 0:
            return {
                'avg': 0,
                'min': float('inf'),
                'max': float('-inf'),
                'count': 0
            }
        else:
            return {
                'avg': round(accumulator['sum'] / accumulator['count'], 2),
                'min': round(accumulator['min'], 2),
                'max': round(accumulator['max'], 2),
                'count': accumulator['count']
            }

    def merge(self, acc_a, acc_b):
        acc_a['count'] += acc_b['count']
        acc_a['sum'] += acc_b['sum']
        acc_a['min'] = min(acc_a['min'], acc_b['min'])
        acc_a['max'] = max(acc_a['max'], acc_b['max'])
        return acc_a


def extract_entity_sensor_data_type(topic):
    match = re.match(r'i483-sensors-([a-zA-Z0-9]+)-([A-Z0-9]+)-([a-z_]+)', topic)
    if match:
        return match.groups()
    else:
        raise ValueError(f"Topic {topic} does not match expected pattern")


env = StreamExecutionEnvironment.get_execution_environment()


def create_kafka_producer(topic):
    return FlinkKafkaProducer(
        topic=topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': '150.65.230.59:9092'}
    )


def try_parse_float(value):
    try:
        return float(value)
    except ValueError:
        return None


topics = [
    'i483-sensors-s2410014-BMP180-temperature',
    'i483-sensors-s2410014-BMP180-air_pressure',
    'i483-sensors-s2410014-SCD41-temperature',
    'i483-sensors-s2410014-SCD41-co2',
    'i483-sensors-s2410014-SCD41-humidity',
    'i483-sensors-team2-RPR0521RS-ambient_illumination',
]


for topic in topics:
    # 返却されるデータにトピックの情報が存在しないのでトピック毎にコンシューマーを作成する
    kafka_consumer = FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': '150.65.230.59:9092',
            'group.id': f'flink-consumer-{topic}'
        }
    )

    kafka_consumer.set_start_from_latest()

    data_stream = env.add_source(kafka_consumer)

    parsed_stream = data_stream.map(lambda value: try_parse_float(value), output_type=Types.FLOAT())

    filtered_stream = parsed_stream.filter(lambda value: value is not None)

    aggregated_stream = (
        filtered_stream
        .window_all(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))
        .aggregate(MyAggregateFunction(), output_type=Types.MAP(Types.STRING(), Types.FLOAT()))
    )

    entity, sensor, data_type = extract_entity_sensor_data_type(topic)

    min_stream = aggregated_stream.map(lambda x: str(x['min']), output_type=Types.STRING())
    max_stream = aggregated_stream.map(lambda x: str(x['max']), output_type=Types.STRING())
    avg_stream = aggregated_stream.map(lambda x: str(x['avg']), output_type=Types.STRING())

    min_stream.add_sink(create_kafka_producer(f'i483-sensors-{entity}-analytics-{sensor}-min-{data_type}'))
    max_stream.add_sink(create_kafka_producer(f'i483-sensors-{entity}-analytics-{sensor}-max-{data_type}'))
    avg_stream.add_sink(create_kafka_producer(f'i483-sensors-{entity}-analytics-{sensor}-avg-{data_type}'))

env.execute()
