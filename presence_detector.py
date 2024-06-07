from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.datastream.functions import AggregateFunction
from pyflink.common.time import Time
import datetime
from typing import Optional


class MyAggregateFunction(AggregateFunction):

    def create_accumulator(self):
        return {
            'type': '',
            'count': 0,
            'sum': 0.0,
        }

    def add(self, value, accumulator):
        sensor_type, reading = value  # value is a tuple ('CO2' or 'Illumination', float_value)
        v = float(reading)
        accumulator['type'] = sensor_type
        accumulator['count'] += 1
        accumulator['sum'] += v
        return accumulator

    def get_result(self, accumulator):
        return {
            'type': accumulator['type'],
            'avg': accumulator['sum'] / accumulator['count'] if accumulator['count'] > 0 else 0
        }

    def merge(self, a, b):
        return {
            'type': a['type'],
            'count': a['count'] + b['count'],
            'sum': a['sum'] + b['sum'],
        }


co2_avg = 0.0
illumination_avg = 0.0
threshold_exceeded: Optional[bool] = None

CO2_TOPIC = 'i483-sensors-s2410014-SCD41-co2'
ILLUMINATION_TOPIC = 'i483-sensors-team2-RPR0521RS-ambient_illumination'
BOOTSTRAP_SERVER = '150.65.230.59:9092'
CO2_TYPE = 'CO2'
ILLUMINATION_TYPE = 'Illumination'
PRODUCE_TOPIC = 'i483-sensors-s2410014-presence_detector'

env = StreamExecutionEnvironment.get_execution_environment()


def create_kafka_producer(topic: str):
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


def detect_presence(value):
    data_type = value['type']  # Illumination or CO2
    reading = value['avg']

    current_hour = datetime.datetime.now().hour  # Assume you have imported datetime and you get the current hour

    global co2_avg, illumination_avg  # Use global variables to store the averages for CO2 and Illumination
    global threshold_exceeded

    if data_type == CO2_TYPE:
        co2_avg = reading
    elif data_type == ILLUMINATION_TYPE:
        illumination_avg = reading

    print(f'Type:{data_type} Reading:{reading} Hour:{current_hour}')

    if threshold_exceeded is None:
        if co2_avg > 700 or ((18 <= current_hour or current_hour < 4) and illumination_avg > 300):
            threshold_exceeded = True
            return 'yes'
        else:
            threshold_exceeded = False
            return 'no'
    else:
        if threshold_exceeded is False and (co2_avg >= 700 or ((18 <= current_hour or current_hour < 4) and illumination_avg > 300)):
            threshold_exceeded = True
            return 'yes'
        elif threshold_exceeded is True and ((co2_avg < 700) and not ((18 <= current_hour or current_hour < 4) and illumination_avg > 300)):
            threshold_exceeded = False
            return 'no'

    return None


co2_consumer = FlinkKafkaConsumer(
    topics=CO2_TOPIC,
    deserialization_schema=SimpleStringSchema(),
    properties={
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'group.id': f'human-detector-{CO2_TOPIC}'
    }
)

illumination_consumer = FlinkKafkaConsumer(
    topics=ILLUMINATION_TOPIC,
    deserialization_schema=SimpleStringSchema(),
    properties={
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'group.id': f'human-detector-{ILLUMINATION_TOPIC}'
    }
)

co2_consumer.set_start_from_latest()
illumination_consumer.set_start_from_latest()

co2_ds = env.add_source(co2_consumer) \
    .map(lambda value: try_parse_float(value), output_type=Types.FLOAT()) \
    .filter(lambda value: value is not None)

illumination_ds = env.add_source(illumination_consumer) \
    .map(lambda value: try_parse_float(value), output_type=Types.FLOAT()) \
    .filter(lambda value: value is not None)

co2_parsed = co2_ds.map(lambda x: (CO2_TYPE, x), output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()]))
illumination_parsed = illumination_ds.map(lambda x: (ILLUMINATION_TYPE, x), output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()]))

merged_ds = co2_parsed.union(illumination_parsed)

keyed_stream = merged_ds.key_by(lambda x: x[0])

aggregated_ds = (
    keyed_stream
    .window_all(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(30)))
    .aggregate(MyAggregateFunction(), output_type=Types.MAP(Types.STRING(), Types.FLOAT()))
)

presence_detected_ds = aggregated_ds.map(detect_presence, output_type=Types.STRING()).filter(lambda x: x is not None)

presence_detected_ds.add_sink(create_kafka_producer(PRODUCE_TOPIC))

env.execute()
