from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.datastream.functions import AggregateFunction
from pyflink.common.time import Time
from typing import Optional
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchemaBuilder, KafkaSinkBuilder
from pyflink.common.serialization import SerializationSchema
from typing import Tuple
from pyflink.java_gateway import get_gateway
import os
import re
from pyflink.common.typeinfo import Types

BOOTSTRAP_SERVER = '150.65.230.59:9092'
CONSUMER_TOPIC = 'i483-allsensors'
JAR_FILE_NAME = 'JFlinkCustomSerializationSchema-1.0-SNAPSHOT-jar-with-dependencies.jar'
SERIALIZATION_JAR_FILE_PATH = 'file://' + os.path.join(os.getcwd(), 'libs', JAR_FILE_NAME)


class Tuple2SerializationSchema(SerializationSchema):
    def __init__(self):
        gateway = get_gateway()
        j_tuple2_serialization_schema = gateway.jvm.org.serialization.Tuple2SerializationSchema()
        super().__init__(j_serialization_schema=j_tuple2_serialization_schema)


class MyAggregateFunction(AggregateFunction):

    def create_accumulator(self) -> Tuple[Optional[str], int, float, float, float]:
        # topic, count, sum, min, max
        return ('', 0, 0.0, float('inf'), float('-inf'))

    def add(self, value: Tuple[str, float], accumulator: Tuple[str, int, float, float, float]):
        topic = value[0]
        v = value[1]
        new_accumulator = (
            topic,
            accumulator[1] + 1,  # count
            accumulator[2] + v,  # sum
            min(accumulator[3], v),  # min
            max(accumulator[4], v)   # max
        )
        return new_accumulator

    def get_result(self, accumulator: Tuple[str, int, float, float, float]) -> Tuple[str, str, str, str]:
        if accumulator[1] == 0:
            return ('', '', '', '')
        else:
            avg = str(accumulator[2] / accumulator[1])
            min_val = str(accumulator[3])
            max_val = str(accumulator[4])
            return (accumulator[0], avg, min_val, max_val)

    def merge(self, acc_a: Tuple[str, int, float, float, float], acc_b: Tuple[str, int, float, float, float]) -> Tuple[str, int, float, float, float]:
        return (
            acc_a[0],
            acc_a[1] + acc_b[1],
            acc_a[2] + acc_b[2],
            min(acc_a[3], acc_b[3]),
            max(acc_a[4], acc_b[4])
        )


class KafkaSink:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers

    def create_schema(self, topic_selector):
        return KafkaRecordSerializationSchemaBuilder() \
            .set_key_serialization_schema(Tuple2SerializationSchema()) \
            .set_value_serialization_schema(Tuple2SerializationSchema()) \
            .set_topic_selector(topic_selector).build()

    def create_sink(self, schema):
        return KafkaSinkBuilder() \
            .set_bootstrap_servers(self.bootstrap_servers) \
            .set_record_serializer(schema) \
            .build()


def parse_message(message) -> Optional[Tuple[str, float]]:
    parts = message.split(',')
    topic, _, value = parts
    if len(parts) == 3 and topic != 'i483-sensors-gen-data-increasing':
        try:
            return (topic, float(value))
        except ValueError:
            return None
    return None


def extract_entity_sensor_data_type(topic):
    match = re.match(r'i483-sensors-([a-zA-Z0-9]+)-([A-Z0-9]+)-([a-zA-Z0-9_]+)', topic)
    if match:
        return match.groups()
    else:
        raise ValueError(f"Topic {topic} does not match expected pattern")


def create_topic_selector(suffix):
    def topic_selector(row):
        topic, value = row
        entity, sensor, data_type = extract_entity_sensor_data_type(topic)
        producer_topic = f"i483-sensors-s2410014-analytics-{entity}_{sensor}_{suffix}-{data_type}_v3"
        print(f"Topic: {producer_topic} Value: {value}")
        return producer_topic
    return topic_selector


env = StreamExecutionEnvironment.get_execution_environment()

env.add_jars(SERIALIZATION_JAR_FILE_PATH)


kafka_consumer = FlinkKafkaConsumer(
    topics=CONSUMER_TOPIC,
    deserialization_schema=SimpleStringSchema(),
    properties={
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'group.id': f'flink-consumer-{CONSUMER_TOPIC}'
    }
)

kafka_consumer.set_start_from_latest()
data_stream = env.add_source(kafka_consumer)

avg_topic_selector = create_topic_selector('avg')
min_topic_selector = create_topic_selector('min')
max_topic_selector = create_topic_selector('max')

ks = KafkaSink(BOOTSTRAP_SERVER)

avg_val_schema = ks.create_schema(avg_topic_selector)
min_val_schema = ks.create_schema(min_topic_selector)
max_val_schema = ks.create_schema(max_topic_selector)

avg_val_sink = ks.create_sink(avg_val_schema)
min_val_sink = ks.create_sink(min_val_schema)
max_val_sink = ks.create_sink(max_val_schema)

parsed_stream = data_stream.map(parse_message).filter(lambda x: x is not None)
keyed_stream = parsed_stream.key_by(lambda x: x[0])  # key data streams by topic
agg_stream = (
    keyed_stream
    .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(30)))
    .aggregate(
        MyAggregateFunction(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()])
    )
)

agg_avg_stream = agg_stream.map(lambda x: (x[0], x[1]), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
agg_min_stream = agg_stream.map(lambda x: (x[0], x[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
agg_max_stream = agg_stream.map(lambda x: (x[0], x[3]), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))

_ = agg_avg_stream.sink_to(avg_val_sink)
_ = agg_min_stream.sink_to(min_val_sink)
_ = agg_max_stream.sink_to(max_val_sink)

env.execute()
