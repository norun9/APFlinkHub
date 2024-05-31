from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.datastream.functions import AggregateFunction
from pyflink.common.time import Time
from typing import Optional, Dict, Union
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchemaBuilder, KafkaSinkBuilder, DeliveryGuarantee
from pyflink.common.serializer import TypeSerializer
from pyflink.common.serialization import SerializationSchema
from io import BytesIO
from typing import Tuple
import json
from pyflink.java_gateway import get_gateway
import os
import re
from pyflink.common.typeinfo import Types


# https://nightlies.apache.org/flink/flink-docs-master/api/python/reference/pyflink.common/api/pyflink.common.serializer.TypeSerializer.html#pyflink.common.serializer.TypeSerializer
class Tuple2SerializationSchema(SerializationSchema):
    def __init__(self):
        gateway = get_gateway()
        # jvmのSerializationクラス
        j_dict_serialization_schema = gateway.jvm.org.serialization.Tuple2SerializationSchema()
        super().__init__(j_serialization_schema=j_dict_serialization_schema)


class MyAggregateFunction(AggregateFunction):

    def create_accumulator(self) -> Tuple[Optional[str], int, float, float, float]:
        # topic, count, sum, min, max,
        return ('', 0, 0.0, float('inf'), float('-inf'))

    def add(self, value: Tuple[str, float], accumulator: Tuple[str, int, float, float, float]):
        topic = value[0]
        v = float(value[1])  # value
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
            # topic, avg, min, max
            avg = str(round(accumulator[2] / accumulator[1], 2))
            min = str(round(accumulator[3], 2))
            max = str(round(accumulator[4], 2))
            return (accumulator[0], avg, min, max)

    def merge(self, acc_a: Tuple[str, int, float, float, float], acc_b: Tuple[str, int, float, float, float]) -> Tuple[str, int, float, float, float]:
        acc_a[1] += acc_b[1]
        acc_a[2] += acc_b[2]
        acc_a[3] = min(acc_a[3], acc_b[3])
        acc_a[4] = max(acc_a[4], acc_b[4])
        acc_a[0] = acc_b[0]
        return acc_a


def parse_message(message) -> Optional[Tuple[str, float]]:
    # Example message: "i483-sensors-team1-SCD41-co2,1716958268066,566"
    parts = message.split(',')
    topic, _, value = parts
    if len(parts) == 3 and topic != 'i483-sensors-gen-data-increasing':
        try:
            value = float(value)
            return (topic, value)
        except ValueError:
            return None
    else:
        return None


env = StreamExecutionEnvironment.get_execution_environment()

jar_file_path = 'file://' + os.path.join(os.getcwd(), 'JFlinkCustomSerializationSchema-1.0-SNAPSHOT-jar-with-dependencies.jar')
env.add_jars(jar_file_path)

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


def extract_entity_sensor_data_type(topic):
    match = re.match(r'i483-sensors-([a-zA-Z0-9]+)-([A-Z0-9]+)-([a-zA-Z0-9_]+)', topic)
    if match:
        return match.groups()
    else:
        raise ValueError(f"Topic {topic} does not match expected pattern")


def avg_topic_selector(row):
    topic = row[0]
    avg = row[1]
    entity, sensor, data_type = extract_entity_sensor_data_type(topic)
    producer_topic = f"i483-sensors-s2410014-analytics-{entity}_{sensor}_avg-{data_type}"
    print(f"Topic: {producer_topic} Value: {avg}")
    return producer_topic


def min_topic_selector(row):
    topic = row[0]
    min = row[1]
    entity, sensor, data_type = extract_entity_sensor_data_type(topic)
    producer_topic = f"i483-sensors-s2410014-analytics-{entity}_{sensor}_min-{data_type}"
    print(f"Topic: {producer_topic} Value: {min}")
    return producer_topic


def max_topic_selector(row):
    topic = row[0]
    max = row[1]
    entity, sensor, data_type = extract_entity_sensor_data_type(topic)
    producer_topic = f"i483-sensors-s2410014-analytics-{entity}_{sensor}_max-{data_type}"
    print(f"Topic: {producer_topic} Value: {max}")
    return producer_topic


bootstrap_server = '150.65.230.59:9092'

# https://nightlies.apache.org/flink/flink-docs-master/api/python/reference/pyflink.datastream/api/pyflink.datastream.connectors.kafka.KafkaRecordSerializationSchemaBuilder.html#pyflink.datastream.connectors.kafka.KafkaRecordSerializationSchemaBuilder
# keyもTuple4をシリアライズしているみたいだが、なぜ？keyには何が入ってくるの？
avg_schema = KafkaRecordSerializationSchemaBuilder() \
    .set_key_serialization_schema(Tuple2SerializationSchema()) \
    .set_value_serialization_schema(Tuple2SerializationSchema()) \
    .set_topic_selector(avg_topic_selector).build()

min_schema = KafkaRecordSerializationSchemaBuilder() \
    .set_key_serialization_schema(Tuple2SerializationSchema()) \
    .set_value_serialization_schema(Tuple2SerializationSchema()) \
    .set_topic_selector(min_topic_selector).build()

max_schema = KafkaRecordSerializationSchemaBuilder() \
    .set_key_serialization_schema(Tuple2SerializationSchema()) \
    .set_value_serialization_schema(Tuple2SerializationSchema()) \
    .set_topic_selector(max_topic_selector).build()

# https://nightlies.apache.org/flink/flink-docs-master/api/python/reference/pyflink.datastream/api/pyflink.datastream.connectors.kafka.KafkaSinkBuilder.html#pyflink.datastream.connectors.kafka.KafkaSinkBuilder
avg_sink = KafkaSinkBuilder() \
    .set_bootstrap_servers(bootstrap_server) \
    .set_record_serializer(avg_schema) \
    .build()

min_sink = KafkaSinkBuilder() \
    .set_bootstrap_servers(bootstrap_server) \
    .set_record_serializer(min_schema) \
    .build()

max_sink = KafkaSinkBuilder() \
    .set_bootstrap_servers(bootstrap_server) \
    .set_record_serializer(max_schema) \
    .build()


parsed_stream = data_stream.map(parse_message).filter(lambda x: x is not None)

keyed_stream = parsed_stream.key_by(lambda x: x[0])

# output_type指定しないとjava.lang.ClassCastException
aggregated_stream = (
    keyed_stream
    .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(30)))
    .aggregate(
        MyAggregateFunction(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]),
    )
)

aggregated_avg_stream = aggregated_stream.map(lambda x: (x[0], x[1]), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
aggregated_min_stream = aggregated_stream.map(lambda x: (x[0], x[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
aggregated_max_stream = aggregated_stream.map(lambda x: (x[0], x[3]), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))

_ = aggregated_avg_stream.sink_to(avg_sink)
_ = aggregated_min_stream.sink_to(min_sink)
_ = aggregated_max_stream.sink_to(max_sink)

env.execute()
