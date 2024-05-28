from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.datastream.functions import AggregateFunction, MapFunction
from pyflink.common.time import Time


class MyAggregateFunction(AggregateFunction):

    def create_accumulator(self):
        # initialize new accumulator
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
        return {
            'avg': accumulator['sum'] / accumulator['count'] if accumulator['count'] != 0 else 0,
            'min': accumulator['min'],
            'max': accumulator['max'],
            'count': accumulator['count']
        }

    def merge(self, a, b):
        a['count'] += b['count']
        a['sum'] += b['sum']
        a['min'] = min(a['min'], b['min'])
        a['max'] = max(a['max'], b['max'])
        return a


env = StreamExecutionEnvironment.get_execution_environment()


kafka_consumer = FlinkKafkaConsumer(
    topics='i483-sensors-s2410014-BMP180-temperature',
    deserialization_schema=SimpleStringSchema(),
    properties={
        'bootstrap.servers': '150.65.230.59:9092',
        'group.id': 'kafka-consumer'
    }
)

kafka_consumer.set_start_from_latest()


def create_kafka_producer(topic):
    return FlinkKafkaProducer(
        topic=topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': '150.65.230.59:9092'}
    )


data_stream = env.add_source(kafka_consumer)


float_stream = data_stream.map(lambda value: float(value), output_type=Types.FLOAT())


aggregated_stream = (
    float_stream
    .window_all(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(30)))
    .aggregate(MyAggregateFunction(), output_type=Types.MAP(Types.STRING(), Types.FLOAT()))
)

aggregated_stream.map(lambda x: str(x['min']), output_type=Types.STRING()) \
    .add_sink(create_kafka_producer('i483-sensors-s2410014-analytics-BMP180-temperature-min'))

aggregated_stream.map(lambda x: str(x['max']), output_type=Types.STRING()) \
    .add_sink(create_kafka_producer('i483-sensors-s2410014-analytics-BMP180-temperature-max'))

aggregated_stream.map(lambda x: str(x['avg']), output_type=Types.STRING()) \
    .add_sink(create_kafka_producer('i483-sensors-s2410014-analytics-BMP180-temperature-avg'))


env.execute()
