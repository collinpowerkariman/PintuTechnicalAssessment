import json
import logging
import os
import sys
from typing import Iterable

from pyflink.common import WatermarkStrategy, Duration, Types, Time, SimpleStringSchema
from pyflink.common.types import Row
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaOffsetResetStrategy, \
    KafkaRecordSerializationSchema, KafkaSink
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import ProcessAllWindowFunction
from pyflink.datastream.state import MapStateDescriptor, MapState
from pyflink.datastream.window import TumblingEventTimeWindows


class CreatedAtTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: Row, record_timestamp: int) -> int:
        return record_timestamp

        # use field created_at as timestamp if producer can maintain monotonously increasing timestamp
        # return value['created_at']


class OrderBookingProcess(ProcessAllWindowFunction):

    def process(self, context: 'ProcessAllWindowFunction.Context', elements: Iterable[Row]) -> Iterable[str]:
        open_order_books_state_desc = MapStateDescriptor("open_order_books", Types.STRING(), Types.PICKLED_BYTE_ARRAY())
        open_order_books = context.global_state().get_map_state(open_order_books_state_desc)
        closed_order_books = []

        for order in elements:
            if order['status'] == 'CLOSED':
                self.__move_values(open_order_books, closed_order_books)
                continue

            key, order_book = self.__to_order_book(order)
            if key in open_order_books:
                open_order_books[key] = self.__merge_order_book(open_order_books[key], order_book)
                continue

            open_order_books[key] = order_book

        return [self.__prepare_result(open_order_books, closed_order_books)]

    @staticmethod
    def __merge_order_book(order_a, order_b):
        order_a['amount'] += order_b['amount']
        order_a['total'] = order_a['price'] * order_a['amount']
        order_a['cum_sum'] = order_a['total']
        return order_a

    @staticmethod
    def __to_order_book(order):
        order_book = {
            'symbol': order['symbol'],
            'side': order['order_side'],
            'price': order['price'],
            'amount': order['size'],
            'total': order['price'] * order['size'],
            'cum_sum': order['price'] * order['size'],
        }
        key = f'{order_book["price"]}@{order_book["side"]}'
        return key, order_book

    @staticmethod
    def __prepare_result(open_order_books: MapState, closed_order_books: list) -> str:
        all_books = closed_order_books
        for book in open_order_books.values():
            all_books.append(book)

        buy_idx = 1
        sell_idx = 1
        prev_buy_cum_sum = 0
        prev_sell_cum_sum = 0

        native = []
        for book in all_books:
            if str(book['side']).startswith("SELL"):
                book['side'] = f'SELL_{sell_idx}'
                sell_idx += 1
                book['cum_sum'] += prev_sell_cum_sum
                prev_sell_cum_sum = book['cum_sum']
            else:
                book['side'] = f'BUY_{buy_idx}'
                buy_idx += 1
                book['cum_sum'] += prev_buy_cum_sum
                prev_buy_cum_sum = book['cum_sum']
            native.append(book)

        return json.dumps(native)

    @staticmethod
    def __move_values(open_order_books, closed_order_books):
        for k in list(open_order_books.keys()):
            closed_order_books.append(open_order_books[k])
            del open_order_books[k]


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.add_jars(f'file://{os.getcwd()}/jars/flink-sql-connector-kafka-3.0.1-1.18.jar')

    schema = Types.ROW_NAMED(['order_id', 'symbol', 'order_side', 'price', 'size', 'status', 'created_at']
                             , [Types.LONG(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(),
                                Types.STRING(), Types.INSTANT()])
    deserializer = JsonRowDeserializationSchema.builder().type_info(schema).build()

    source = KafkaSource.builder() \
        .set_topics('technical_assessment') \
        .set_group_id('order-bookkeeper') \
        .set_bootstrap_servers('localhost:9092') \
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
        .set_value_only_deserializer(deserializer) \
        .build()

    watermark_strategy = WatermarkStrategy \
        .for_monotonous_timestamps() \
        .with_idleness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(CreatedAtTimestampAssigner())

    stream = env.from_source(source_name='order', source=source, watermark_strategy=watermark_strategy)

    stream = stream \
        .window_all(TumblingEventTimeWindows.of(Time.seconds(1))) \
        .process(OrderBookingProcess(), output_type=Types.STRING())

    serializer = KafkaRecordSerializationSchema.builder() \
        .set_topic('order_books') \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_record_serializer(serializer) \
        .build()

    stream.sink_to(sink)
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    main()
