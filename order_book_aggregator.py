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
from pyflink.datastream.state import MapStateDescriptor, ListStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows


class CreatedAtTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: Row, record_timestamp: int) -> int:
        # return record_timestamp

        # use field created_at as timestamp if producer can maintain monotonously increasing timestamp
        return value['created_at']


class OrderBookingProcess(ProcessAllWindowFunction):
    orders_state_desc = MapStateDescriptor("orders", Types.LONG(), Types.TUPLE([Types.STRING(), Types.DOUBLE()]))
    closed_ids_state_desc = ListStateDescriptor("closed_ids", Types.LONG())
    bookings_state_desc = MapStateDescriptor("bookings", Types.STRING(), Types.PICKLED_BYTE_ARRAY())

    def process(self, context: 'ProcessAllWindowFunction.Context', elements: Iterable[Row]) -> Iterable[str]:
        orders = context.global_state().get_map_state(self.orders_state_desc)
        closed_ids = context.global_state().get_list_state(self.closed_ids_state_desc)
        bookings = context.global_state().get_map_state(self.bookings_state_desc)

        for order_id in closed_ids:
            booking_key = orders[order_id][0]
            bookings[booking_key]['amount'] -= orders[order_id][1]
            del orders[order_id]

            if bookings[booking_key]['amount'] <= 0:
                del bookings[booking_key]

        closed_ids.clear()

        for order in elements:
            order_id = order['order_id']
            if str(order['status']).lower() == 'closed':
                closed_ids.add(order_id)
                continue

            booking_key = f"{order['order_side']}@{order['price']}".lower()
            orders[order_id] = (booking_key, order['size'])
            if booking_key in bookings:
                bookings[booking_key]['amount'] += order['size']
                continue

            bookings[booking_key] = {
                'symbol': order['symbol'],
                'side': order['order_side'],
                'price': order['price'],
                'amount': order['size'],
            }

        buy_idx = 1
        buy_sum = 0
        sell_idx = 1
        sell_sum = 0

        native = []
        booking_keys = list(bookings.keys())
        for booking_key in booking_keys:
            booking = bookings[booking_key]
            if str(booking_key).startswith('buy'):
                booking['side'] = f'BUY_{buy_idx}'
                buy_idx += 1
                booking['total'] = booking['price'] * booking['amount']
                booking['cum_sum'] = booking['total'] + buy_sum
                buy_sum += booking['total']
            else:
                booking['side'] = f'SELL_{sell_idx}'
                sell_idx += 1
                booking['total'] = booking['price'] * booking['amount']
                booking['cum_sum'] = booking['total'] + sell_sum
                sell_sum += booking['total']

            native.append(booking)

        return [json.dumps(native)]


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
