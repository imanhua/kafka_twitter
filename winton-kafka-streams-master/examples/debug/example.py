"""
Winton Kafka Streams

Main entrypoints

"""

import logging
import time

from winton_kafka_streams.processor import BaseProcessor, TopologyBuilder
import winton_kafka_streams.kafka_config as kafka_config
import winton_kafka_streams.kafka_streams as kafka_streams
import winton_kafka_streams.state as state_stores
import argparse

log = logging.getLogger(__name__)


class DoubleProcessor(BaseProcessor):
    """
    Example processor that will double the value passed in

    """

    def initialise(self, name, context):
        super().initialise(name, context)
        self.state = context.get_store('double_store')

    def process(self, _, value):
        log.debug(f'DoubleProcessor::process({str(value)})')
        doubled = value*2
        items_in_state = len(self.state)
        self.state[items_in_state] = doubled
        if items_in_state >= 4:
            self.punctuate()

    def punctuate(self):
        for _, value in self.state.items():
            log.debug(f'Forwarding to sink ({str(value)})')
            self.context.forward(None, value)
        self.state.clear()


def _debug_run(config_file):
    kafka_config.read_local_config(config_file)

    double_store = state_stores.create('double_store').with_integer_keys().with_integer_values().in_memory().build()

    with TopologyBuilder() as topology_builder:
        topology_builder.source('input-value', ['wks-debug-example-topic-rz']).\
            processor('double', DoubleProcessor, 'input-value').\
            state_store(double_store, 'double').\
            sink('output-double', 'wks-debug-output-rz', 'double')

    wks = kafka_streams.KafkaStreams(topology_builder, kafka_config)
    wks.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        wks.close()


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description="Debug runner for Python Kafka Streams")
    parser.add_argument('--config-file', '-c', help="Local configuration - will override internal defaults",
                        default='config.properties')
    args = parser.parse_args()

    _debug_run(args.config_file)
