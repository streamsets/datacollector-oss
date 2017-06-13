#!/usr/bin/env python
#
# Copyright 2017 StreamSets Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# -*- coding: utf-8 -*-
import json
import logging
import sdc
import subprocess
import time

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


class TestStreamSetsDataCollector:
    sdc_urls = []

    def __init__(self):
        pass

    @classmethod
    def setup_class(cls):
        required_sdcs = [
            ('node30.local', 18630),
            ('node31.local', 18630)
        ]
        cls.sdc_urls = []
        for hostPort in required_sdcs:
            cls.sdc_urls.append("http://%s:%s/rest/v1/" % (hostPort[0], hostPort[1]))
        for sdc_url in cls.sdc_urls:
            api = sdc.api.DataCollector(sdc_url)
            api.ping()

    @classmethod
    def teardown_class(cls):
        pass

    @classmethod
    def setup(cls):
        # Clear any existing pipelines
        for collector in cls.sdc_urls:
            api = sdc.api.DataCollector(collector)
            pipeline_names = api.list_pipeline_names()
            for name in pipeline_names:
                api.delete_pipeline(name)

    def test_import_pipeline(self):
        api = sdc.api.DataCollector(self.sdc_urls[0])
        with open('resources/SIMPLE_PIPELINE.json') as f:
            pipelineDef = f.read()
            logger.info("SIMPLE PIPELINE = " + pipelineDef)
            pipeline = json.loads(pipelineDef)
            api.import_pipeline(pipeline)

        pipeline_names = api.list_pipeline_names()
        num_pipelines = len(pipeline_names)
        assert num_pipelines == 1, 'Expected 1 pipeline but found %s' % num_pipelines
        assert 'SIMPLE_PIPELINE' in pipeline_names, \
            "Didn't find added pipeline in the library. [%s]" % (', '.join(pipeline_names))

    def test_dual_pipelines(self):
        cmd = 'sudo kafka-topics --create --topic TEST_TOPIC --zookeeper localhost:2181 --partition 1 --replication-factor 1'
        subprocess.call('ssh -t -l ec2-user node00.local "%s 2>/dev/null"' % (cmd), shell=True)

        num_sdcs = len(self.sdc_urls)
        assert num_sdcs > 1, 'This test requires at least 2 SDCs running but found %s' % num_sdcs

        source_api = sdc.api.DataCollector(self.sdc_urls[0])
        dest_api = sdc.api.DataCollector(self.sdc_urls[1])
        with open('resources/KAFKA_CONSUMER.json') as f:
            source = json.loads(f.read())
            source_api.import_pipeline(source)

        with open('resources/RANDOM_KAFKA_PRODUCER.json') as f:
            dest = json.loads(f.read())
            dest_api.import_pipeline(dest)

        source_api.start_pipeline('KAFKA_CONSUMER')
        dest_api.start_pipeline('RANDOM_KAFKA_PRODUCER')

        self._check_state(source_api, 'RUNNING')
        self._check_state(dest_api, 'RUNNING')

        # producer
        assert self._check_counter_gtet(
            dest_api,
            'stage.com_streamsets_pipeline_stage_devtest_RandomSource1427993628355.outputRecords.counter',
            10,
        ), 'Expected >= 10 output records from source pipeline Random origin.'

        assert self._check_counter_gtet(
            dest_api,
            'stage.com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget1427993635467.outputRecords.counter',
            10,
        ), 'Expected >= 10 output records at the source pipeline Kafka target.'

        # consumer
        assert self._check_counter_gtet(
            source_api,
            'stage.com_streamsets_pipeline_stage_origin_kafka_KafkaDSource1427993703274.outputRecords.counter',
            10,
        ), 'Expected >= 10 output records at the dest pipeline Kafka origin.'

        assert self._check_counter_gtet(
            source_api,
            'stage.com_streamsets_pipeline_stage_destination_devnull_NullDTarget1427993710330.inputRecords.counter',
            10,
        ), 'Expected >= 10 input records at dest pipeline null target.'

        time.sleep(5)

        dest_api.stop_pipeline()
        source_api.stop_pipeline()

        time.sleep(5)

        assert self._check_state(source_api, 'STOPPED')
        assert self._check_state(dest_api, 'STOPPED')

    @classmethod
    def _check_counter_gtet(cls, pipeline, metric_name, expected, max_tries=60):
        """Repeatedly check metrics until expected condition is achieved or fail."""
        tries = 0
        actual = [None]
        while tries < max_tries:
            logger.debug("pipeline.pipeline_metrics() = " + str(pipeline.pipeline_metrics()))
            actual = list(cls._find_key(pipeline.pipeline_metrics(), metric_name))
            assert len(actual) == 1, 'There should be exactly 1 matching metric, there were: %s' % len(actual)
            actual = actual[0]['count']
            logger.info("Metric %s is expected to be greater than %s and is %s" % (metric_name, expected, actual))
            if actual > expected:
              return True
            tries += 1
            time.sleep(5)
        logger.error('Expected: %s for metric %s but found: %s' % (expected, metric_name, actual[0]))
        return False

    @classmethod
    def _find_key(cls, dictionary, target):
        for k, v in dictionary.iteritems():
            if k == target:
                yield v
            elif isinstance(v, dict):
                for result in cls._find_key(v, target):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in cls._find_key(d, target):
                        yield result

    @staticmethod
    def _check_state(api, expected, max_tries=5):
        tries = 0
        while tries < max_tries:
            status = api.pipeline_status()
            if status and expected == status['state']:
                return True
            tries += 1
            time.sleep(1)
        logger.error('Pipeline was expected to be %s but was [%s]' % (expected, status))
        return False
