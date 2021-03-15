/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.mqtt;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.lib.mqtt.Errors;
import com.streamsets.pipeline.lib.mqtt.MqttClientCommon;
import com.streamsets.pipeline.lib.mqtt.MqttClientConfigBean;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class MqttClientSource implements PushSource, MqttCallback {

  private static final Logger LOG = LoggerFactory.getLogger(MqttClientSource.class);
  private static final String TOPIC_HEADER_NAME = "topic";
  private final MqttClientConfigBean commonConf;
  private final MqttClientSourceConfigBean subscriberConf;
  private final MqttClientCommon mqttClientCommon;
  private DataParserFactory parserFactory;
  private MqttClient mqttClient;
  private Context context;
  private AtomicLong counter = new AtomicLong();
  private BlockingQueue<Exception> errorQueue;
  private List<Exception> errorList;
  private boolean connectionLostError = false;

  MqttClientSource(MqttClientConfigBean commonConf, MqttClientSourceConfigBean subscriberConf) {
    this.commonConf = commonConf;
    this.subscriberConf = subscriberConf;
    this.mqttClientCommon = new MqttClientCommon(this.commonConf);
  }

  @Override
  public int getNumberOfThreads() {
    return 1;
  }

  @Override
  public List<ConfigIssue> init(Info info, Context context) {
    List<ConfigIssue> issues = new ArrayList<>();
    this.context = context;

    this.mqttClientCommon.init(context, issues);

    subscriberConf.dataFormatConfig.stringBuilderPoolSize = getNumberOfThreads();
    subscriberConf.dataFormatConfig.init(
        context,
        subscriberConf.dataFormat,
        com.streamsets.pipeline.stage.origin.httpserver.Groups.DATA_FORMAT.name(),
        "dataFormatConfig",
        DataFormatConstants.MAX_OVERRUN_LIMIT,
        issues
    );
    parserFactory = subscriberConf.dataFormatConfig.getParserFactory();

    errorQueue = new ArrayBlockingQueue<>(100);
    errorList = new ArrayList<>(100);
    return issues;
  }

  @Override
  public void produce(Map<String, String> map, int i) throws StageException {
    try {
      initializeClient();

      while (!context.isStopped() && !connectionLostError) {
        dispatchHttpReceiverErrors(100);
      }

    } catch(MqttException me) {
      throw new StageException(Errors.MQTT_04, me, me);
    }
  }

  private void initializeClient() throws MqttException, StageException {
    connectionLostError = false;
    mqttClient = mqttClientCommon.createMqttClient(this);
    for (String topicFilter: subscriberConf.topicFilters) {
      mqttClient.subscribe(topicFilter, commonConf.qos.getValue());
    }
  }

  private void dispatchHttpReceiverErrors(long intervalMillis) {
    if (intervalMillis > 0) {
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException ex) {
      }
    }
    // report errors  reported by the HttpReceiverServer
    errorList.clear();
    errorQueue.drainTo(errorList);
    for (Exception exception : errorList) {
      context.reportError(exception);
    }
  }

  @Override
  public void destroy() {
    if (mqttClient != null) {
      try {
        mqttClient.disconnect();
      } catch (MqttException ex) {
        LOG.error(Errors.MQTT_03.getMessage(), ex.toString(), ex);
      }
    }
  }

  @Override
  public void connectionLost(Throwable throwable) {
    destroy();
    try {
      initializeClient();
    } catch(Exception ex) {
      errorQueue.offer(ex);
      LOG.warn("Error while reconnecting to MQTT: {}", ex.toString(), ex);
      connectionLostError = true;
    }
  }

  @Override
  public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
    String requestId = System.currentTimeMillis() + "." + counter.getAndIncrement();
    try (DataParser parser = parserFactory.getParser(requestId, mqttMessage.getPayload())) {
      process(topic, parser);
    } catch (DataParserException ex) {
      errorQueue.offer(ex);
      LOG.warn("Error while processing request payload from: {}", ex.toString(), ex);
    }
  }

  private void process(String topic, DataParser parser) throws IOException, DataParserException {
    BatchContext batchContext = context.startBatch();
    List<Record> records = new ArrayList<>();
    Record parsedRecord = parser.parse();
    while (parsedRecord != null) {
      parsedRecord.getHeader().setAttribute(TOPIC_HEADER_NAME, topic);
      records.add(parsedRecord);
      parsedRecord = parser.parse();
    }
    for (Record record : records) {
      batchContext.getBatchMaker().addRecord(record);
    }
    context.processBatch(batchContext);
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
  }
}
