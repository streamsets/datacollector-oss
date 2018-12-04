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
package com.streamsets.pipeline.stage.destination.mqtt;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.http.HttpClientCommon;
import com.streamsets.pipeline.lib.mqtt.Errors;
import com.streamsets.pipeline.lib.mqtt.Groups;
import com.streamsets.pipeline.lib.mqtt.MqttClientCommon;
import com.streamsets.pipeline.lib.mqtt.MqttClientConfigBean;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class MqttClientTarget extends BaseTarget implements MqttCallback {

  private static final Logger LOG = LoggerFactory.getLogger(MqttClientTarget.class);
  private final MqttClientConfigBean commonConf;
  private final MqttClientTargetConfigBean publisherConf;
  private final MqttClientCommon mqttClientCommon;
  private DataGeneratorFactory generatorFactory;
  private ErrorRecordHandler errorRecordHandler;
  private MqttClient mqttClient = null;

  private ELEval topicEval;
  private ELVars topicVars;
  private Set<String> allowedTopics;
  private boolean allowAllTopics;

  MqttClientTarget(MqttClientConfigBean commonConf, MqttClientTargetConfigBean publisherConf) {
    this.commonConf = commonConf;
    this.publisherConf = publisherConf;
    this.mqttClientCommon = new MqttClientCommon(this.commonConf);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    if(issues.size() == 0) {
      publisherConf.dataGeneratorFormatConfig.init(
          getContext(),
          publisherConf.dataFormat,
          Groups.MQTT.name(),
          HttpClientCommon.DATA_FORMAT_CONFIG_PREFIX,
          issues
      );
      generatorFactory = publisherConf.dataGeneratorFormatConfig.getDataGeneratorFactory();

      try {
        this.mqttClientCommon.init(getContext(), issues);
        mqttClient = mqttClientCommon.createMqttClient(this);
      } catch (MqttException|StageException e) {
        throw new RuntimeException(new StageException(Errors.MQTT_04, e, e));
      }

      this.allowedTopics = new HashSet<>();
      allowAllTopics = false;
      if (publisherConf.runtimeTopicResolution) {
        if (publisherConf.topicExpression == null || publisherConf.topicExpression.trim().isEmpty()) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.MQTT.name(),
                  "publisherConf.topicExpression",
                  Errors.MQTT_05
              )
          );
        }
        validateTopicExpression(getContext(), issues);
        validateTopicWhiteList(getContext(), issues);
      }
    }

    return issues;
  }

  private void validateTopicExpression(Stage.Context context, List<Stage.ConfigIssue> issues) {
    topicEval = context.createELEval("topicExpression");
    topicVars = context.createELVars();
    ELUtils.validateExpression(publisherConf.topicExpression,
        context,
        Groups.MQTT.name(),
        "publisherConf.topicExpression",
        Errors.MQTT_06, issues
    );
  }

  private void validateTopicWhiteList(Stage.Context context, List<Stage.ConfigIssue> issues) {
    // if runtimeTopicResolution then topic white list cannot be empty
    if (isEmpty(publisherConf.topicWhiteList)) {
      issues.add(
          context.createConfigIssue(
              Groups.MQTT.name(),
              "publisherConf.topicWhiteList",
              Errors.MQTT_07
          )
      );
    } else if ("*".equals(publisherConf.topicWhiteList)) {
      allowAllTopics = true;
    } else {
      // Must be comma separated list of topic names
      String[] topics = publisherConf.topicWhiteList.split(",");
      for (String t : topics) {
        allowedTopics.add(t.trim());
      }
    }
  }

  /**
   * Returns the topic given the record.
   *
   * Returns the configured topic or statically evaluated topic in case runtime resolution is not required.
   *
   * If runtime resolution is required then the following is done:
   * 1. Resolve the topic name by evaluating the topic expression
   * 2. If the white list does not contain topic name and white list is not configured with "*" throw StageException
   *    and the record will be handled based on the OnError configuration for the stage
   * */
  String getTopic(Record record) throws StageException {
    String result = publisherConf.topic;
    if (publisherConf.runtimeTopicResolution) {
      RecordEL.setRecordInContext(topicVars, record);
      try {
        result = topicEval.eval(topicVars, publisherConf.topicExpression, String.class);
        if (isEmpty(result)) {
          throw new StageException(Errors.MQTT_08, publisherConf.topicExpression, record.getHeader().getSourceId());
        }
        if (!allowedTopics.contains(result) && !allowAllTopics) {
          throw new StageException(Errors.MQTT_09, result, record.getHeader().getSourceId());
        }
      } catch (ELEvalException e) {
        throw new StageException(
            Errors.MQTT_10,
            publisherConf.topicExpression,
            record.getHeader().getSourceId(),
            e.toString()
        );
      }
    }
    return result;
  }


  @Override
  public void write(Batch batch) throws StageException {
    try {
      if (!mqttClient.isConnected()) {
        // if connection is closed try reconnecting
        mqttClient = mqttClientCommon.createMqttClient(this);
      }
      Iterator<Record> records = batch.getRecords();
      while (records.hasNext()) {
        Record record = records.next();
        ByteArrayOutputStream byteBufferOutputStream = new ByteArrayOutputStream();
        try (DataGenerator dataGenerator = generatorFactory.getGenerator(byteBufferOutputStream)) {
          dataGenerator.write(record);
          dataGenerator.flush();
          MqttMessage message = new MqttMessage(byteBufferOutputStream.toByteArray());
          message.setQos(commonConf.qos.getValue());
          message.setRetained(publisherConf.retained);
          mqttClient.publish(getTopic(record), message);
        } catch(Exception ex) {
          LOG.error(Errors.MQTT_01.getMessage(), ex.toString(), ex);
          errorRecordHandler.onError(new OnRecordErrorException(record, Errors.MQTT_01, ex.getMessage()));
        }
      }
    } catch(Exception ex) {
      LOG.error(Errors.MQTT_01.getMessage(), ex.toString(), ex);
      errorRecordHandler.onError(Lists.newArrayList(batch.getRecords()), throwStageException(ex));
    }
  }

  private static StageException throwStageException(Exception e) {
    if (e instanceof RuntimeException) {
      Throwable cause = e.getCause();
      if (cause != null) {
        return new StageException(Errors.MQTT_01, cause, cause);
      }
    } else if (e instanceof StageException) {
      return (StageException)e;
    }
    return new StageException(Errors.MQTT_01, e, e);
  }


  @Override
  public void destroy() {
    super.destroy();
    if (mqttClient != null) {
      try {
        mqttClient.disconnect();
      } catch (MqttException ex) {
        LOG.error(Errors.MQTT_03.getMessage(), ex.toString(), ex);
      }
    }
  }

  @Override
  public void connectionLost(Throwable cause) {
    // pipeline retry logic will retry to reconnect
    throw new RuntimeException((new StageException(Errors.MQTT_00, cause, cause)));
  }

  @Override
  public void messageArrived(String topic, MqttMessage message) {
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {
  }
}
