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
package com.streamsets.pipeline.kafka.impl;


import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class BaseKafkaProducer09 implements SdcKafkaProducer {

  private KafkaProducer producer;
  private final List<Future<RecordMetadata>> futureList;

  public BaseKafkaProducer09() {
    this.futureList = new ArrayList<>();
  }

  @Override
  public void init() throws StageException {
    producer = createKafkaProducer();
  }

  @Override
  public void destroy() {
    if(producer != null) {
      producer.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void enqueueMessage(String topic, Object message, Object partitionKey) {
    ProducerRecord e = new ProducerRecord<>(topic, partitionKey, message);
    // send will place this record in the buffer to be batched later
    futureList.add(producer.send(e));
  }

  @Override
  public void write() throws StageException {
    // force all records in the buffer to be written out
    producer.flush();
    // make sure each record was written and handle exception if any
    List<Integer> failedRecordIndices = new ArrayList<Integer>();
    List<Exception> failedRecordExceptions = new ArrayList<Exception>();
    for (int i = 0; i < futureList.size(); i++) {
      Future<RecordMetadata> f = futureList.get(i);
      try {
        f.get();
      } catch (InterruptedException | ExecutionException e) {
        Throwable actualCause = e.getCause();
        if (actualCause != null && actualCause instanceof RecordTooLargeException) {
          failedRecordIndices.add(i);
          failedRecordExceptions.add((Exception)actualCause);
        } else {
          throw createWriteException(e);
        }
      }
    }
    futureList.clear();
    if (!failedRecordIndices.isEmpty()) {
      throw new StageException(KafkaErrors.KAFKA_69, failedRecordIndices, failedRecordExceptions);
    }
  }

  @Override
  public void clearMessages() {
    futureList.clear();
  }

  @Override
  public String getVersion() {
    return Kafka09Constants.KAFKA_VERSION;
  }

  protected abstract KafkaProducer<String, byte[]> createKafkaProducer();

  protected abstract StageException createWriteException(Exception e);

}
