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
package com.streamsets.pipeline.stage.origin.cdc.maprdb;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ojai.FieldPath;
import org.ojai.KeyValue;
import org.ojai.Value;
import org.ojai.store.cdc.ChangeDataRecord;
import org.ojai.store.cdc.ChangeNode;
import org.ojai.types.ODate;
import org.ojai.types.OInterval;
import org.ojai.types.OTime;
import org.ojai.types.OTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class MapRDBCDCSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(MapRDBCDCSource.class);
  private static final String MAPR_FIELD_PATH = "mapr.field.path";
  private static final String MAPR_TABLE_NAME = "mapr.table.name";
  private static final String MAPR_OP_TIMESTAMP = "mapr.op.timestamp";
  private static final String MAPR_SERVER_TIMESTAMP = "mapr.server.timestamp";

  private MapRDBCDCBeanConfig conf;
  private AtomicBoolean shutdownCalled = new AtomicBoolean(false);
  private int batchSize;

  private MapRDBCDCKafkaConsumerFactory consumerFactory;
  private ExecutorService executor;


  public MapRDBCDCSource(MapRDBCDCBeanConfig conf, MapRDBCDCKafkaConsumerFactory consumerFactory) {
    this.conf = conf;
    batchSize = conf.maxBatchSize;
    this.consumerFactory = consumerFactory;
  }

  public class MapRDBCDCCallable implements Callable<Long> {
    private KafkaConsumer<byte[], ChangeDataRecord> consumer;
    private final long threadID;
    private final List<String> topicList;
    private final CountDownLatch startProcessingGate;

    public MapRDBCDCCallable(
        long threadID, List<String> topicList, KafkaConsumer<byte[], ChangeDataRecord> consumer, CountDownLatch startProcessingGate
    ) {
      Thread.currentThread().setName("maprKafkaConsumerThread-" + threadID);
      LOG.trace("MapRDBCDC thread {} begin", Thread.currentThread().getName());
      this.consumer = consumer;
      this.threadID = threadID;
      this.topicList = topicList;
      this.startProcessingGate = startProcessingGate;
    }

    @Override
    public Long call() throws Exception {
      LOG.trace("Starting poll loop in thread {}", Thread.currentThread().getName());
      long messagesProcessed = 0;

      //wait until all threads are spun up before processing
      startProcessingGate.await();

      try {
        consumer.subscribe(topicList);

        while (!getContext().isStopped()) {
          BatchContext batchContext = getContext().startBatch();
          ConsumerRecords<byte[], ChangeDataRecord> messages = consumer.poll(conf.batchWaitTime);

          for (ConsumerRecord<byte[], ChangeDataRecord> message : messages) {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put(HeaderAttributeConstants.TOPIC, message.topic());
            attributes.put(HeaderAttributeConstants.PARTITION, String.valueOf(message.partition()));
            attributes.put(HeaderAttributeConstants.OFFSET, String.valueOf(message.offset()));
            attributes.put(MAPR_TABLE_NAME, conf.topicTableList.get(message.topic()));

            iterateNode(message.value(), batchContext.getBatchMaker(), attributes);
          }

          getContext().processBatch(batchContext);
          messagesProcessed += messages.count();
          LOG.info("MapRDBCDC thread {} finished processing {} messages", threadID, messages.count());
        }
      } catch (Exception e) {
        LOG.error("Encountered error in MapRDBCDC thread {} during read {}", threadID, e);
        handleException(MaprDBCDCErrors.MAPRDB_03, e.getMessage(), e);
      } finally {
        consumer.unsubscribe();
        consumer.close();
      }

      LOG.info("MapRDBCDC kafka thread {} consumed {} messages", threadID, messagesProcessed);
      return messagesProcessed;
    }

    private void iterateNode(
        ChangeDataRecord changeRecord, BatchMaker batchMaker, Map<String, Object> attributes
    ) throws StageException {
      switch (changeRecord.getType()) {
        case RECORD_INSERT:
        case RECORD_UPDATE:
          for (KeyValue<FieldPath, ChangeNode> entry : changeRecord) {
            String fieldPath = entry.getKey().asPathString();
            ChangeNode node = entry.getValue();

            Record record = getContext().createRecord(getMessageId((String) attributes.get(HeaderAttributeConstants.TOPIC),
                (String) attributes.get(HeaderAttributeConstants.PARTITION),
                (String) attributes.get(HeaderAttributeConstants.OFFSET)
            ));
            Record.Header recordHeader = record.getHeader();
            recordHeader.setAllAttributes(attributes);
            recordHeader.setAttribute(MAPR_OP_TIMESTAMP, String.valueOf(node.getOpTimestamp()));
            recordHeader.setAttribute(MAPR_SERVER_TIMESTAMP, String.valueOf(node.getServerTimestamp()));
            if(node.getType() == Value.Type.MAP) {
              record.set(
                  Field.create(node.getMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,e -> generateField(e.getValue())))));
            } else {
              Map<String, Field> fields = new HashMap<>();
              fields.put(fieldPath, generateField(node.getValue()));
              record.set(Field.create(fields));
            }

            record.set("/_id", Field.create(changeRecord.getId().getString()));
            if (fieldPath == null || fieldPath.equals("")) {
              recordHeader.setAttribute(OperationType.SDC_OPERATION_TYPE,
                  String.valueOf(MaprDBCDCOperationType.INSERT.code)
              );
            } else {
              recordHeader.setAttribute(OperationType.SDC_OPERATION_TYPE,
                  String.valueOf(MaprDBCDCOperationType.UPDATE.code)
              );
              recordHeader.setAttribute(MAPR_FIELD_PATH, fieldPath);
            }

            batchMaker.addRecord(record);
          }
          break;
        case RECORD_DELETE:
          Record record = getContext().createRecord(getMessageId((String) attributes.get(HeaderAttributeConstants.TOPIC),
              (String) attributes.get(HeaderAttributeConstants.PARTITION),
              (String) attributes.get(HeaderAttributeConstants.OFFSET)
          ));
          Record.Header recordHeader = record.getHeader();
          recordHeader.setAllAttributes(attributes);

          HashMap<String, Field> root = new HashMap<>();
          record.set(Field.create(root));
          record.set("/_id", Field.create(changeRecord.getId().getString()));

          recordHeader.setAttribute(OperationType.SDC_OPERATION_TYPE,
              String.valueOf(MaprDBCDCOperationType.DELETE.code)
          );

          batchMaker.addRecord(record);
          break;
        default:
      }
    }

    private String getMessageId(String topic, String partition, String offset) {
      return topic + "::" + partition + "::" + offset;
    }

    private void handleException(MaprDBCDCErrors error, Object... args) throws StageException {
      // all threads should halt when an error is encountered
      shutdown();
      throw new StageException(error, args);
    }
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    executor = Executors.newFixedThreadPool(getNumberOfThreads());

    return issues;
  }

  @Override
  public int getNumberOfThreads() {
    return conf.numberOfThreads;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
    shutdownCalled.set(false);
    batchSize = Math.min(maxBatchSize, conf.maxBatchSize);
    int numThreads = getNumberOfThreads();
    List<String> topicList = new ArrayList<>(conf.topicTableList.keySet());
    List<Future<Long>> futures = new ArrayList<>(numThreads);
    CountDownLatch startProcessingGate = new CountDownLatch(numThreads);

    // Run all the threads
    for (int i = 0; i < numThreads; i++) {
      try {
        futures.add(executor.submit(new MapRDBCDCCallable(i,
            topicList,
            consumerFactory.create(getKafkaProperties()),
            startProcessingGate
        )));
      } catch (Exception e) {
        LOG.error("{}", e);
      }
      startProcessingGate.countDown();
    }

    // Wait for proper execution completion
    long totalMessagesProcessed = 0;
    for (Future<Long> future : futures) {
      try {
        totalMessagesProcessed += future.get();
      } catch (InterruptedException e) {
        // all threads should stop if the main thread is interrupted
        shutdown();
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.info("MapRDBCDC kafka thread halted unexpectedly: {}", future, e.getCause().getMessage());
        shutdown();
        throw (StageException) e.getCause();
      }
    }

    LOG.info("Total messages consumed by all threads: {}", totalMessagesProcessed);
    executor.shutdown();
  }

  //no trespassing...
  private Properties getKafkaProperties() {
    Properties props = new Properties();
    props.putAll(conf.streamsOptions);

    props.setProperty("group.id", conf.consumerGroup);
    props.setProperty("max.poll.records", String.valueOf(batchSize));
    props.setProperty("enable.auto.commit", "true");
    props.setProperty("auto.commit.interval.ms", "1000");
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.setProperty("value.deserializer", "com.mapr.db.cdc.ChangeDataRecordDeserializer");

    return props;
  }

  public void setKafkaConsumerFactory(MapRDBCDCKafkaConsumerFactory consumerFactory) {
    this.consumerFactory = consumerFactory;
  }

  public void await() throws InterruptedException {
    if (executor != null) {
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  public boolean isRunning() {
    if (executor == null) {
      return false;
    }

    return !executor.isShutdown() && !executor.isTerminated();
  }

  @Override
  public void destroy() {
    executor.shutdownNow();
    super.destroy();
  }

  private void shutdown() {
    if (!shutdownCalled.getAndSet(true)) {
      executor.shutdownNow();
    }
  }

  private Field generateField(Value value) {
    switch (value.getType()) {
      case INT:
        return Field.create(value.getInt());
      case LONG:
        return Field.create(value.getLong());
      case SHORT:
        return Field.create(value.getShort());
      case BOOLEAN:
        return Field.create(value.getBoolean());
      case DECIMAL:
        return Field.create(value.getDecimal());
      case BYTE:
        return Field.create(value.getByte());
      case DATE:
        return Field.createDate(value.getDate().toDate());
      case FLOAT:
        return Field.create(value.getFloat());
      case DOUBLE:
        return Field.create(value.getDouble());
      case STRING:
        return Field.create(value.getString());
      case MAP:
        return Field.create(value.getMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, this::generateField)));
      case TIME:
        return Field.createTime(value.getTime().toDate());
      case ARRAY:
        return Field.create(value.getList().stream().map(this::generateField).collect(Collectors.toList()));
      case BINARY:
        return Field.create(value.getBinary().array());
      case TIMESTAMP:
        return Field.createDatetime(value.getTimestamp().toDate());
      case INTERVAL:
        return Field.create(value.getInterval().getTimeInMillis());
      default:
        throw new IllegalArgumentException("Unsupported type " + value.getType().toString());
    }
  }

  @SuppressWarnings("unchecked")
  private Field generateField(Object value) {
    if(value instanceof Integer) {
      return Field.create((Integer) value);
    } else if(value instanceof List) {
      return Field.create(((List<Object>) value).stream().map(this::generateField).collect(Collectors.toList()));
    } else if(value instanceof byte[]) {
      return Field.create((byte[]) value);
    } else if(value instanceof ByteBuffer) {
      return Field.create(((ByteBuffer) value).array());
    } else if(value instanceof Long) {
      return Field.create((Long) value);
    } else if(value instanceof Short) {
      return Field.create((Short) value);
    } else if(value instanceof Boolean) {
      return Field.create((Boolean) value);
    } else if(value instanceof BigDecimal) {
      return Field.create((BigDecimal) value);
    } else if(value instanceof Byte) {
      return Field.create((Byte) value);
    } else if(value instanceof Float) {
      return Field.create((Float) value);
    } else if(value instanceof Double) {
      return Field.create((Double) value);
    } else if(value instanceof String) {
      return Field.create((String) value);
    } else if(value instanceof OTime) {
      return Field.createTime(((OTime) value).toDate());
    } else if(value instanceof ODate) {
      return Field.createDate(((ODate) value).toDate());
    } else if(value instanceof OTimestamp) {
      return Field.createDatetime(((OTimestamp)value).toDate());
    } else if(value instanceof OInterval) {
      return Field.create(((OInterval)value).getTimeInMillis());
    } else if(value instanceof Map.Entry) {
      return generateField(((Map.Entry) value).getValue());
    } else if(value instanceof Map) {
      return Field.create(((Map<String, Object>) value).entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, this::generateField)));
    } else {
      throw new IllegalArgumentException("Unsupported type " + value.getClass().toString());
    }
  }
}
