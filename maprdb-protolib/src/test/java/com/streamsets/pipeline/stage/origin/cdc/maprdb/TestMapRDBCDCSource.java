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

import com.google.common.base.Throwables;
import com.mapr.db.cdc.ChangeDataKeyValue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.CircularIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.ojai.DocumentReader;
import org.ojai.FieldPath;
import org.ojai.FieldSegment;
import org.ojai.KeyValue;
import org.ojai.Value;
import org.ojai.json.JsonOptions;
import org.ojai.store.cdc.ChangeDataRecord;
import org.ojai.store.cdc.ChangeDataRecordType;
import org.ojai.store.cdc.ChangeNode;
import org.ojai.types.ODate;
import org.ojai.types.OInterval;
import org.ojai.types.OTime;
import org.ojai.types.OTimestamp;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMapRDBCDCSource {
  private List<Object> objectList = Arrays.asList(
      123,
      123L,
      (short)123,
      true,
      new BigDecimal(123),
      (byte)123,
      (float)12.3,
      (double)12.3,
      "123",
      "123".getBytes(),
      ByteBuffer.wrap("123".getBytes()),
      new OTimestamp(123),
      new OTime(123),
      new ODate(123),
      new OInterval(123),
      new HashMap<String, Object>(),
      new HashMap<String, Object>(Collections.singletonMap("asd", "hello")),
      new HashMap<String, Object>(Collections.singletonMap("asd", Collections.singletonMap("jkl", "world"))),
      Collections.singletonMap("asd", Arrays.asList("a", "b", "c")),
      new ArrayList<Object>()
  );
  private CircularIterator<Object> objectRing = new CircularIterator<>(objectList);

  private CircularIterator<Value> valueRing = new CircularIterator<>(Arrays.asList(
      new SetValue(123, Value.Type.INT),
      new SetValue(123L, Value.Type.LONG),
      new SetValue((short)123, Value.Type.SHORT),
      new SetValue(true, Value.Type.BOOLEAN),
      new SetValue(new BigDecimal(123), Value.Type.DECIMAL),
      new SetValue((byte)123, Value.Type.BYTE),
      new SetValue((float)12.3, Value.Type.FLOAT),
      new SetValue((double)12.3, Value.Type.DOUBLE),
      new SetValue("123", Value.Type.STRING),
      new SetValue("123".getBytes(), Value.Type.BINARY),
      new SetValue(new OTimestamp(123), Value.Type.TIMESTAMP),
      new SetValue(new OTime(123), Value.Type.TIME),
      new SetValue(new ODate(123), Value.Type.DATE),
      new SetValue(new OInterval(123), Value.Type.INTERVAL),
      new SetValue(new HashMap<String, Object>(), Value.Type.MAP),
      new SetValue(objectList, Value.Type.ARRAY)
  ));

  @Before
  public void setUp() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);
  }

  @SuppressWarnings("unchecked")
  private MapRDBCDCSource createSource(MapRDBCDCBeanConfig conf, Iterator<KafkaConsumer> iter) {
    return new MapRDBCDCSource(conf, new MapRDBCDCKafkaConsumerFactory() {
      @Override
      public KafkaConsumer<byte[], ChangeDataRecord> create(Properties props) {
        return iter.next();
      }
    });
  }

  private MapRDBCDCBeanConfig getConfig() {
    MapRDBCDCBeanConfig conf = new MapRDBCDCBeanConfig();
    conf.consumerGroup = "sdc";
    conf.maxBatchSize = 9;
    conf.batchWaitTime = 5000;
//    conf.produceSingleRecordPerMessage = false;
    conf.streamsOptions = new HashMap<>();

    return conf;
  }

  @Test
  public void testProduceStringRecords() throws StageException, InterruptedException {
    MapRDBCDCBeanConfig conf = getConfig();
    conf.topicTableList = Collections.singletonMap("topic", "table");
    conf.numberOfThreads = 1;

    ConsumerRecords<byte[], ChangeDataRecord> consumerRecords = generateConsumerRecords(5, 1, "topic", 0, ChangeDataRecordType.RECORD_INSERT);
    ConsumerRecords<byte[], ChangeDataRecord> emptyRecords = generateConsumerRecords(0, 1, "topic", 0, ChangeDataRecordType.RECORD_INSERT);

    KafkaConsumer mockConsumer = Mockito.mock(KafkaConsumer.class);
    List<KafkaConsumer> consumerList = Collections.singletonList(mockConsumer);
    Mockito.when(mockConsumer.poll(conf.batchWaitTime)).thenReturn(consumerRecords).thenReturn(emptyRecords);

    MapRDBCDCSource source = createSource(conf, consumerList.iterator());
    PushSourceRunner sourceRunner = new PushSourceRunner.Builder(MapRDBCDCDSource.class, source)
        .addOutputLane("lane")
        .build();
    sourceRunner.runInit();

    MultiKafkaPushSourceTestCallback callback = new MultiKafkaPushSourceTestCallback(sourceRunner, 1);
    try {
      sourceRunner.runProduce(new HashMap<>(), 5, callback);
      int records = callback.waitForAllBatches();

      source.await();
      Assert.assertEquals(5, records);
      Assert.assertFalse(source.isRunning());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
      throw e;
    } finally {
      sourceRunner.runDestroy();
    }
  }

  @Test
  public void testDeleteRecords() throws StageException, InterruptedException {
    MapRDBCDCBeanConfig conf = getConfig();
    conf.topicTableList = Collections.singletonMap("topic", "table");
    conf.numberOfThreads = 1;

    ConsumerRecords<byte[], ChangeDataRecord> consumerRecords = generateConsumerRecords(5, 1, "topic", 0, ChangeDataRecordType.RECORD_DELETE);
    ConsumerRecords<byte[], ChangeDataRecord> emptyRecords = generateConsumerRecords(0, 1, "topic", 0, ChangeDataRecordType.RECORD_DELETE);

    KafkaConsumer mockConsumer = Mockito.mock(KafkaConsumer.class);
    List<KafkaConsumer> consumerList = Collections.singletonList(mockConsumer);
    Mockito.when(mockConsumer.poll(conf.batchWaitTime)).thenReturn(consumerRecords).thenReturn(emptyRecords);

    MapRDBCDCSource source = createSource(conf, consumerList.iterator());
    PushSourceRunner sourceRunner = new PushSourceRunner.Builder(MapRDBCDCDSource.class, source)
        .addOutputLane("lane")
        .build();
    sourceRunner.runInit();

    MultiKafkaPushSourceTestCallback callback = new MultiKafkaPushSourceTestCallback(sourceRunner, 1);
    try {
      sourceRunner.runProduce(new HashMap<>(), 5, callback);
      int records = callback.waitForAllBatches();

      source.await();
      Assert.assertEquals(5, records);
      Assert.assertFalse(source.isRunning());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
      throw e;
    } finally {
      sourceRunner.runDestroy();
    }
  }

  @Test
  public void testMultiplePartitions() throws StageException, InterruptedException {
    MapRDBCDCBeanConfig conf = getConfig();
    conf.topicTableList = Collections.singletonMap("topic", "table");
    conf.numberOfThreads = 1;

    ConsumerRecords<byte[], ChangeDataRecord> consumerRecords1 = generateConsumerRecords(5, 1, "topic", 0, ChangeDataRecordType.RECORD_INSERT);
    ConsumerRecords<byte[], ChangeDataRecord> consumerRecords2 = generateConsumerRecords(5, 1, "topic", 1, ChangeDataRecordType.RECORD_INSERT);
    ConsumerRecords<byte[], ChangeDataRecord> emptyRecords = generateConsumerRecords(0, 1, "topic", 0, ChangeDataRecordType.RECORD_INSERT);

    KafkaConsumer mockConsumer = Mockito.mock(KafkaConsumer.class);
    List<KafkaConsumer> consumerList = Collections.singletonList(mockConsumer);
    Mockito
        .when(mockConsumer.poll(conf.batchWaitTime))
        .thenReturn(consumerRecords1)
        .thenReturn(consumerRecords2)
        .thenReturn(emptyRecords);

    MapRDBCDCSource source = createSource(conf, consumerList.iterator());
    PushSourceRunner sourceRunner = new PushSourceRunner.Builder(MapRDBCDCDSource.class, source)
        .addOutputLane("lane")
        .build();
    sourceRunner.runInit();

    MultiKafkaPushSourceTestCallback callback = new MultiKafkaPushSourceTestCallback(sourceRunner, 2);
    try {
      sourceRunner.runProduce(new HashMap<>(), 5, callback);
      int records = callback.waitForAllBatches();

      source.await();
      Assert.assertEquals(10, records);
      Assert.assertFalse(source.isRunning());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
      throw e;
    } finally {
      sourceRunner.runDestroy();
    }
  }

  @Test
  public void testMultipleTopics() throws StageException, InterruptedException, ExecutionException {
    MapRDBCDCBeanConfig conf = getConfig();
    conf.numberOfThreads = 100;
    int numTopics = 20;
    long totalMessages = 0;
    Random rand = new Random();

    Map<String, String> topicTableList = new HashMap<>(numTopics);
//    List<String> topicNames = new ArrayList<>(numTopics);
    List<KafkaConsumer> consumerList = new ArrayList<>(numTopics);

    for(int i=0; i<numTopics; i++) {
      topicTableList.put("topic-" + i, "table-" + i);
    }

    for(int i=0; i<conf.numberOfThreads; i++) {
      int numMessages = rand.nextInt(40)+1;
      totalMessages += numMessages;
      ConsumerRecords<byte[], ChangeDataRecord> consumerRecords =
          generateConsumerRecords(numMessages, 1, "topic"+rand.nextInt(numTopics), 0, ChangeDataRecordType.RECORD_UPDATE);
      ConsumerRecords<byte[], ChangeDataRecord> emptyRecords =
          generateConsumerRecords(1, 0, "topic"+rand.nextInt(numTopics), 0, ChangeDataRecordType.RECORD_UPDATE);

      KafkaConsumer mockConsumer = Mockito.mock(KafkaConsumer.class);
      consumerList.add(mockConsumer);

      Mockito.when(mockConsumer.poll(conf.batchWaitTime)).thenReturn(consumerRecords).thenReturn(emptyRecords);
    }

    conf.topicTableList = topicTableList;

    MapRDBCDCSource source = createSource(conf, consumerList.iterator());
    PushSourceRunner sourceRunner = new PushSourceRunner.Builder(MapRDBCDCDSource.class, source)
        .addOutputLane("lane")
        .build();
    sourceRunner.runInit();

    MultiKafkaPushSourceTestCallback callback = new MultiKafkaPushSourceTestCallback(sourceRunner, conf.numberOfThreads);

    try {
      sourceRunner.runProduce(new HashMap<>(), 5, callback);
      int records = callback.waitForAllBatches();

      source.await();
      Assert.assertEquals(totalMessages, records);
      Assert.assertFalse(source.isRunning());
    } finally {
      sourceRunner.runDestroy();
    }
  }

  @Test(expected = ExecutionException.class)
  public void testPollFail() throws StageException, InterruptedException, ExecutionException {
    MapRDBCDCBeanConfig conf = getConfig();
    conf.topicTableList = Collections.singletonMap("topic", "table");
    conf.numberOfThreads = 1;

    KafkaConsumer mockConsumer = Mockito.mock(KafkaConsumer.class);
    List<KafkaConsumer> consumerList = Collections.singletonList(mockConsumer);
    Mockito
        .when(mockConsumer.poll(conf.batchWaitTime))
        .thenThrow(new IllegalStateException());

    MapRDBCDCSource source = createSource(conf, consumerList.iterator());
    PushSourceRunner sourceRunner = new PushSourceRunner.Builder(MapRDBCDCDSource.class, source)
        .addOutputLane("lane")
        .build();
    sourceRunner.runInit();

    MultiKafkaPushSourceTestCallback callback = new MultiKafkaPushSourceTestCallback(sourceRunner, conf.numberOfThreads);
    sourceRunner.runProduce(new HashMap<>(), 5, callback);

    //IllegalStateException in source's threads cause a StageException
    //StageException is caught by source's executor service and packaged into ExecutionException
    //ExecutionException is unpacked by source and thrown as StageException
    //sourceRunner sees this and throws as RuntimeException
    //sourceRunner's executor service then packages as ExecutionException
    try {
      sourceRunner.waitOnProduce();
    } catch (ExecutionException e) {
      Throwable except = e.getCause().getCause();
      Assert.assertEquals(StageException.class, except.getClass());
      Assert.assertEquals(MaprDBCDCErrors.MAPRDB_03, ((StageException) except).getErrorCode());
      throw e;
    } finally {
      sourceRunner.runDestroy();
    }
    Assert.fail();
  }

  @Test(expected = InterruptedException.class)
  public void testInterrupt() throws StageException, InterruptedException, ExecutionException {
    MapRDBCDCBeanConfig conf = getConfig();
    conf.numberOfThreads = 10;

    int numTopics = conf.numberOfThreads;
    Map<String, String> topicTableNames = new HashMap<>(numTopics);
    List<KafkaConsumer> consumerList = new ArrayList<>(numTopics);

    for(int i=0; i<numTopics; i++) {
      String topic =  "topic-" + i;
      topicTableNames.put(topic, "table" + i);
      ConsumerRecords<byte[], ChangeDataRecord> consumerRecords = generateConsumerRecords(5, 1, topic, 0, ChangeDataRecordType.RECORD_DELETE);
      ConsumerRecords<byte[], ChangeDataRecord> emptyRecords = generateConsumerRecords(0, 1, topic, 0, ChangeDataRecordType.RECORD_DELETE);

      KafkaConsumer mockConsumer = Mockito.mock(KafkaConsumer.class);
      consumerList.add(mockConsumer);

      Mockito.when(mockConsumer.poll(conf.batchWaitTime)).thenReturn(consumerRecords).thenReturn(emptyRecords);
    }

    conf.topicTableList = topicTableNames;

    MapRDBCDCSource source = createSource(conf, consumerList.iterator());
    PushSourceRunner sourceRunner = new PushSourceRunner.Builder(MapRDBCDCDSource.class, source)
        .addOutputLane("lane")
        .build();
    sourceRunner.runInit();

    MultiKafkaPushSourceTestCallback callback = new MultiKafkaPushSourceTestCallback(sourceRunner, conf.numberOfThreads);
    sourceRunner.runProduce(new HashMap<>(), 5, callback);

    //start the interrupt cascade
    Thread.currentThread().interrupt();

    try {
      sourceRunner.waitOnProduce();
    } finally {
      sourceRunner.runDestroy();
    }
    Assert.fail();
  }

  private ConsumerRecords<byte[], ChangeDataRecord> generateConsumerRecords(int recordCount, int nodeCount, String topic, int partition, ChangeDataRecordType type) {
    List<ConsumerRecord<byte[], ChangeDataRecord>> consumerRecordsList = new ArrayList<>();
    for(int i=0; i<recordCount; i++) {
      long now = Instant.now().toEpochMilli();

      Value idVal = Mockito.mock(Value.class);
      Mockito.when(idVal.getString()).thenReturn(String.valueOf(i));
      Mockito.when(idVal.getType()).thenReturn(Value.Type.STRING);

      ChangeDataRecord cdr = Mockito.mock(ChangeDataRecord.class);
      Mockito.when(cdr.getId()).thenReturn(idVal);
      Mockito.when(cdr.getOpTimestamp()).thenReturn(now);
      Mockito.when(cdr.getServerTimestamp()).thenReturn(now);
      Mockito.when(cdr.getType()).thenReturn(type);

      if(type != ChangeDataRecordType.RECORD_DELETE) {
        List<KeyValue<FieldPath, ChangeNode>> nodeList = new ArrayList<>();
        ChangeNode node = Mockito.mock(ChangeNode.class);

        for (int j = 0; j < nodeCount; j++) {
          String fieldPath = "";
          if(type == ChangeDataRecordType.RECORD_UPDATE) {
            fieldPath = String.valueOf(i);
            Mockito.when(node.getType()).thenReturn(valueRing.peek().getType());
            Mockito.when(node.getValue()).thenReturn(valueRing.next());
          } else {
            Mockito.when(node.getType()).thenReturn(Value.Type.MAP);
            Mockito.when(node.getMap()).thenReturn(Collections.singletonMap("datakey" + i, objectRing.next()));
          }

          Mockito.when(node.getOpTimestamp()).thenReturn(now);
          Mockito.when(node.getServerTimestamp()).thenReturn(now);

          nodeList.add(new ChangeDataKeyValue(new FieldPath(new FieldSegment.NameSegment(fieldPath,
              null,
              false
          )), node));
        }

        Mockito.when(cdr.iterator()).thenReturn(nodeList.iterator());
      }

      consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, ("key" + i).getBytes(), cdr));
    }

    Map<TopicPartition, List<ConsumerRecord<byte[], ChangeDataRecord>>> recordsMap = new HashMap<>();
    recordsMap.put(new TopicPartition(topic, partition), consumerRecordsList);
    return new ConsumerRecords<>(recordsMap);
  }

  static class MultiKafkaPushSourceTestCallback implements PushSourceRunner.Callback {
    private final PushSourceRunner pushSourceRunner;
    private final AtomicInteger batchesProduced;
    private final AtomicInteger recordsProcessed;
    private final int numberOfBatches;

    MultiKafkaPushSourceTestCallback(PushSourceRunner pushSourceRunner, int numberOfBatches) {
      this.pushSourceRunner = pushSourceRunner;
      this.numberOfBatches = numberOfBatches;
      this.batchesProduced = new AtomicInteger(0);
      this.recordsProcessed = new AtomicInteger(0);
    }

    synchronized int waitForAllBatches() {
      try {
        pushSourceRunner.waitOnProduce();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      return recordsProcessed.get();
    }

    @Override
    public void processBatch(StageRunner.Output output) {
      List<Record> records = output.getRecords().get("lane");
      if (!records.isEmpty()) {
        recordsProcessed.addAndGet(records.size());
        if (batchesProduced.incrementAndGet() == numberOfBatches) {
          pushSourceRunner.setStop();
        }
      }
    }
  }

  private class SetValue implements Value {
    private Object obj;
    private Type type;

    public SetValue(Object obj, Type type) {
      this.obj = obj;
      this.type = type;
    }

    @Override
    public String asJsonString() {
      return null;
    }

    @Override
    public String asJsonString(JsonOptions options) {
      return null;
    }

    @Override
    public Type getType() {
      return type;
    }

    @Override
    public byte getByte() {
      return (byte) obj;
    }

    @Override
    public short getShort() {
      return (short) obj;
    }

    @Override
    public int getInt() {
      return (int) obj;
    }

    @Override
    public long getLong() {
      return (long) obj;
    }

    @Override
    public float getFloat() {
      return (float) obj;
    }

    @Override
    public double getDouble() {
      return (double) obj;
    }

    @Override
    public BigDecimal getDecimal() {
      return (BigDecimal) obj;
    }

    @Override
    public boolean getBoolean() {
      return (boolean) obj;
    }

    @Override
    public String getString() {
      return (String) obj;
    }

    @Override
    public OTimestamp getTimestamp() {
      return (OTimestamp) obj;
    }

    @Override
    public long getTimestampAsLong() {
      return ((OTimestamp) obj).toDate().getTime();
    }

    @Override
    public ODate getDate() {
      return (ODate) obj;
    }

    @Override
    public int getDateAsInt() {
      return (int) ((ODate) obj).toDate().getTime();
    }

    @Override
    public OTime getTime() {
      return (OTime) obj;
    }

    @Override
    public int getTimeAsInt() {
      return ((OTime) obj).toTimeInMillis();
    }

    @Override
    public OInterval getInterval() {
      return (OInterval) obj;
    }

    @Override
    public long getIntervalAsLong() {
      return ((OInterval) obj).getTimeInMillis();
    }

    @Override
    public ByteBuffer getBinary() {
      return ByteBuffer.wrap((byte[]) obj);
    }

    @Override
    public Map<String, Object> getMap() {
      return (Map) obj;
    }

    @Override
    public List<Object> getList() {
      return (List) obj;
    }

    @Override
    public Object getObject() {
      return obj;
    }

    @Override
    public DocumentReader asReader() {
      return null;
    }
  }
}
