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
package com.streamsets.pipeline.stage.processor.kv.redis;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.redis.DataType;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.processor.kv.LookupMode;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.stage.processor.kv.LookupMode.BATCH;
import static com.streamsets.pipeline.stage.processor.kv.LookupMode.RECORD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public class RedisLookupIT {
  private static final int REDIS_PORT = 6379;
  private Processor.Context context;

  @Parameterized.Parameters
  public static Collection<LookupMode> modes() {
    return ImmutableList.of(RECORD, BATCH);
  }

  @Parameterized.Parameter
  public LookupMode mode;

  @ClassRule
  public static GenericContainer redis = new GenericContainer("redis:3.0.7").withExposedPorts(REDIS_PORT);

  @BeforeClass
  public static void setUpClass() {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    JedisPool pool = new JedisPool(poolConfig, redis.getContainerIpAddress(), redis.getMappedPort(REDIS_PORT));
    Jedis jedis = pool.getResource();

    //setup string
    jedis.mset(
        "key1", "value1",
        "key2", "value2",
        "key3", "value3"
    );

    //setup list
    for(int i = 1; i < 4; i++) {
      jedis.lpush("lkey"+ i, "lvalue"+i+"_1", "lvalue"+i+"_2", "lvalue"+i+"_3");
    }

    //setup hash
    Map<String, String> mset = new HashMap<String, String>();
    mset.put("hfield1", "hvalue1");
    mset.put("hfield2", "hvalue2");
    mset.put("hfield3", "hvalue3");
    jedis.hmset("hkey1", mset);

    //setup set
    for(int i = 1; i < 4; i++) {
      jedis.sadd("skey"+ i, "svalue"+i+"_1", "svalue"+i+"_2", "svalue"+i+"_3");
    }

    jedis.close();
    pool.close();
  }

  @Test
  public void testEmptyBatch() throws Exception {
    Processor processor = new RedisLookupProcessor(getDefaultConfig());

    List<Record> records = new ArrayList<>();

    ProcessorRunner runner = new ProcessorRunner.Builder(RedisLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    runner.runProcess(records);

    assertTrue(runner.getErrorRecords().isEmpty());
  }

  @Test
  public void testInvalidURI() throws Exception {
    RedisLookupConfig conf = getDefaultConfig();
    conf.uri = "123:abc";
    Processor processor = new RedisLookupProcessor(conf);

    ProcessorRunner runner = new ProcessorRunner.Builder(RedisLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertFalse(issues.isEmpty());
  }

  @Test
  public void testUnreachableRedis() throws Exception {
    RedisLookupConfig conf = getDefaultConfig();
    conf.uri = "redis://invalid:6379";
    Processor processor = new RedisLookupProcessor(conf);

    ProcessorRunner runner = new ProcessorRunner.Builder(RedisLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertFalse(issues.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetSingleKey() throws Exception {
    final String expected = "value1";
    RedisLookupConfig conf = new RedisLookupConfig();
    conf.cache.enabled = false;
    conf.uri = "redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(REDIS_PORT);
    RedisStore redisStore = new RedisStore(conf);
    LookupValue value = redisStore.get(Pair.of("key1", DataType.STRING));
    redisStore.close();
    assertTrue(Optional.fromNullable((String)value.getValue()).isPresent());
    assertEquals(expected, Optional.fromNullable((String)value.getValue()).get());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetEmptyKey() throws Exception {
    RedisLookupConfig conf = new RedisLookupConfig();
    conf.cache.enabled = false;
    conf.uri = "redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(REDIS_PORT);
    RedisStore redisStore = new RedisStore(conf);
    LookupValue value = redisStore.get(Pair.of("", DataType.STRING));
    redisStore.close();
    assertTrue(!Optional.fromNullable((String)value.getValue()).isPresent());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetSingleKeyOfList() throws Exception {
    final List<String> expected = ImmutableList.of("lvalue1_3", "lvalue1_2", "lvalue1_1");
    RedisLookupConfig conf = new RedisLookupConfig();
    conf.cache.enabled = false;
    conf.uri = "redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(REDIS_PORT);
    RedisStore redisStore = new RedisStore(conf);
    LookupValue value = redisStore.get(Pair.of("lkey1", DataType.LIST));
    redisStore.close();
    assertTrue(value.getValue() != null && ((ArrayList<String>)value.getValue()).size() > 0);
    assertEquals(expected, value.getValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetEmptyKeyOfList() throws Exception {
    final List<String> expected = ImmutableList.of("lvalue3", "lvalue2", "lvalue1");
    RedisLookupConfig conf = new RedisLookupConfig();
    conf.cache.enabled = false;
    conf.uri = "redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(REDIS_PORT);
    RedisStore redisStore = new RedisStore(conf);
    LookupValue value = redisStore.get(Pair.of("", DataType.LIST));
    redisStore.close();
    assertTrue(value.getValue() != null && ((ArrayList<String>)value.getValue()).size() == 0);
  }


  @Test
  public void testGetSingleKeyOfWrongType() throws Exception {
    RedisLookupConfig conf = getDefaultConfig();
    conf.cache.enabled = false;
    conf.uri = "redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(REDIS_PORT);
    conf.mode = LookupMode.RECORD;

    Processor processor = new RedisLookupProcessor(conf);

    ProcessorRunner runner = new ProcessorRunner.Builder(RedisLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    List<Record> inputRecords = new ArrayList<>();
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("keyField", Field.create("lkey1"));
    record.set(Field.create(fields));
    inputRecords.add(record);

    runner.runProcess(inputRecords);

    assertTrue(!runner.getErrorRecords().isEmpty());
  }

  @Test
  public void testGetMultipleKeys() throws Exception {
    List<Pair<String, DataType>> keys = ImmutableList.of(
        Pair.of("key1", DataType.STRING),
        Pair.of("lkey1", DataType.LIST),
        Pair.of("hkey1", DataType.HASH),
        Pair.of("skey1", DataType.SET),
        Pair.of("key2", DataType.STRING),
        Pair.of("key3", DataType.STRING),
        Pair.of("key4", DataType.STRING)
    );

    List<LookupValue> expected = ImmutableList.of(
        new LookupValue("value1", DataType.STRING),
        new LookupValue(ImmutableList.of("lvalue1_3", "lvalue1_2", "lvalue1_1"), DataType.LIST),
        new LookupValue(ImmutableMap.of("hfield1", "hvalue1", "hfield3", "hvalue3", "hfield2", "hvalue2"), DataType.HASH),
        new LookupValue(ImmutableSet.of("svalue1_3", "svalue1_2", "svalue1_1"), DataType.SET),
        new LookupValue("value2", DataType.STRING),
        new LookupValue("value3", DataType.STRING),
        new LookupValue(null, DataType.STRING)
    );
    RedisLookupConfig conf = new RedisLookupConfig();
    conf.cache.enabled = false;
    conf.uri = "redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(REDIS_PORT);
    conf.mode = LookupMode.BATCH;

    RedisStore redisStore = new RedisStore(conf);
    List<LookupValue> values = redisStore.get(keys);
    redisStore.close();
    assertEquals(keys.size(), values.size());
    assertArrayEquals(expected.toArray(), values.toArray());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPutSingleKey() throws Exception{
    final String expected = "putSingleValue";
    RedisLookupConfig conf = new RedisLookupConfig();
    conf.cache.enabled = false;
    conf.uri = "redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(REDIS_PORT);
    RedisStore redisStore = new RedisStore(conf);
    redisStore.put("putSingleKey", expected);
    LookupValue value = redisStore.get(Pair.of("putSingleKey", DataType.STRING));
    redisStore.close();
    assertTrue(Optional.fromNullable((String)value.getValue()).isPresent());
    assertEquals(expected, Optional.fromNullable((String)value.getValue()).get());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPutMultipleKeys() throws Exception {
    List<String> keys = ImmutableList.of("key1", "key2", "key3", "key4");
    Map<String, String> toAdd = ImmutableMap.of(
        "putMultipleKey1", "value1",
        "putMultipleKey2", "value2",
        "putMultipleKey3", "value3"
    );
    List<Optional<?>> expected = ImmutableList.of(
        Optional.of("value1"), Optional.of("value2"), Optional.of("value3"), Optional.absent()
    );
    RedisLookupConfig conf = new RedisLookupConfig();
    conf.cache.enabled = false;
    conf.uri = "redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(REDIS_PORT);
    RedisStore redisStore = new RedisStore(conf);
    redisStore.putAll(toAdd);
    List<Pair<String, DataType>> lookupKeys = new ArrayList<>();
    for(String key : keys) {
      lookupKeys.add(Pair.of(key, DataType.STRING));
    }
    List<LookupValue> values = redisStore.get(lookupKeys);
    redisStore.close();
    assertEquals(keys.size(), values.size());
    List<Optional<?>> actual = new ArrayList<>();
    for(LookupValue value : values) {
      actual.add(Optional.fromNullable((String)value.getValue()));
    }
    assertArrayEquals(expected.toArray(), actual.toArray());
  }

  private List<Record> getRecords(int i) {
    List<Record> records = new ArrayList<>();
    for (int j = 1; j <= i; j++) {
      Record record = RecordCreator.create();
      Map<String, Field> fields = new HashMap<>();
      fields.put("keyField", Field.create("key" + j));
      record.set(Field.create(fields));
      records.add(record);
    }
    return records;
  }

  private List<Record> getExpectedRecords(int i) {
    List<Record> records = new ArrayList<>();
    for (int j = 1; j <= i; j++) {
      Record record = RecordCreator.create();
      Map<String, Field> fields = new HashMap<>();
      fields.put("keyField", Field.create("key" + j));
      // Don't set a value for the last one as it's absent.
      if (j != i) {
        fields.put("output", Field.create("value" + j));
      }
      record.set(Field.create(fields));
      records.add(record);
    }
    return records;
  }

  private RedisLookupConfig getDefaultConfig() {
    RedisLookupConfig conf = new RedisLookupConfig();
    conf.uri = "redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(REDIS_PORT);
    RedisLookupParameterConfig parameters = new RedisLookupParameterConfig();
    parameters.keyExpr = "${record:value('/keyField')}";
    parameters.outputFieldPath = "/output";
    parameters.dataType = DataType.STRING;
    conf.lookups.add(parameters);
    conf.mode = mode;
    return conf;
  }
}
