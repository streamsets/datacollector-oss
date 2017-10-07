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
package com.streamsets.pipeline.stage.destination.redis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.lib.redis.DataType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public class RedisTargetIT {
  private static final int REDIS_PORT = 6379;
  private static final String channel = "pubsub_channel";
  private static final int size = 4;
  private static JedisPool pool;
  private static Jedis jedis;

  @ClassRule
  public static GenericContainer redis = new GenericContainer("redis:3.0.7").withExposedPorts(REDIS_PORT);

  @BeforeClass
  public static void setUpClass() {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    pool = new JedisPool(poolConfig, redis.getContainerIpAddress(), redis.getMappedPort(REDIS_PORT));
    jedis = pool.getResource();
    jedis.flushAll();

    //setup string
    jedis.mset(
        "key1", "oldValue1",
        "key2", "oldValue2",
        "key3", "oldValue3"
    );
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(jedis != null) {
      jedis.close();
    }
    if(pool != null) {
      pool.close();
    }
  }

  @Test
  public void testInvalidURI() throws Exception {
    RedisTargetConfig conf = getDefaultConfig();
    conf.uri = "123:abc";

    Target target = new RedisTarget(conf);
    TargetRunner runner = new TargetRunner.Builder(RedisDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertFalse(issues.isEmpty());
  }

  @Test
  public void testUnreachableRedis() throws Exception {
    RedisTargetConfig conf = getDefaultConfig();
    conf.uri = "redis://invalid:6379";
    Target target = new RedisTarget(conf);
    TargetRunner runner = new TargetRunner.Builder(RedisDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertFalse(issues.isEmpty());
  }

  @Test
  public void testStringInsert() throws Exception {
    Target target = new RedisTarget(getDefaultConfig());

    List<Record> records = getTestRecords();

    TargetRunner runner = new TargetRunner.Builder(RedisDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    runner.runInit();
    runner.runWrite(records.subList(0,size));

    assertTrue(runner.getErrorRecords().isEmpty());

    List<String> actual = jedis.mget("key1", "key2", "key3");
    List<String> expected = ImmutableList.of("value1", "value2", "value3");
    assertEquals(expected, actual);
  }

  @Test
  public void testListInsert() throws Exception {
    RedisTargetConfig config = getDefaultConfig();
    config.redisFieldMapping.get(0).dataType = DataType.LIST;

    Target target = new RedisTarget(config);

    List<Record> records = getTestRecords();

    TargetRunner runner = new TargetRunner.Builder(RedisDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    runner.runInit();
    runner.runWrite(records.subList(size,size * 2));

    assertTrue(runner.getErrorRecords().isEmpty());

    List<String> actual = jedis.lrange("lkey1", 0, 4);
    List<String> expected = ImmutableList.of("lvalue4", "lvalue3", "lvalue2", "lvalue1");
    assertEquals(expected, actual);
  }

  @Test
  public void testSetInsert() throws Exception {
    RedisTargetConfig config = getDefaultConfig();
    config.redisFieldMapping.get(0).dataType = DataType.SET;

    Target target = new RedisTarget(config);

    List<Record> records = getTestRecords();

    TargetRunner runner = new TargetRunner.Builder(RedisDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    runner.runInit();
    runner.runWrite(records.subList(size*2,size * 3));

    assertTrue(runner.getErrorRecords().isEmpty());

    Set<String> actual = jedis.smembers("skey1");
    Set<String> expected = ImmutableSet.of("svalue4", "svalue3", "svalue2", "svalue1");
    assertEquals(actual, expected);
  }

  @Test
  public void testHashInsert() throws Exception {
    RedisTargetConfig config = getDefaultConfig();
    config.redisFieldMapping.get(0).dataType = DataType.HASH;

    Target target = new RedisTarget(config);

    List<Record> records = getTestRecords();

    TargetRunner runner = new TargetRunner.Builder(RedisDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    runner.runInit();
    runner.runWrite(records.subList(size*3,size * 4));

    assertTrue(runner.getErrorRecords().isEmpty());

    Map<String, String> actual = jedis.hgetAll("hkey1");
    Map<String, String> expected = ImmutableMap.of(
        "field1", "hvalue1",
        "field2", "hvalue2",
        "field3", "hvalue3",
        "field4", "hvalue4"
    );
    assertEquals(actual, expected);
  }

  @Test
  public void testInvalidBatchInsert() throws Exception {
    RedisTargetConfig config = getDefaultConfig();
    config.redisFieldMapping.get(0).dataType = DataType.HASH;

    Target target = new RedisTarget(config);

    List<Record> records = new ArrayList<>();

    // record hash type
    for(int i = 1; i <= size; i++) {
      Record record = RecordCreator.create();
      Map<String, Field> fields = new HashMap<>();
      fields.put("key", Field.create("key" + i));

      Map<String, Field> values = new HashMap<>();
      for(int j = 1; j <= size; j++) {
        values.put("field" + j, Field.create("hvalue" + j));
      }

      fields.put("value", Field.create(values));
      record.set(Field.create(fields));
      records.add(record);
    }

    TargetRunner runner = new TargetRunner.Builder(RedisDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    runner.runWrite(records);

    assertFalse(runner.getErrorRecords().isEmpty());
    assertEquals(Errors.REDIS_03.getCode(), runner.getErrorRecords().get(0).getHeader().getErrorCode());
  }

  @Test
  public void testValidPublish() throws Exception {
    // subscriber
    RedisTargetConfig conf = getDefaultConfig();
    conf.mode = ModeType.PUBLISH;
    conf.channel = ImmutableList.of(channel);
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonMode = JsonMode.MULTIPLE_OBJECTS;

    Target target = new RedisTarget(conf);

    List<Record> records = getTestRecords();

    TargetRunner runner = new TargetRunner.Builder(RedisDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    runner.runWrite(records);

    assertTrue(runner.getErrorRecords().isEmpty());
  }

  @Test
  public void testBatchTTL() throws Exception {
    RedisTargetConfig conf = getDefaultConfig();
    for (RedisFieldMappingConfig mappingConfig : conf.redisFieldMapping) {
      mappingConfig.ttl = 3600;
    }

    Target target = new RedisTarget(conf);

    List<Record> records = getTestRecords();

    TargetRunner runner = new TargetRunner.Builder(RedisDTarget.class, target)
        .setOnRecordError(OnRecordError.DISCARD)
        .build();
    runner.runInit();
    runner.runWrite(records.subList(0,4));

    assertTrue(runner.getErrorRecords().isEmpty());

    final String keyExpression = "key";
    for (Record record : records.subList(0,4)) {
      String key = record.get().getValueAsMap().get(keyExpression).getValueAsString();
      assertTrue(jedis.ttl(key) >= Long.valueOf(0));
    }
  }

  private RedisTargetConfig getDefaultConfig() {
    RedisTargetConfig conf = new RedisTargetConfig();
    conf.uri = "redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(REDIS_PORT);

    conf.mode = ModeType.BATCH;

    RedisFieldMappingConfig fieldMapping1 = new RedisFieldMappingConfig();
    fieldMapping1.dataType = DataType.STRING;
    fieldMapping1.keyExpr = "/key";
    fieldMapping1.valExpr = "/value";

    conf.redisFieldMapping = ImmutableList.of(fieldMapping1);

    return conf;
  }

  private List<Record> getTestRecords() {
    List<Record> records = new ArrayList<>();

    // record string type
    for(int i = 1; i <= size; i++) {
      Record record = RecordCreator.create();
      Map<String, Field> fields = new HashMap<>();
      fields.put("key", Field.create("key" + i));
      fields.put("value", Field.create("value" + i));
      record.set(Field.create(fields));
      records.add(record);
    }

    // record list type
    for(int i = 1; i <= size; i++) {
      Record record = RecordCreator.create();
      Map<String, Field> fields = new HashMap<>();
      fields.put("key", Field.create("lkey" + i));

      List<Field> values = new ArrayList<>();
      for(int j = 1; j <= size; j++) {
        values.add(Field.create("lvalue" + j));
      }

      fields.put("value", Field.create(values));
      record.set(Field.create(fields));
      records.add(record);
    }

    // record set type
    for(int i = 1; i <= size; i++) {
      Record record = RecordCreator.create();
      Map<String, Field> fields = new HashMap<>();
      fields.put("key", Field.create("skey" + i));

      List<Field> values = new ArrayList<>();
      for(int j = 1; j <= size; j++) {
        values.add(Field.create("svalue" + j));
      }

      fields.put("value", Field.create(values));
      record.set(Field.create(fields));
      records.add(record);
    }

    // record hash type
    for(int i = 1; i <= size; i++) {
      Record record = RecordCreator.create();
      Map<String, Field> fields = new HashMap<>();
      fields.put("key", Field.create("hkey" + i));

      Map<String, Field> values = new HashMap<>();
      for(int j = 1; j <= size; j++) {
        values.put("field" + j, Field.create("hvalue" + j));
      }

      fields.put("value", Field.create(values));
      record.set(Field.create(fields));
      records.add(record);
    }

    return records;
  }
}
