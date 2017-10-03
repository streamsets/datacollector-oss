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

import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import com.streamsets.pipeline.lib.redis.DataType;
import org.apache.commons.lang3.tuple.Pair;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisStore extends CacheLoader<Pair<String, DataType>, LookupValue> {
  private final RedisLookupConfig conf;
  private final JedisPool pool;

  public RedisStore(RedisLookupConfig conf) {
    this.conf = conf;

    final JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setBlockWhenExhausted(true);

    pool = new JedisPool(poolConfig, URI.create(conf.uri), conf.connectionTimeout * 1000); // connectionTimeout value is in seconds
  }

  @Override
  public LookupValue load(Pair<String, DataType> key) throws Exception {
    return get(key);
  }

  @Override
  public Map<Pair<String, DataType>, LookupValue> loadAll(Iterable<? extends Pair<String, DataType>> keys) throws Exception {
    ArrayList<Pair<String, DataType>> keyList = Lists.newArrayList(keys);
    List<LookupValue> values = get(keyList);

    Iterator<? extends Pair<String, DataType>> keyIterator = keyList.iterator();
    Iterator<LookupValue> valueIterator = values.iterator();

    Map<Pair<String, DataType>, LookupValue> result = new HashMap<>(keyList.size());
    while (keyIterator.hasNext() && valueIterator.hasNext()) {
      result.put(keyIterator.next(), valueIterator.next());
    }
    return result;
  }

  public LookupValue get(Pair<String, DataType> pair) {
    Jedis jedis = pool.getResource();
    LookupValue values;

    // check the type
    DataType type = pair.getRight();
    String key = pair.getLeft();
    switch (type) {
      case STRING:
        values = new LookupValue(jedis.get(key), type);
        break;
      case LIST:
        values = new LookupValue(jedis.lrange(key, 0, jedis.llen(key)), type);
        break;
      case HASH:
        values = new LookupValue(jedis.hgetAll(key), type);
        break;
      case SET:
        values = new LookupValue(jedis.smembers(key), type);
        break;
      default:
        values = null;
    }
    jedis.close();

    return values;
  }

  public List<LookupValue> get(List<Pair<String, DataType>> keys) {
    List<LookupValue> result = new ArrayList<>();
    for(Pair<String, DataType> key : keys) {
      result.add(get(key));
    }
    return result;
  }

  public void put(String key, String value) {
    // Persist any new keys to Redis.
    Jedis jedis = pool.getResource();
    jedis.set(key, value);
  }

  @SuppressWarnings("unchecked")
  public void put(Pair<String, DataType> key, LookupValue value) {
    // Persist any new keys to Redis.
    Jedis jedis = pool.getResource();
    switch(key.getRight()) {
      case STRING:
        jedis.set(key.getLeft(), (String)value.getValue());
        break;
      case LIST:
        for(String element : (List<String>)value.getValue())
          jedis.lpush(key.getLeft(), element);
        break;
      case HASH:
        Map<String, String> map = (Map<String, String>)value.getValue();
        jedis.hmset(key.getLeft(), map);
        break;
      case SET:
        for(String element : (Set<String>)value.getValue())
          jedis.lpush(key.getLeft(), element);
        break;
      default:
        throw new IllegalStateException(Errors.REDIS_LOOKUP_04.getMessage());
    }
    jedis.close();
  }

  public void putAll(Map<String, String> entries) {
    Jedis jedis = pool.getResource();
    jedis.mset(kvToList(entries));
    jedis.close();
  }

  public void close() throws Exception {
    pool.close();
  }

  private static String[] kvToList(Map<String, String> kv) {
    String[] array = new String[kv.size() * 2];

    int i = 0;
    for (Map.Entry<String, String> entry : kv.entrySet()) {
      array[2 * i] = entry.getKey();
      array[(2 * i) + 1] = entry.getValue();
      ++i;
    }

    return array;
  }
}
