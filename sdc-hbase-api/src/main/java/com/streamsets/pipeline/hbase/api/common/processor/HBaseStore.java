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
package com.streamsets.pipeline.hbase.api.common.processor;

import com.google.common.base.Optional;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.hbase.api.HBaseProcessor;
import com.streamsets.pipeline.hbase.api.common.Errors;
import com.streamsets.pipeline.hbase.api.common.producer.HBaseColumn;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class HBaseStore extends CacheLoader<Pair<String, HBaseColumn>, Optional<String>> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseStore.class);

  private final HBaseProcessor hBaseProcessor;

  public HBaseStore(HBaseProcessor hBaseProcessor) throws Exception {
    this.hBaseProcessor = hBaseProcessor;
    try {
      hBaseProcessor.createTable();
    } catch (IOException e) {
      LOG.debug("Got exception while reading batch from HBase", e);
      throw new StageException(Errors.HBASE_36, e);
    }
  }

  public void close() {
    hBaseProcessor.destroyTable();
  }

  @Override
  public Optional<String> load(Pair<String, HBaseColumn> key) throws Exception {
    return get(key);
  }

  @Override
  public Map<Pair<String, HBaseColumn>, Optional<String>> loadAll(Iterable<? extends Pair<String, HBaseColumn>> keys) throws Exception {
    ArrayList<Pair<String, HBaseColumn>> keyList = Lists.newArrayList(keys);
    List<Optional<String>> values = get(keyList);

    Iterator<? extends Pair<String, HBaseColumn>> keyIterator = keyList.iterator();
    Iterator<Optional<String>> valueIterator = values.iterator();

    Map<Pair<String, HBaseColumn>, Optional<String>> result = new HashMap<>(keyList.size());
    while (keyIterator.hasNext() && valueIterator.hasNext()) {
      result.put(keyIterator.next(), valueIterator.next());
    }
    return result;
  }

  public Optional<String> get(Pair<String, HBaseColumn> key) throws Exception {
    if(key.getKey().isEmpty()) {
      return Optional.absent();
    }
    Get g = new Get(Bytes.toBytes(key.getKey()));
    if(key.getValue().getCf() != null && key.getValue().getQualifier() != null) {
      g.addColumn(key.getValue().getCf(), key.getValue().getQualifier());
    }
    if(key.getValue().getTimestamp() > 0) {
      g.setTimeStamp(key.getValue().getTimestamp());
    }
    Result result = hBaseProcessor.get(g);

    String value = getValue(key.getValue(), result);
    return Optional.fromNullable(value);
  }

  public List<Optional<String>> get(List<Pair<String, HBaseColumn>> keys) throws Exception {
    ArrayList<Optional<String>> values = new ArrayList<>();
    List<Get> gets = new ArrayList<>();
    for(Pair<String, HBaseColumn> key : keys) {
      Get g = new Get(Bytes.toBytes(key.getKey()));

      HBaseColumn hBaseColumn = key.getValue();
      if(hBaseColumn.getCf() != null && hBaseColumn.getQualifier() != null) {
        g.addColumn(hBaseColumn.getCf(), hBaseColumn.getQualifier());
      }
      if(hBaseColumn.getTimestamp() > 0) {
        g.setTimeStamp(hBaseColumn.getTimestamp());
      }
      gets.add(g);
    }

    Result[] results = hBaseProcessor.get(gets);

    int index = 0;
    for(Pair<String, HBaseColumn> key : keys) {
      Result result = results[index];
      HBaseColumn hBaseColumn = key.getValue();

      String value = getValue(hBaseColumn, result);
      values.add(Optional.fromNullable(value));
      index++;
    }
    return values;
  }

  private String getValue(HBaseColumn hBaseColumn, Result result) {
    String value = null;
    if(result.isEmpty()) {
      return value;
    }
    if(hBaseColumn.getCf() == null || hBaseColumn.getQualifier() == null) {
      Map<String, String> columnMap = new HashMap<>();
      // parse column family, column, timestamp, and value
      for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : result.getMap().entrySet()) {
        String columnFamily = Bytes.toString(entry.getKey());
        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> cells : entry.getValue().entrySet()) {
          String column = Bytes.toString(cells.getKey());
          NavigableMap<Long, byte[]> cell = cells.getValue();
          Map.Entry<Long, byte[]> v = cell.lastEntry();
          String columnValue = Bytes.toString(v.getValue());
          columnMap.put(columnFamily + ":" + column, columnValue);
        }
      }
      JSONObject json = new JSONObject(columnMap);
      value = json.toString();
    } else {
      value = Bytes.toString(result.getValue(hBaseColumn.getCf(), hBaseColumn.getQualifier()));
    }
    return value;
  }
}
