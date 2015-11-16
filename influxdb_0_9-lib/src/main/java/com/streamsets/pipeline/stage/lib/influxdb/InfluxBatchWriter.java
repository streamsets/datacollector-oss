/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.lib.influxdb;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class InfluxBatchWriter {
  private static final Logger LOG = LoggerFactory.getLogger(InfluxBatchWriter.class);
  private static final ImmutableSet<String> metadataFields = ImmutableSet.of("/time", "/time_hires", "/plugin");
  private static final ImmutableSet<String> tagFields = ImmutableSet.of(
      "/host", "/type", "/plugin_instance", "/type_instance"
  );

  private final InfluxDB influx;
  private final String dbName;
  private final InfluxDB.ConsistencyLevel consistencyLevel;
  private final String retentionPolicy;

  public InfluxBatchWriter(
      String influxEndpoint,
      String username,
      String password,
      String dbName,
      InfluxDB.ConsistencyLevel consistencyLevel,
      String retentionPolicy
  ) {
    influx = InfluxDBFactory.connect(influxEndpoint, username, password);
    this.dbName = dbName;
    this.consistencyLevel = consistencyLevel;
    this.retentionPolicy = retentionPolicy;
  }

  /**
   * Writes a batch of records including provided tags
   * @param batch of SDC Records
   * @return number of records written
   */
  public long writeBatch(Batch batch) throws OnRecordErrorException {
    long recordsWritten = 0;

    BatchPoints batchPoints = BatchPoints
        .database(dbName)
        .consistency(consistencyLevel)
        .retentionPolicy(retentionPolicy)
        .build();

    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      Record record = records.next();

      String measurement = record.get("/plugin").getValueAsString();
      long timestamp = 0L;
      TimeUnit timeUnit = TimeUnit.NANOSECONDS;

      if (record.has("/time_hires")) {
        timestamp = record.get("/time_hires").getValueAsLong();
      } else if (record.has("/time")) {
        timestamp = record.get("/time").getValueAsLong();
        timeUnit = TimeUnit.MILLISECONDS;
      }

      Set<String> fieldPaths = record.getFieldPaths();
      Map<String, Object> fields = new HashMap<>();
      Map<String, String> tags = new HashMap<>();

      for (String fieldPath : fieldPaths) {
        if (!metadataFields.contains(fieldPath) && !tagFields.contains(fieldPath) && !fieldPath.isEmpty()) {
          fields.put("value", record.get(fieldPath).getValue());
        }
      }

      for (String tagField : tagFields) {
        if (record.has(tagField)) {
          String tagValue = record.get(tagField).getValueAsString();
          if (!tagValue.isEmpty()) {
            tags.put(tagField.substring(1), tagValue);
          }
        }
      }

      Point point = Point.measurement(measurement)
          .time(timestamp, timeUnit)
          .fields(fields)
          .tag(tags)
          .build();

      batchPoints.point(point);
      ++recordsWritten;
    }

    influx.write(batchPoints);
    return recordsWritten;
  }
}
