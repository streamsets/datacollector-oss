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
package com.streamsets.pipeline.stage.destination.influxdb;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class InfluxTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(InfluxTarget.class);

  private final InfluxConfigBean conf;

  private InfluxDB client;
  private RecordConverter converter;

  public InfluxTarget(InfluxConfigBean conf) {
    this.conf = conf;
  }

  @Override
  public List<ConfigIssue> init(Info info, Target.Context context) {
    List<ConfigIssue> issues = super.init(info, context);

    client = getClient(conf);
    if (!client.describeDatabases().contains(conf.dbName)) {
      if (conf.autoCreate) {
        client.createDatabase(conf.dbName);
      } else {
        issues.add(getContext().createConfigIssue("INFLUX", "dbName", Errors.INFLUX_01, conf.dbName));
      }
    }

    try {
      converter = createRecordConverter(conf.recordConverterType);
    } catch (StageException e) {
      LOG.error(Errors.INFLUX_05.getMessage(), conf.recordConverterType, e);
      issues.add(
          getContext().createConfigIssue("INFLUX", "recordConverterType", Errors.INFLUX_05, conf.recordConverterType)
      );
    }

    return issues;
  }

  private InfluxDB getClient(InfluxConfigBean conf) {
    return InfluxDBFactory.connect(conf.url, conf.username.get(), conf.password.get());
  }

  private RecordConverter createRecordConverter(RecordConverterType recordConverterType) throws StageException {
    if (recordConverterType == RecordConverterType.COLLECTD) {
      return new CollectdRecordConverter(conf.fieldMapping);
    } else if (recordConverterType == RecordConverterType.CUSTOM) {
      return new GenericRecordConverter(conf.fieldMapping);
    }

    throw new StageException(Errors.INFLUX_05, recordConverterType);
  }

  @Override
  public void write(Batch batch) throws StageException {
    BatchPoints batchPoints = BatchPoints
        .database(conf.dbName)
        .retentionPolicy(conf.retentionPolicy)
        .consistency(conf.consistencyLevel)
        .build();

    Iterator<Record> recordIterator = batch.getRecords();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();

      for (Point point : converter.getPoints(record)) {
        batchPoints.point(point);
      }
    }

    client.write(batchPoints);
  }
}
