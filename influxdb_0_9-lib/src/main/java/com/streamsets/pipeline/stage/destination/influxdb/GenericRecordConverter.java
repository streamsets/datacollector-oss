/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.influxdb;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.stage.destination.influxdb.CollectdRecordConverter.stripPathPrefix;

public class GenericRecordConverter implements RecordConverter {
  private final GenericRecordConverterConfigBean conf;

  public GenericRecordConverter(GenericRecordConverterConfigBean conf) {
    this.conf = conf;
  }

  @Override
  public List<Point> getPoints(Record record) throws OnRecordErrorException {
    List<Point> points = new ArrayList<>();

    verifyRequireFieldsPresent(record);

    final String measurementName = record.get(conf.measurementField).getValueAsString();

    for (String fieldPath : conf.valueFields) {
      if (!record.has(fieldPath)) {
        continue;
      }
      Point.Builder point = Point
          .measurement(measurementName)
          .tag(getTags(record))
          .field(stripPathPrefix(fieldPath), record.get(fieldPath).getValue());

      if (!conf.timeField.isEmpty()) {
        point.time(getTime(record), conf.timeUnit);
      }

      points.add(point.build());
    }
    return points;
  }

  private long getTime(Record record) throws OnRecordErrorException {
    Field timeField = record.get(conf.timeField);
    if (timeField.getType() == Field.Type.DATE || timeField.getType() == Field.Type.DATETIME) {
      return timeField.getValueAsDatetime().getTime();
    } else if (timeField.getType() == Field.Type.LONG) {
      return timeField.getValueAsLong();
    }

    throw new OnRecordErrorException(Errors.INFLUX_09, timeField.getType());
  }

  private void verifyRequireFieldsPresent(Record record) throws OnRecordErrorException {
    if (!record.has(conf.measurementField)) {
      throw new OnRecordErrorException(Errors.INFLUX_07, conf.measurementField);
    }

    if (!conf.timeField.isEmpty() && !record.has(conf.timeField)) {
      throw new OnRecordErrorException(Errors.INFLUX_07, conf.timeField);
    }
  }

  private Map<String, String> getTags(Record record) throws OnRecordErrorException {
    Map<String, String> tags = new HashMap<>();

    for (String fieldPath : conf.tagFields) {
      if (!record.has(fieldPath)) {
        continue;
      }

      Field tagField = record.get(fieldPath);
      switch (tagField.getType()) {
        case MAP:
          // fall through
        case LIST_MAP:
          for (Map.Entry<String, Field> entry : tagField.getValueAsMap().entrySet()) {
            tags.put(entry.getKey(), entry.getValue().getValueAsString());
          }
          break;
        case LIST:
          throw new OnRecordErrorException(Errors.INFLUX_08, fieldPath);
        default:
          tags.put(stripPathPrefix(fieldPath), tagField.getValueAsString());
          break;
      }
    }

    return tags;
  }
}
