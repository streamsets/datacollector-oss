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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CollectdRecordConverter implements RecordConverter {
  private static final Joiner joiner = Joiner.on("_");
  private static final String FIELD_PATH_PREFIX = "/";
  private static final Set<String> TAG_FIELDS = ImmutableSet.of(
      "host",
      "plugin_instance",
      "instance",
      "type",
      "type_instance"
  );
  private static final Set<String> NON_VALUE_FIELDS = ImmutableSet.of(
      "time",
      "time_hires",
      "plugin"
  );
  private static final String PLUGIN = "plugin";
  private static final String TIME = "time";
  private static final String TIME_HIRES = "time_hires";

  private final GenericRecordConverterConfigBean conf;

  public CollectdRecordConverter() {
    this.conf = new GenericRecordConverterConfigBean();
  }

  public CollectdRecordConverter(GenericRecordConverterConfigBean conf) {
    this.conf = conf;
  }

  @VisibleForTesting
  static Map<String, String> getTags(List<String> tagFields, Record record) throws OnRecordErrorException {
    Map<String, String> tags = new HashMap<>(TAG_FIELDS.size());

    for (String tag : TAG_FIELDS) {
      putIfTag(record, tags, tag);
    }

    tags.putAll(RecordConverterUtil.getTags(tagFields, record));
    return tags;
  }

  private static void putIfTag(Record record, Map<String, String> tags, String tag) {
    if (record.has(FIELD_PATH_PREFIX + tag)) {
      String tagValue = record.get(FIELD_PATH_PREFIX + tag).getValueAsString();
      if (tagValue != null && !tagValue.isEmpty()) {
        // To match the behavior of Influx's built-in collectd support we must rename this field.
        if ("plugin_instance".equals(tag)) {
          tags.put("instance", tagValue);
        } else {
          tags.put(tag, tagValue);
        }
      }
    }
  }

  @VisibleForTesting
  static String getMeasurementBaseName(Record record) throws OnRecordErrorException {
    if (!record.has(FIELD_PATH_PREFIX + PLUGIN)) {
      throw new OnRecordErrorException(Errors.INFLUX_03);
    }

    return record.get(FIELD_PATH_PREFIX + PLUGIN).getValueAsString();
  }

  @VisibleForTesting
  static long getTime(Record record) throws OnRecordErrorException {
    if (record.has(FIELD_PATH_PREFIX + TIME_HIRES)) {
      return fromCollectdHighResTime(record.get(FIELD_PATH_PREFIX + TIME_HIRES).getValueAsLong());
    } else if (record.has(FIELD_PATH_PREFIX + TIME)) {
      return record.get(FIELD_PATH_PREFIX + TIME).getValueAsLong();
    }

    throw new OnRecordErrorException(Errors.INFLUX_03);
  }

  private static long fromCollectdHighResTime(long value) {
    // https://collectd.org/wiki/index.php/High_resolution_time_format
    return ((value >> 30) * 1000000000) + ((value & 0x3FFFFFFF) << 30) / 1000000000;
  }

  @VisibleForTesting
  static TimeUnit getTimePrecision(Record record) throws OnRecordErrorException {
    if (record.has(FIELD_PATH_PREFIX + TIME_HIRES)) {
      return TimeUnit.NANOSECONDS;
    } else if (record.has(FIELD_PATH_PREFIX + TIME)) {
      return TimeUnit.MILLISECONDS;
    }

    throw new OnRecordErrorException(Errors.INFLUX_04);
  }

  @VisibleForTesting
  List<String> getValueFields(Record record) throws OnRecordErrorException {
    List<String> fields = new ArrayList<>();
    Set<String> fieldPaths = record.getEscapedFieldPaths();

    for (String fieldPath : fieldPaths) {
      if (!isValueField(fieldPath)) {
        continue;
      }
      fields.add(stripPathPrefix(fieldPath));
    }

    if (fields.isEmpty()) {
      throw new OnRecordErrorException(Errors.INFLUX_06);
    }

    return fields;
  }

  static String stripPathPrefix(String fieldPath) {
    return fieldPath.substring(fieldPath.lastIndexOf(FIELD_PATH_PREFIX) + 1);
  }

  @VisibleForTesting
  boolean isValueField(String fieldPath) {
    String fieldName = stripPathPrefix(fieldPath);
    if (fieldPath.isEmpty() || FIELD_PATH_PREFIX.equals(fieldPath) ) {
      return false;
    }
    return !TAG_FIELDS.contains(fieldName) && !NON_VALUE_FIELDS.contains(fieldName)
        && !conf.tagFields.contains(fieldPath);
  }

  @Override
  public List<Point> getPoints(Record record) throws OnRecordErrorException {
    List<Point> points = new ArrayList<>();

    List<String> fieldNames = getValueFields(record);
    for (String fieldName : fieldNames) {
      points.add(Point
          .measurement(joiner.join(getMeasurementBaseName(record), fieldName))
          .tag(getTags(conf.tagFields, record))
          .time(getTime(record), getTimePrecision(record))
          .field("value", record.get(FIELD_PATH_PREFIX + fieldName).getValue())
          .build()
      );
    }
    return points;
  }
}
