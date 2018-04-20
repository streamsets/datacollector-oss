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
package com.streamsets.pipeline.lib.io.fileref;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ChecksumAlgorithm;
import com.streamsets.pipeline.lib.event.EventCreator;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class FileRefUtil {
  private FileRefUtil() {}

  //Metric Constants
  public static final String GAUGE_NAME = "File Transfer Statistics";
  public static final String FILE = "File";
  public static final String TRANSFER_THROUGHPUT = "Transfer Rate";
  public static final String SENT_BYTES = "Sent Bytes";
  public static final String REMAINING_BYTES = "Remaining Bytes";
  public static final String TRANSFER_THROUGHPUT_METER = "transferRate";
  public static final String COMPLETED_FILE_COUNT = "Completed File Count";

  public static final String BRACKETED_TEMPLATE = "%s (%s)";

  //Whole File Record constants
  public static final String FILE_REF_FIELD_NAME = "fileRef";
  public static final String FILE_INFO_FIELD_NAME = "fileInfo";

  public static final String FILE_REF_FIELD_PATH = "/" + FILE_REF_FIELD_NAME;
  public static final String FILE_INFO_FIELD_PATH = "/" + FILE_INFO_FIELD_NAME;


  //Whole File event Record constants
  public static final String WHOLE_FILE_WRITE_FINISH_EVENT = "wholeFileProcessed";

  public static final String WHOLE_FILE_SOURCE_FILE_INFO = "sourceFileInfo";
  public static final String WHOLE_FILE_TARGET_FILE_INFO = "targetFileInfo";

  public static final String WHOLE_FILE_SOURCE_FILE_INFO_PATH = "/" + WHOLE_FILE_SOURCE_FILE_INFO;
  public static final String WHOLE_FILE_TARGET_FILE_INFO_PATH = "/" + WHOLE_FILE_TARGET_FILE_INFO;

  public static final String WHOLE_FILE_CHECKSUM = "checksum";
  public static final String WHOLE_FILE_CHECKSUM_ALGO = "checksumAlgorithm";

  public static final Joiner COMMA_JOINER = Joiner.on(",");

  public static final EventCreator FILE_TRANSFER_COMPLETE_EVENT =
      new EventCreator.Builder(FileRefUtil.WHOLE_FILE_WRITE_FINISH_EVENT, 1)
          .withRequiredField(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO)
          .withRequiredField(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO)
          .withOptionalField(FileRefUtil.WHOLE_FILE_CHECKSUM)
          .withOptionalField(FileRefUtil.WHOLE_FILE_CHECKSUM_ALGO)
          .build();


  public static final ImmutableSet<String> MANDATORY_METADATA_INFO =
      new ImmutableSet.Builder<String>().add("size").build();


  public static final Set<String> MANDATORY_FIELD_PATHS =
      ImmutableSet.of(FILE_REF_FIELD_PATH, FILE_INFO_FIELD_PATH, FILE_INFO_FIELD_PATH + "/size");

  public static final Map<String, Integer> GAUGE_MAP_ORDERING
      = new ImmutableMap.Builder<String, Integer>()
      .put(FileRefUtil.FILE, 1)
      .put(FileRefUtil.TRANSFER_THROUGHPUT, 2)
      .put(FileRefUtil.SENT_BYTES, 3)
      .put(FileRefUtil.REMAINING_BYTES, 4)
      .put(FileRefUtil.COMPLETED_FILE_COUNT, 5)
      .build();

  /**
   * Creates a gauge if it is already not. This is done only once for the stage
   * @param context the {@link com.streamsets.pipeline.api.Stage.Context} of this stage
   */
  @SuppressWarnings("unchecked")
  public static void initMetricsIfNeeded(ProtoConfigurableEntity.Context context) {
    Gauge<Map<String, Object>> gauge = context.getGauge(FileRefUtil.GAUGE_NAME);
    if(gauge == null) {
      gauge = context.createGauge(FileRefUtil.GAUGE_NAME, Comparator.comparing(GAUGE_MAP_ORDERING::get));
      Map<String, Object> gaugeStatistics = gauge.getValue();
      //File name is populated at the MetricEnabledWrapperStream.
      gaugeStatistics.put(FileRefUtil.FILE, "");
      gaugeStatistics.put(FileRefUtil.TRANSFER_THROUGHPUT, 0L);
      gaugeStatistics.put(FileRefUtil.SENT_BYTES, String.format(FileRefUtil.BRACKETED_TEMPLATE, 0, 0));
      gaugeStatistics.put(FileRefUtil.REMAINING_BYTES, 0L);
      gaugeStatistics.put(FileRefUtil.COMPLETED_FILE_COUNT, 0L);
    }

    Meter dataTransferMeter = context.getMeter(FileRefUtil.TRANSFER_THROUGHPUT_METER);
    if (dataTransferMeter == null) {
      context.createMeter(FileRefUtil.TRANSFER_THROUGHPUT_METER);
    }
  }

  public static Field getWholeFileRecordRootField(FileRef fileRef, Map<String, Object> metadata) {
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    map.put(FILE_REF_FIELD_NAME, Field.create(Field.Type.FILE_REF, fileRef));
    map.put(FILE_INFO_FIELD_NAME, createFieldForMetadata(metadata));
    return Field.create(map);
  }

  public static Field createFieldForMetadata(Object metadataObject) {
    if (metadataObject == null) {
      return Field.create("");
    }
    if (metadataObject instanceof Boolean) {
      return Field.create((Boolean) metadataObject);
    } else if (metadataObject instanceof Character) {
      return Field.create((Character) metadataObject);
    } else if (metadataObject instanceof Byte) {
      return Field.create((Byte) metadataObject);
    } else if (metadataObject instanceof Short) {
      return Field.create((Short) metadataObject);
    } else if (metadataObject instanceof Integer) {
      return Field.create((Integer) metadataObject);
    } else if (metadataObject instanceof Long) {
      return Field.create((Long) metadataObject);
    } else if (metadataObject instanceof Float) {
      return Field.create((Float) metadataObject);
    } else if (metadataObject instanceof Double) {
      return Field.create((Double) metadataObject);
    } else if (metadataObject instanceof Date) {
      return Field.createDatetime((Date) metadataObject);
    } else if (metadataObject instanceof BigDecimal) {
      return Field.create((BigDecimal) metadataObject);
    } else if (metadataObject instanceof String) {
      return Field.create((String) metadataObject);
    } else if (metadataObject instanceof byte[]) {
      return Field.create((byte[]) metadataObject);
    } else if (metadataObject instanceof Collection) {
      Iterator iterator = ((Collection)metadataObject).iterator();
      List<Field> fields = new ArrayList<>();
      while (iterator.hasNext()) {
        fields.add(createFieldForMetadata(iterator.next()));
      }
      return Field.create(fields);
    } else if (metadataObject instanceof Map) {
      boolean isListMap = (metadataObject instanceof LinkedHashMap);
      Map<String, Field> fieldMap = isListMap? new LinkedHashMap<String, Field>() : new HashMap<String, Field>();
      Map<Object, Object> map = (Map)metadataObject;
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        fieldMap.put(entry.getKey().toString(), createFieldForMetadata(entry.getValue()));
      }
      return isListMap? Field.create(Field.Type.LIST_MAP, fieldMap) : Field.create(fieldMap);
    } else {
      return Field.create(metadataObject.toString());
    }
  }

  public static EventRecord createAndInitWholeFileEventRecord(Stage.Context context) {
    String recordSourceId = Utils.format("event:{}:{}:{}", WHOLE_FILE_WRITE_FINISH_EVENT, 1, System.currentTimeMillis());
    EventRecord wholeFileEventRecord = context.createEventRecord(WHOLE_FILE_WRITE_FINISH_EVENT, 1, recordSourceId);
    Map<String, Field> fieldMap = new HashMap<>();
    fieldMap.put(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO, Field.create(Field.Type.MAP, new HashMap<String, Field>()));
    fieldMap.put(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO, Field.create(Field.Type.MAP, new HashMap<String, Field>()));
    wholeFileEventRecord.set(Field.create(Field.Type.MAP, fieldMap));
    return wholeFileEventRecord;
  }

  @SuppressWarnings("unchecked")
  public static  <T extends AutoCloseable> T getReadableStream(
      ProtoConfigurableEntity.Context context,
      FileRef fileRef,
      Class<T> streamClass,
      boolean includeChecksumInTheEvents,
      ChecksumAlgorithm checksumAlgorithm,
      StreamCloseEventHandler<?> streamCloseEventHandler
  ) throws IOException {
    T stream = fileRef.createInputStream(context, streamClass);
    if (includeChecksumInTheEvents) {
      Utils.checkArgument(
          FileRefStreamCloseEventHandler.class.isAssignableFrom(streamCloseEventHandler.getClass()),
          "Stream Close Event handler should be of type " + FileRefStreamCloseEventHandler.class.getCanonicalName()
      );
      stream = (T) new ChecksumCalculatingWrapperStream(stream, checksumAlgorithm.getHashType(), streamCloseEventHandler);
    }
    return stream;
  }

  public static void validateWholeFileRecord(Record record) {
    Set<String> fieldPathsInRecord = record.getEscapedFieldPaths();
    Utils.checkArgument(
        fieldPathsInRecord.containsAll(MANDATORY_FIELD_PATHS),
        Utils.format(
            "Record does not contain the mandatory fields {} for Whole File Format.",
            COMMA_JOINER.join(Sets.difference(MANDATORY_FIELD_PATHS, fieldPathsInRecord))
        )
    );
  }
  public static ELEval createElEvalForRateLimit(Stage.Context context) {
    return context.createELEval("rateLimit");
  }

  public static Double evaluateAndGetRateLimit(ELEval elEval, ELVars elVars, String rateLimit) throws ELEvalException {
    return elEval.eval(elVars, rateLimit, Double.class);
  }
}
