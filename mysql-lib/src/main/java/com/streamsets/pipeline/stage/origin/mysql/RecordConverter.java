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
package com.streamsets.pipeline.stage.origin.mysql;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.origin.event.EnrichedEvent;
import com.streamsets.pipeline.stage.origin.mysql.offset.BinLogPositionSourceOffset;
import com.streamsets.pipeline.stage.origin.mysql.offset.GtidSourceOffset;
import com.streamsets.pipeline.stage.origin.mysql.offset.SourceOffset;
import com.streamsets.pipeline.stage.origin.mysql.schema.Column;
import com.streamsets.pipeline.stage.origin.mysql.schema.ColumnValue;
import com.streamsets.pipeline.stage.origin.mysql.schema.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.api.Field.create;

public class RecordConverter {
  static final String TYPE_FIELD = "Type";
  static final String DATABASE_FIELD = "Database";
  static final String TABLE_FIELD = "Table";
  static final String BIN_LOG_FILENAME_FIELD = "BinLogFilename";
  static final String BIN_LOG_POSITION_FIELD = "BinLogPosition";
  static final String GTID_FIELD = "GTID";
  static final String EVENT_SEQ_NO_FIELD = "SeqNo";
  static final String OFFSET_FIELD = "Offset";
  static final String SERVER_ID_FIELD = "ServerId";
  static final String TIMESTAMP_ID_FIELD = "Timestamp";
  static final String DATA_FIELD = "Data";
  static final String OLD_DATA_FIELD = "OldData";

  private final RecordFactory recordFactory;

  public RecordConverter(RecordFactory recordFactory) {
    this.recordFactory = recordFactory;
  }

  public List<Record> toRecords(EnrichedEvent event) {
    EventType eventType = event.getEvent().getHeader().getEventType();
    switch (eventType) {
      case PRE_GA_WRITE_ROWS:
      case WRITE_ROWS:
      case EXT_WRITE_ROWS:
        return toRecords(
            event.getTable(),
            event.getEvent().getHeader(),
            event.getEvent().<WriteRowsEventData>getData(),
            event.getOffset()
        );
      case PRE_GA_UPDATE_ROWS:
      case UPDATE_ROWS:
      case EXT_UPDATE_ROWS:
        return toRecords(
            event.getTable(),
            event.getEvent().getHeader(),
            event.getEvent().<UpdateRowsEventData>getData(),
            event.getOffset()
        );
      case PRE_GA_DELETE_ROWS:
      case DELETE_ROWS:
      case EXT_DELETE_ROWS:
        return toRecords(
            event.getTable(),
            event.getEvent().getHeader(),
            event.getEvent().<DeleteRowsEventData>getData(),
            event.getOffset()
        );
      default:
        throw new IllegalArgumentException(String.format("EventType '%s' not supported", eventType));
    }
  }


  private List<Record> toRecords(Table table,
                                 EventHeader eventHeader,
                                 UpdateRowsEventData eventData,
                                 SourceOffset offset) {
    List<Record> res = new ArrayList<>(eventData.getRows().size());
    for (Map.Entry<Serializable[], Serializable[]> row : eventData.getRows()) {
      Record record = recordFactory.create(offset.format());
      Map<String, Field> fields = createHeader(table, eventHeader, offset);
      fields.put(TYPE_FIELD, create("UPDATE"));
      record.getHeader().setAttribute(
          OperationType.SDC_OPERATION_TYPE,
          String.valueOf(OperationType.UPDATE_CODE)
      );
      List<ColumnValue> columnValuesOld = zipColumnsValues(
          eventData.getIncludedColumnsBeforeUpdate(),
          table,
          row.getKey()
      );
      Map<String, Field> oldData = toMap(columnValuesOld);
      fields.put(OLD_DATA_FIELD, create(oldData));

      List<ColumnValue> columnValues = zipColumnsValues(
          eventData.getIncludedColumns(),
          table,
          row.getValue()
      );
      Map<String, Field> data = toMap(columnValues);
      fields.put(DATA_FIELD, create(data));

      record.set(create(fields));
      res.add(record);
    }
    return res;
  }

  private List<Record> toRecords(Table table,
                                 EventHeader eventHeader,
                                 WriteRowsEventData eventData,
                                 SourceOffset offset) {
    List<Record> res = new ArrayList<>(eventData.getRows().size());
    for (Serializable[] row : eventData.getRows()) {
      Record record = recordFactory.create(offset.format());
      Map<String, Field> fields = createHeader(table, eventHeader, offset);
      fields.put(TYPE_FIELD, create("INSERT"));
      record.getHeader().setAttribute(
          OperationType.SDC_OPERATION_TYPE,
          String.valueOf(OperationType.INSERT_CODE)
      );
      List<ColumnValue> columnValues = zipColumnsValues(
          eventData.getIncludedColumns(),
          table,
          row
      );
      Map<String, Field> data = toMap(columnValues);
      fields.put(DATA_FIELD, create(data));

      record.set(create(fields));
      res.add(record);
    }
    return res;
  }

  private List<Record> toRecords(Table table,
                                 EventHeader eventHeader,
                                 DeleteRowsEventData eventData,
                                 SourceOffset offset) {
    List<Record> res = new ArrayList<>(eventData.getRows().size());
    for (Serializable[] row : eventData.getRows()) {
      Record record = recordFactory.create(offset.format());
      Map<String, Field> fields = createHeader(table, eventHeader, offset);
      fields.put(TYPE_FIELD, create("DELETE"));
      record.getHeader().setAttribute(
          OperationType.SDC_OPERATION_TYPE,
          String.valueOf(OperationType.DELETE_CODE)
      );
      List<ColumnValue> columnValues = zipColumnsValues(eventData.getIncludedColumns(), table, row);
      Map<String, Field> data = toMap(columnValues);
      fields.put(OLD_DATA_FIELD, create(data));

      record.set(create(fields));
      res.add(record);
    }
    return res;
  }


  private Map<String, Field> createHeader(Table table, EventHeader header, SourceOffset offset) {
    Map<String, Field> map = new HashMap<>();
    map.put(DATABASE_FIELD, create(table.getDatabase()));
    map.put(TABLE_FIELD, create(table.getName()));
    map.put(OFFSET_FIELD, create(offset.format()));
    map.put(SERVER_ID_FIELD, create(header.getServerId()));
    map.put(TIMESTAMP_ID_FIELD, create(header.getTimestamp()));

    if (offset instanceof BinLogPositionSourceOffset) {
      BinLogPositionSourceOffset bo = (BinLogPositionSourceOffset) offset;
      map.put(BIN_LOG_FILENAME_FIELD, create(bo.getFilename()));
      map.put(BIN_LOG_POSITION_FIELD, create(bo.getPosition()));
    } else if (offset instanceof GtidSourceOffset) {
      GtidSourceOffset go = (GtidSourceOffset) offset;
      map.put(GTID_FIELD, create(go.getGtid()));
      map.put(EVENT_SEQ_NO_FIELD, create(go.getSeqNo()));
    }

    return map;
  }

  private Map<String, Field> toMap(List<ColumnValue> columnValues) {
    Map<String, Field> data = new HashMap<>(columnValues.size());
    for (ColumnValue cv : columnValues) {
      String name = cv.getHeader().getName();
      Field value = cv.getHeader().getType().toField(cv.getValue());
      data.put(name, value);
    }
    return data;
  }

  private List<ColumnValue> zipColumnsValues(BitSet columns, Table table, Serializable[] values) {
    List<ColumnValue> res = new ArrayList<>(columns.size());
    int n = 0;
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i)) {
        Column col = table.getColumn(i);
        res.add(new ColumnValue(col, values[n]));
        n++;
      }
    }
    return res;
  }
}
