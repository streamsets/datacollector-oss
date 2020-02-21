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
package com.streamsets.pipeline.stage.origin.mysql.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.common.base.Optional;
import com.streamsets.pipeline.stage.origin.event.EnrichedEvent;
import com.streamsets.pipeline.stage.origin.event.EventBuffer;
import com.streamsets.pipeline.stage.origin.mysql.MysqlSchemaRepository;
import com.streamsets.pipeline.stage.origin.mysql.error.MySQLBinLogErrors;
import com.streamsets.pipeline.stage.origin.mysql.offset.BinLogPositionSourceOffset;
import com.streamsets.pipeline.stage.origin.mysql.offset.GtidSourceOffset;
import com.streamsets.pipeline.stage.origin.mysql.offset.SourceOffset;
import com.streamsets.pipeline.stage.origin.mysql.schema.DatabaseAndTable;
import com.streamsets.pipeline.stage.origin.mysql.schema.Table;
import com.streamsets.pipeline.stage.origin.mysql.schema.TableWithoutColumnsNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Event listener for {@link BinaryLogClient} enriching events with metadata, such as table and column names,
 * applying filters and storing results in {@link EventBuffer}.
 */
public class BinaryLogConsumer implements EventListener {
  private static final Logger LOG = LoggerFactory.getLogger(BinaryLogConsumer.class);

  private final MysqlSchemaRepository schemaRepository;
  private final Map<Long, DatabaseAndTable> tableMapping = new HashMap<>();
  private final EventBuffer eventBuffer;
  private final BinaryLogClient client;

  // current consumed gtids
  private String currentGtidSet;

  // current transaction gtid
  private String currentTxGtid;

  // seq no of the CRUD event in current tx
  private int currentTxEventSeqNo;

  private String currentBinLogFileName;

  private SourceOffset currentOffset;

  public BinaryLogConsumer(MysqlSchemaRepository schemaRepository, EventBuffer eventBuffer, BinaryLogClient client) {
    this.schemaRepository = schemaRepository;
    this.eventBuffer = eventBuffer;
    this.client = client;
  }

  public void setOffset(SourceOffset offset) {
    this.currentOffset = offset;
  }

  @Override
  public void onEvent(Event event) {
    LOG.trace("Received event {}", event);
    EventType eventType = event.getHeader().getEventType();
    currentBinLogFileName = client.getBinlogFilename();
    switch (eventType) {
      case TABLE_MAP:
        handleTableMappingEvent((TableMapEventData) event.getData());
        break;
      case PRE_GA_WRITE_ROWS:
      case WRITE_ROWS:
      case EXT_WRITE_ROWS:
        handleRowEvent(event, event.<WriteRowsEventData>getData().getTableId());
        break;
      case PRE_GA_UPDATE_ROWS:
      case UPDATE_ROWS:
      case EXT_UPDATE_ROWS:
        handleRowEvent(event, event.<UpdateRowsEventData>getData().getTableId());
        break;
      case PRE_GA_DELETE_ROWS:
      case DELETE_ROWS:
      case EXT_DELETE_ROWS:
        handleRowEvent(event, event.<DeleteRowsEventData>getData().getTableId());
        break;
      case QUERY:
        QueryEventData queryEventData = event.getData();
        String query = queryEventData.getSql();
        if (isCommit(query)) {
          finishTx();
        } else if (isSchemaChangeQuery(query)) {
          schemaRepository.evictAll();
        }
        break;
      case XID:
        finishTx();
        break;
      case GTID:
        GtidEventData eventData = event.getData();
        currentGtidSet = client.getGtidSet();
        currentTxGtid = eventData.getGtid();
        currentTxEventSeqNo = 0;
        LOG.trace("Started new tx, gtid: {}", currentTxGtid);
        break;
      default:
        // ignore
        break;
    }
  }

  private void finishTx() {
    if (isGtidEnabled()) {
      // remove tx from offset's incomplete transactions
      String nextGtidSet = client.getGtidSet();
      if (currentOffset != null) {
        currentOffset = ((GtidSourceOffset) currentOffset)
            .finishTransaction(currentTxGtid)
            .withGtidSet(nextGtidSet);
      }
      LOG.trace("Finished tx {}. Current offset: {}", currentTxGtid, currentOffset);
    }
  }

  private void handleTableMappingEvent(TableMapEventData eventData) {
    tableMapping.put(
        eventData.getTableId(),
        new DatabaseAndTable(eventData.getDatabase(), eventData.getTable())
    );
  }

  private void handleRowEvent(Event event, long tableId) {
    LOG.trace("New event, current offset: {}, event: {}", currentOffset, event);
    currentTxEventSeqNo++;

    // for gtid offsets it is impossible to position client to precise position (it always positions to tx beginning)
    // so we need to add additional filtering based on event seqNo and skip some events
    if (currentOffset instanceof GtidSourceOffset) {
      if (((GtidSourceOffset) currentOffset).incompleteTransactionsContain(currentTxGtid, currentTxEventSeqNo)) {
        LOG.info("Skipping event gtid {}, seqNo {}", currentTxGtid, currentTxEventSeqNo);
        // skip
        return;
      } else {
        // record current gtid + seqNo as incomplete tx
        currentOffset = ((GtidSourceOffset) currentOffset)
            .withIncompleteTransaction(currentTxGtid, currentTxEventSeqNo)
            .withGtidSet(currentGtidSet);
      }
    } else {
      // current offset is null or gtid mode off
      currentOffset = createOffset(event);
    }

    DatabaseAndTable tableName = tableMapping.get(tableId);
    Optional<? extends Table> tableOpt = schemaRepository.getTable(tableName);
    Table table = null;
    if (tableOpt.isPresent()) {
      table = tableOpt.get();
    } else {
      LOG.error(MySQLBinLogErrors.MYSQL_BIN_LOG_002.getMessage(), tableName.getDatabase(), tableName.getTable());
      // fallback to table without columns names
      table = new TableWithoutColumnsNames(tableName.getDatabase(), tableName.getTable());
    }
    EnrichedEvent enrichedEvent = new EnrichedEvent(event, table, currentOffset);
    if (!eventBuffer.put(enrichedEvent)) {
      LOG.error("Error adding event to buffer. Closing event buffer, disconnecting client.");
      eventBuffer.close();
      try {
        client.disconnect();
      } catch (IOException e) {
        LOG.error("Error disconnecting client: {}", e.getMessage(), e);
      }
    }
  }

  private boolean isGtidEnabled() {
    // when gtid enabled all CRUD events are preceded by GTID event
    return currentTxGtid != null;
  }

  private SourceOffset createOffset(Event event) {
    if (isGtidEnabled()) {
      return new GtidSourceOffset(currentGtidSet)
          .withIncompleteTransaction(currentTxGtid, currentTxEventSeqNo);
    } else {
      return new BinLogPositionSourceOffset(
          currentBinLogFileName,
          ((EventHeaderV4) event.getHeader()).getNextPosition()
      );
    }
  }

  private static boolean isCommit(String sql) {
    return "COMMIT".equals(sql);
  }

  private static boolean isSchemaChangeQuery(String sql) {
    String q = sql.toLowerCase().trim();
    // remove extra spaces
    q = q.replaceAll(" {2,}", " ");
    return (q.startsWith("alter table") || q.startsWith("alter ignore table") || q.startsWith("drop table"));
  }
}
