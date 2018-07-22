/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaAndTable;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresWalRunner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresWalRunner.class);
  private PostgresCDCSource postgresCDCSource;
  private boolean hasTableFilter;
  private boolean hasStartFilter;
  private boolean hasOperationFilter;

  public PostgresWalRunner(PostgresCDCSource pgSource) {
    this.postgresCDCSource = pgSource;
    this.hasTableFilter = true;
    this.hasStartFilter = true;
    this.hasOperationFilter = true;
  }

  @Override
  public void run()  {
    ByteBuffer buffer = null;
    LogSequenceNumber lastLSN = null;
    PGReplicationStream stream = postgresCDCSource.getWalReceiver().getStream();
    try {
      buffer = stream.readPending();
      while (buffer != null) {
        //feedback
        lastLSN = stream.getLastReceiveLSN();
        if (lastLSN.asLong() == 0) {
          lastLSN = LogSequenceNumber.valueOf(postgresCDCSource.getOffset());
        }
        PostgresWalRecord postgresWalRecord = new PostgresWalRecord(
            buffer,
            lastLSN,
            postgresCDCSource.getConfigBean().decoderValue
        );

        if (passesFilter(postgresWalRecord)) {
          postgresCDCSource.getQueue().add(postgresWalRecord);
          LOG.debug("CDC: {} ", postgresWalRecord.toString());
        } else {
          LOG.debug("Filtered out CDC: {} ", postgresWalRecord.toString());
        }
        buffer = stream.readPending();
      }
      if (lastLSN != null) {
        stream.forceUpdateStatus();
      }
    } catch (SQLException e) {
      LOG.error("Error reading PostgreSQL replication stream: {}", e.getMessage());
    }

  }

  private boolean passesStartValueFilter(PostgresWalRecord postgresWalRecord) {
    // If the startValue parameter isn't a date, we can ignore this filter since
    // LSN filtering is done via the Replication connection in PostgresWalReceiver
    if ( ! postgresCDCSource.getConfigBean().startValue.equals(StartValues.DATE)) {
      hasStartFilter = false;
      return true;
    }

    /*
        This is the date that shows up in the CDC record:
        "2018-07-06 12:57:33.383899-07";
      -07 refers to Pacific Summer Time.

      This won't parse. It will parse if there is "-07:00" instead.

      Since 2018-07-06 12:57:33.383899 is static (26 chars), if we see 29chars we assume
      that the :xx is missing for timezones that are +/- 30min differences.

     */

    String changeTimeStr = postgresWalRecord.getTimestamp();
    String dateFormat = "yyyy-MM-dd HH:mm:ss.SSSSSSXXX";
    if(changeTimeStr.length() == 29) {
      changeTimeStr += ":00";
    }
    if(changeTimeStr.length() == 28) {
      changeTimeStr += ":00";
      dateFormat = "yyyy-MM-dd HH:mm:ss.SSSSSXXX";
    }
    if(changeTimeStr.length() == 27) {
      changeTimeStr += ":00";
      dateFormat = "yyyy-MM-dd HH:mm:ss.SSSSXXX";
    }
    if(changeTimeStr.length() == 26) {
      changeTimeStr += ":00";
      dateFormat = "yyyy-MM-dd HH:mm:ss.SSSXXX";
    }
    ZonedDateTime zonedDateTime = ZonedDateTime
        .parse(changeTimeStr, DateTimeFormatter.ofPattern(dateFormat));
    ZoneOffset startZoneOffset = postgresCDCSource.getZoneId().getRules().getOffset(postgresCDCSource
        .getStartDate());
    ZonedDateTime startDate =  OffsetDateTime.of(postgresCDCSource.getStartDate(),
        startZoneOffset).toZonedDateTime();
    //Compare to configured startDate. If its less than start, then ignore
    if (zonedDateTime.compareTo(startDate) < 0 ) {
      return false;
    }

    return true;
  }

  private boolean passesOperationFilter(PostgresWalRecord postgresWalRecord) {
    List<PostgresChangeTypeValues> configuredChangeTypes = postgresCDCSource
        .getConfigBean()
        .postgresChangeTypes;

    List<String> changeTypes = new ArrayList<String>();
    for (PostgresChangeTypeValues configuredChangeType : configuredChangeTypes) {
      changeTypes.add(configuredChangeType.getLabel());
    }

    // This means that all legal operations are requested.
    if (changeTypes.size() == PostgresChangeTypeValues.values().length) {
      hasOperationFilter = false;
      return true;
    }

    List<Field> changes = postgresWalRecord.getChanges();


    /*
      Please note carefully. Due to the nature of Postgres CDC, many types are contained
      in the one record. INSERT, UPDATE, DELETE all can be present in the one CDC record.
      If the user configures a filter such that only INSERT, UPDATE are wanted (and by
      inference DELETE is NOT then if ANY change part contains DELETE, then we filter out
      the whole CDC. To just rip out the DELETE part, it is recommended to use FieldPivot
     */
    for (Field change: changes) {
      String changeType = PostgresWalRecord.getTypeFromChangeMap(change.getValueAsMap());
      if (! changeTypes.contains(changeType)) {
        return false;
      }
    }
    return true;
  }

  private boolean passesTableFilter(PostgresWalRecord postgresWalRecord) {
    //The list if valid schemas and tables
    List<SchemaAndTable> schemasAndTables =
        postgresCDCSource.getWalReceiver().getSchemasAndTables();

    if (schemasAndTables == null || schemasAndTables.isEmpty()) {
      hasTableFilter = false;
      return true;
    }

    //Changes are structured as a Map with key:"change" that returns a list of all changes
    List<Field> changes = postgresWalRecord.getChanges();
    // Inside each element in the change list is a map with keys "schema" and "table"
    // that have the values for the the schema and table for the change.
    for (Field change : changes) {
      String table = PostgresWalRecord.getTableFromChangeMap(change.getValueAsMap());
      String schema = PostgresWalRecord.getSchemaFromChangeMap(change.getValueAsMap());
      SchemaAndTable changeSchemaAndTable = new SchemaAndTable(schema, table);
      // If the change's schema/table pair is not in the valid range, we filter out (return false)
      if (!schemasAndTables.contains(changeSchemaAndTable)) {
        return false;
      }
    }
    return true;
  }

  public boolean passesFilter(PostgresWalRecord postgresWalRecord) {

    if (( ! hasTableFilter) &&
        ( ! hasStartFilter) &&
        ( ! hasOperationFilter)) {
      return true;
    }

    if (hasTableFilter && ( ! passesTableFilter(postgresWalRecord))) {
      return false;
    }

    if (hasStartFilter && ( ! passesStartValueFilter(postgresWalRecord))) {
      return false;
    }

    if (hasOperationFilter && ( ! passesOperationFilter(postgresWalRecord))) {
      return false;
    }

    return true;
  }

}
