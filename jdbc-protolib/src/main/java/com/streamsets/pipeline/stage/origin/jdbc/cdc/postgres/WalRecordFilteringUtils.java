/*
 * Copyright 2020 StreamSets Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaAndTable;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class WalRecordFilteringUtils {

  public static PostgresWalRecord filterRecord(PostgresWalRecord postgresWalRecord, PostgresCDCSource postgresCDCSource) {
    if (postgresWalRecord != null) {
      postgresWalRecord = passesStartValueFilter(postgresWalRecord, postgresCDCSource);
    }

    if (postgresWalRecord != null) {
      postgresWalRecord = passesOperationFilter(postgresWalRecord, postgresCDCSource);
    }

    return postgresWalRecord;
  }

  static List<SchemaAndTable> getSchemasAndTables(PostgresCDCSource postgresCDCSource) {
    return postgresCDCSource.getWalReceiver().getSchemasAndTables();
  }

  @VisibleForTesting
  static boolean hasTableFilter(PostgresCDCSource postgresCDCSource) {
    List<SchemaAndTable> schemasAndTables = getSchemasAndTables(postgresCDCSource);
    return schemasAndTables != null && !schemasAndTables.isEmpty();
  }

  @VisibleForTesting
  static boolean hasStartValueFilter(PostgresCDCSource postgresCDCSource) {
    return postgresCDCSource.getConfigBean().startValue.equals(StartValues.DATE);
  }

  @VisibleForTesting
  static PostgresWalRecord passesTableFilter(PostgresWalRecord postgresWalRecord, PostgresCDCSource postgresCDCSource) {

    if(!hasTableFilter(postgresCDCSource)) {
      return postgresWalRecord;
    }

    List<Field> changes = postgresWalRecord.getChanges();
    List<Field> filteredChanges = new ArrayList<Field>();

    for (Field change : changes) {
      String table = PostgresWalRecord.getTableFromChangeMap(change.getValueAsMap());
      String schema = PostgresWalRecord.getSchemaFromChangeMap(change.getValueAsMap());
      SchemaAndTable changeSchemaAndTable = new SchemaAndTable(schema, table);
      // If the change's schema/table pair is not in the valid range, we filter out (return false)
      if (getSchemasAndTables(postgresCDCSource).contains(changeSchemaAndTable)) {
        filteredChanges.add(change);
      }
    }
    if (filteredChanges == null || filteredChanges.isEmpty()) {
      return null;
    }
    return new PostgresWalRecord(postgresWalRecord, Field.create(filteredChanges));
  }

  @VisibleForTesting
  static PostgresWalRecord passesStartValueFilter(PostgresWalRecord postgresWalRecord, PostgresCDCSource postgresCDCSource) {
    /*
        This is the date that shows up in the CDC record:
        "2018-07-06 12:57:33.383899-07";
      -07 refers to Pacific Summer Time.
      This won't parse. It will parse if there is "-07:00" instead.
      Since 2018-07-06 12:57:33.383899 is static (26 chars), if we see 29chars we assume
      that the :xx is missing for timezones that are +/- 30min differences.
     */
    if(!hasStartValueFilter(postgresCDCSource)) {
      return postgresWalRecord;
    }
    String changeTimeStr = postgresWalRecord.getTimestamp();
    String dateFormat = "yyyy-MM-dd HH:mm:ss.SSSSSSXXX";
    if (changeTimeStr.length() == 29) {
      changeTimeStr += ":00";
    }
    if (changeTimeStr.length() == 28) {
      changeTimeStr += ":00";
      dateFormat = "yyyy-MM-dd HH:mm:ss.SSSSSXXX";
    }
    if (changeTimeStr.length() == 27) {
      changeTimeStr += ":00";
      dateFormat = "yyyy-MM-dd HH:mm:ss.SSSSXXX";
    }
    if (changeTimeStr.length() == 26) {
      changeTimeStr += ":00";
      dateFormat = "yyyy-MM-dd HH:mm:ss.SSSXXX";
    }
    ZonedDateTime zonedDateTime = ZonedDateTime.parse(changeTimeStr, DateTimeFormatter.ofPattern(dateFormat));
    LocalDateTime localDateTime = postgresCDCSource.getStartDate();
    ZoneOffset startZoneOffset = postgresCDCSource.getZoneId().getRules().getOffset(localDateTime);
    ZonedDateTime startDate = OffsetDateTime.of(postgresCDCSource.getStartDate(), startZoneOffset).toZonedDateTime();
    //Compare to configured startDate. If its less than start, then ignore
    if (zonedDateTime.compareTo(startDate) < 0) {
      return null;
    }
    return postgresWalRecord;
  }

  @VisibleForTesting
  static PostgresWalRecord passesOperationFilter(PostgresWalRecord postgresWalRecord, PostgresCDCSource postgresCDCSource) {
    List<PostgresChangeTypeValues> configuredChangeTypes = postgresCDCSource
        .getConfigBean()
        .postgresChangeTypes;

    List<String> changeTypes = new ArrayList<String>();
    for (PostgresChangeTypeValues configuredChangeType : configuredChangeTypes) {
      changeTypes.add(configuredChangeType.getLabel());
    }

    List<Field> changes = postgresWalRecord.getChanges();
    List<Field> filteredChanges = new ArrayList<Field>();

    for (Field change: changes) {
      String changeType = PostgresWalRecord.getTypeFromChangeMap(change.getValueAsMap());
      if (changeTypes.contains(changeType)) {
        filteredChanges.add(change);
      }
    }

    if (filteredChanges == null || filteredChanges.isEmpty()) {
      return null;
    }
    return new PostgresWalRecord(postgresWalRecord, Field.create(filteredChanges));
  }

}
