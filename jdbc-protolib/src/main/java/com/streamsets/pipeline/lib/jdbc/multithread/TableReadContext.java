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
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

public final class TableReadContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableReadContext.class);

  private final DatabaseVendor vendor;
  private final PreparedStatement ps;
  private final String query;
  private final ResultSet rs;
  private final boolean neverEvict;
  private int numberOfBatches;
  private long numberOfRecords;
  private final JdbcUtil jdbcUtil;

  public TableReadContext(
      DatabaseVendor vendor,
      Connection connection,
      String query,
      List<Pair<Integer, String>> paramValuesToSet,
      int fetchSize
  ) throws SQLException, StageException {
    this(vendor, connection, query, paramValuesToSet, fetchSize, false);
  }

  public TableReadContext(
      DatabaseVendor vendor,
      Connection connection,
      String query,
      List<Pair<Integer, String>> paramValuesToSet,
      int fetchSize,
      boolean neverEvict
  ) throws SQLException, StageException {
    this.vendor = vendor;
    jdbcUtil = UtilsProvider.getJdbcUtil();
    this.query = query;
    LOGGER.debug("Executing Query :{}", query);
    LOGGER.debug("Parameter Types And Values {}", paramValuesToSet);
    ps = connection.prepareStatement(query);
    ps.setFetchSize(fetchSize);
    setPreparedStParameters(paramValuesToSet);
    rs = ps.executeQuery();
    resetProcessingMetrics();
    this.neverEvict = neverEvict;
  }

  private void setPreparedStParameters(List<Pair<Integer, String>> paramValuesToSet) throws SQLException, StageException {
    for (int paramIdx = 1; paramIdx <= paramValuesToSet.size(); paramIdx++) {
      setParamVal(vendor, ps, paramIdx, paramValuesToSet.get(paramIdx-1).getLeft(), paramValuesToSet.get(paramIdx-1).getRight());
    }
  }

  private static void setParamVal(
      DatabaseVendor vendor,
      PreparedStatement ps,
      int paramIdx,
      int sqlType,
      String paramVal
  ) throws SQLException, StageException {
    Utils.checkState(
        OffsetQueryUtil.SQL_TYPE_TO_FIELD_TYPE.containsKey(sqlType) || TableContextUtil.VENDOR_PARTITIONABLE_TYPES.getOrDefault(vendor, Collections.emptySet()).contains(sqlType),
        Utils.format("Unsupported Partition Offset Type: {}", sqlType)
    );
    //All Date/Time Types are stored as long offsets
    //Parse string to get long.
    switch (sqlType) {
      case Types.TIME:
        ps.setTime(
            paramIdx,
            new java.sql.Time(Long.valueOf(paramVal))
        );
        break;
      case Types.DATE:
        ps.setDate(
            paramIdx,
            new java.sql.Date(Long.valueOf(paramVal))
        );
        break;
      case Types.TIMESTAMP:
        Timestamp ts = TableContextUtil.getTimestampForOffsetValue(paramVal);
        ps.setTimestamp(
            paramIdx,
            ts
        );
        break;
      default:
        // Oracle must be special
        switch (vendor) {
          case ORACLE:
            if(TableContextUtil.VENDOR_PARTITIONABLE_TYPES.get(DatabaseVendor.ORACLE).contains(sqlType)) {
              switch (sqlType) {
                case TableContextUtil.TYPE_ORACLE_TIMESTAMP_WITH_TIME_ZONE:
                case TableContextUtil.TYPE_ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                  // We insert the timestamp as String
                  ps.setObject(paramIdx, paramVal == null ? null : ZonedDateTime.parse(paramVal, DateTimeFormatter.ISO_OFFSET_DATE_TIME));
                  return;
                default:
                  throw new IllegalStateException(Utils.format("Unsupported type for ORACLE database: {}", sqlType));
              }
            }
            break;
          case SQL_SERVER:
            if(TableContextUtil.VENDOR_PARTITIONABLE_TYPES.get(DatabaseVendor.SQL_SERVER).contains(sqlType)) {
              if (sqlType == TableContextUtil.TYPE_SQL_SERVER_DATETIMEOFFSET) {
               // For Microsoft SQL DATETIMEOFFSET field, send the sqlType (-155) hint to the database as well
               // because otherwise SQLServer complains with - Failed to convert 'Unknown' to 'Unknown' type error
                ps.setObject(paramIdx, paramVal, sqlType);
                return;
              }
            }
            break;
        }
        ps.setObject(
            paramIdx,
            Field.create(OffsetQueryUtil.SQL_TYPE_TO_FIELD_TYPE.get(sqlType), paramVal).getValue()
        );
    }
  }

  public ResultSet getResultSet() {
    return rs;
  }

  public String getQuery() {
    return query;
  }

  public int getNumberOfBatches() {
    return numberOfBatches;
  }

  public long getNumberOfRecords() {
    return numberOfRecords;
  }

  public void resetProcessingMetrics() {
    this.numberOfBatches = 0;
    this.numberOfRecords = 0;
  }

  public void addProcessingMetrics(int batch, long records) {
    this.numberOfBatches += batch;
    this.numberOfRecords += records;
  }

  public boolean isNeverEvict() {
    return neverEvict;
  }

  public void destroy() {
    jdbcUtil.closeQuietly(rs);
    jdbcUtil.closeQuietly(ps);
  }

  public ResultSet getMoreResultSet() throws SQLException {
    ResultSet resultSet = null;
    if (ps.getMoreResults()) {
      resultSet = ps.getResultSet();
    }
    return resultSet;
  }

  public void closeResultSet() {
    try {
      if (rs != null && !rs.isClosed()) {
        rs.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Exception thrown while trying to close result set, continuing");
    }

  }
}
