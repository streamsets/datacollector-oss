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
import java.util.List;

public final class TableReadContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableReadContext.class);

  private final PreparedStatement ps;
  private final String query;
  private final ResultSet rs;
  private final boolean neverEvict;
  private int numberOfBatches;

  public TableReadContext(
      Connection connection,
      String query,
      List<Pair<Integer, String>> paramValuesToSet,
      int fetchSize
  ) throws SQLException, StageException {
    this(connection, query, paramValuesToSet, fetchSize, false);
  }

  public TableReadContext(
      Connection connection,
      String query,
      List<Pair<Integer, String>> paramValuesToSet,
      int fetchSize,
      boolean neverEvict
  ) throws SQLException, StageException {
    this.query = query;
    ps = connection.prepareStatement(query);
    ps.setFetchSize(fetchSize);
    setPreparedStParameters(paramValuesToSet);
    LOGGER.info("Executing Query :{}", query);
    LOGGER.debug("Parameter Types And Values {}", paramValuesToSet);
    rs = ps.executeQuery();
    numberOfBatches = 0;
    this.neverEvict = neverEvict;
  }

  private void setPreparedStParameters(List<Pair<Integer, String>> paramValuesToSet) throws SQLException, StageException {
    for (int paramIdx = 1; paramIdx <= paramValuesToSet.size(); paramIdx++) {
      setParamVal(ps, paramIdx, paramValuesToSet.get(paramIdx-1).getLeft(), paramValuesToSet.get(paramIdx-1).getRight());
    }
  }

  private static void setParamVal(
      PreparedStatement ps,
      int paramIdx,
      int sqlType,
      String paramVal
  ) throws SQLException, StageException {
    Utils.checkState(
        OffsetQueryUtil.SQL_TYPE_TO_FIELD_TYPE.containsKey(sqlType),
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

  public void setNumberOfBatches(int numberOfBatches) {
    this.numberOfBatches = numberOfBatches;
  }

  public boolean isNeverEvict() {
    return neverEvict;
  }

  public void destroy() {
    JdbcUtil.closeQuietly(rs);
    JdbcUtil.closeQuietly(ps);
  }
}
