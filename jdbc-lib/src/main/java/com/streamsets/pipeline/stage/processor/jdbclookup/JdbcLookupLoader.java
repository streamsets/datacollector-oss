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
package com.streamsets.pipeline.stage.processor.jdbclookup;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.cache.CacheLoader;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.jdbc.DataType;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeAction;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JdbcLookupLoader extends CacheLoader<String, List<Map<String, Field>>> {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcLookupLoader.class);
  public static final String DATE_FORMAT = "yyyy/MM/dd";
  public static final String DATETIME_FORMAT = "yyyy/MM/dd HH:mm:ss";
  static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(DATE_FORMAT);
  static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormat.forPattern(DATETIME_FORMAT);

  private final int maxClobSize;
  private final int maxBlobSize;
  private final ErrorRecordHandler errorRecordHandler;
  private final Map<String, String> columnsToFields;
  private final Map<String, String> columnsToDefaults;
  private final Map<String, DataType> columnsToTypes;
  private final DataSource dataSource;
  private final Meter selectMeter;
  private final Timer selectTimer;

  public JdbcLookupLoader(
    Stage.Context context,
    DataSource dataSource,
    Map<String, String> columnsToFields,
    Map<String, String> columnsToDefaults,
    Map<String, DataType> columnsToTypes,
    int maxClobSize,
    int maxBlobSize,
    ErrorRecordHandler errorRecordHandler
  ) {
    this.dataSource = dataSource;
    this.columnsToFields = columnsToFields;
    this.columnsToDefaults = columnsToDefaults;
    this.columnsToTypes = columnsToTypes;
    this.maxClobSize = maxClobSize;
    this.maxBlobSize = maxBlobSize;
    this.errorRecordHandler = errorRecordHandler;
    this.selectMeter = context.createMeter("Select Queries");
    this.selectTimer = context.createTimer("Select Queries");
  }

  @Override
  public List<Map<String, Field>> load(String key) throws Exception {
    return lookupValuesForRecord(key);
  }

  private List<Map<String, Field>> lookupValuesForRecord(String preparedQuery) throws StageException {
    LOG.debug("Executing SQL:  {}", preparedQuery);
    List<Map<String, Field>> lookupItems = new ArrayList<>();

    Timer.Context t = selectTimer.time();
    try (
      Connection connection = dataSource.getConnection();
       Statement stmt = connection.createStatement();
       ResultSet resultSet = stmt.executeQuery(preparedQuery)
    ) {
      // Stop timer immediately so that we're calculating only query execution time and not the processing time
      t.stop();
      t = null;

      // Process whole result set and load it to the memory
      while(resultSet.next()) {
        ResultSetMetaData md = resultSet.getMetaData();

        LinkedHashMap<String, Field> fields = JdbcUtil.resultSetToFields(resultSet,
          maxClobSize,
          maxBlobSize,
          columnsToTypes,
          errorRecordHandler,
          UnknownTypeAction.STOP_PIPELINE
        );

        int numColumns = md.getColumnCount();
        if (fields.size() != numColumns) {
          throw new OnRecordErrorException(JdbcErrors.JDBC_35, fields.size(), numColumns);
        }

        lookupItems.add(fields);
      }

      // If no lookup items were found, use defaults
      if(lookupItems.isEmpty()) {
        Map<String, Field> defaultValues = new HashMap<>();

        // Database returns no row. Use default values.
        for (String column : columnsToFields.keySet()) {
          String defaultValue = columnsToDefaults.get(column);
          DataType dataType = columnsToTypes.get(column);
          if (dataType != DataType.USE_COLUMN_TYPE) {
            Field field;
            try {
              if (dataType == DataType.DATE) {
                field = Field.createDate(DATE_FORMATTER.parseDateTime(defaultValue).toDate());
              } else if (dataType == DataType.DATETIME) {
                field = Field.createDatetime(DATETIME_FORMATTER.parseDateTime(defaultValue).toDate());
              } else {
                field = Field.create(Field.Type.valueOf(columnsToTypes.get(column).getLabel()), defaultValue);
              }
              defaultValues.put(column, field);
            } catch (IllegalArgumentException e) {
              throw new OnRecordErrorException(JdbcErrors.JDBC_03, column, defaultValue, e);
            }
          }

          if(!defaultValues.isEmpty()) {
            lookupItems.add(defaultValues);
          }
        }
      }
    } catch (SQLException e) {
      // Exception executing query
      LOG.error(JdbcErrors.JDBC_02.getMessage(), preparedQuery, e);
      throw new OnRecordErrorException(JdbcErrors.JDBC_02, preparedQuery, e.getMessage());
    } finally {
      // If the timer wasn't stopped due to exception yet, stop it now
      if(t != null) {
        t.stop();
      }
      selectMeter.mark();
    }

    return lookupItems;
  }
}
