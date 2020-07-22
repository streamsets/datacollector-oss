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

package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeAction;
import com.streamsets.pipeline.stage.config.SapHanaHikariPoolConfigBean;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SapHanaSource extends JdbcSource{

  private Map<String,String> clientInfo;
  private SapHanaHikariPoolConfigBean configBean;

  public SapHanaSource(
      boolean isIncrementalMode,
      String query,
      String initialOffset,
      String offsetColumn,
      boolean disableValidation,
      int txnMaxSize,
      JdbcRecordType jdbcRecordType,
      CommonSourceConfigBean commonSourceConfigBean,
      boolean createJDBCNsHeaders,
      String jdbcNsHeaderPrefix,
      SapHanaHikariPoolConfigBean hikariConfigBean,
      UnknownTypeAction unknownTypeAction,
      long queryInterval) {

    super(
        isIncrementalMode,
        query,
        initialOffset,
        offsetColumn,
        disableValidation,
        "",
        txnMaxSize,
        jdbcRecordType,
        commonSourceConfigBean,
        createJDBCNsHeaders,
        jdbcNsHeaderPrefix,
        hikariConfigBean,
        unknownTypeAction,
        queryInterval
    );
    configBean = hikariConfigBean;
  }

  @Override
  protected Connection getProduceConnection() throws SQLException {
    Connection produceConnection = super.getProduceConnection();
    clientInfo = new HashMap<>();
    if(configBean.pullClientInfoSAP) {
      //get client info property
      Properties ci = produceConnection.getClientInfo();
      if (ci != null) {
        for (Map.Entry<Object, Object> element :  ci.entrySet()){
          String key = (String) element.getKey();
          String val = (String) element.getValue();
          clientInfo.put(key, val);
        }
      }
    }
    return produceConnection;
  }


  @Override
  protected Record processRow(ResultSet resultSet, long rowCount) throws SQLException {

    Record record = super.processRow(resultSet, rowCount);
    if(clientInfo!=null) {
      for (String key : clientInfo.keySet()) {
        record.getHeader().setAttribute("SapHANA."+key,clientInfo.get(key));
      }
    }
    return record;
  }

  @Override
  protected Statement getStatement(Connection connection, int resultSetType, int resultSetConcurrency) throws SQLException {
    return connection.prepareStatement(getPreparedQuery(), resultSetType, resultSetConcurrency);
  }

  @Override
  protected ResultSet executeQuery(Statement statement, String preparedQuery) throws SQLException {
    PreparedStatement preparedStatement = (PreparedStatement) statement;
    return preparedStatement.executeQuery();
  }
}
