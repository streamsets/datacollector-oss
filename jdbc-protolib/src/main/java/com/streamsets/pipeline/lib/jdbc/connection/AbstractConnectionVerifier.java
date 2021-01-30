/*
 * Copyright 2021 StreamSets Inc.
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

package com.streamsets.pipeline.lib.jdbc.connection;

import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.connection.common.AbstractJdbcConnection;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class AbstractConnectionVerifier extends ConnectionVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractConnectionVerifier.class);

  protected abstract AbstractJdbcConnection getConnection();

  protected String getType() {
    return null;
  }

  protected DatabaseVendor getDatabaseVendor() {
    return null;
  }

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();

    if (getDatabaseVendor() != null
        && !DatabaseVendor.forUrl(getConnection().connectionString).equals(getDatabaseVendor())) {
      LOG.debug(JdbcErrors.JDBC_503.getMessage(), getType());
      issues.add(getContext().createConfigIssue("JDBC", "connection", JdbcErrors.JDBC_503, getType()));
    } else {
      Properties connectionProps = new Properties();
      if (getConnection().useCredentials) {
        connectionProps.put("user", getConnection().username.get());
        connectionProps.put("password", getConnection().password.get());
      }

      try (Connection conn = DriverManager.getConnection(getConnection().connectionString, connectionProps)) {
        LOG.debug("Successfully connected to the database at {}", getConnection().connectionString);
      } catch (final Exception ex) {
        LOG.debug(JdbcErrors.JDBC_00.getMessage(), getConnection().connectionString, ex.getMessage(), ex);
        issues.add(getContext().createConfigIssue("JDBC", "connection", JdbcErrors.JDBC_00, ex.toString(), ex));
      }
    }

    return issues;
  }
}
