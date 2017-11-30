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
package com.streamsets.pipeline.lib.jdbc.schemawriter;

import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcStageCheckedException;
import com.zaxxer.hikari.HikariDataSource;

public class JdbcSchemaWriterFactory {
  public static JdbcSchemaWriter create(String connectionString, HikariDataSource dataSource) throws
      JdbcStageCheckedException {
    if (connectionString.startsWith(H2SchemaWriter.getConnectionPrefix())) {
      return new H2SchemaWriter(dataSource);
    } else if (connectionString.startsWith(PostgresSchemaWriter.getConnectionPrefix())) {
      return new PostgresSchemaWriter(dataSource);
    } else {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_309, connectionString);
    }
  }
}
