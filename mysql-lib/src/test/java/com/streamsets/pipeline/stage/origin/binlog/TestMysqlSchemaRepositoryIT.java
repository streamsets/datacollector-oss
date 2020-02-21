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
package com.streamsets.pipeline.stage.origin.binlog;

import com.streamsets.pipeline.stage.origin.mysql.MysqlSchemaRepository;
import com.streamsets.pipeline.stage.origin.mysql.schema.Column;
import com.streamsets.pipeline.stage.origin.mysql.schema.DatabaseAndTable;
import com.streamsets.pipeline.stage.origin.mysql.schema.MysqlType;
import com.streamsets.pipeline.stage.origin.mysql.schema.Table;
import com.streamsets.pipeline.stage.origin.mysql.schema.TableImpl;
import com.streamsets.pipeline.stage.origin.binlog.utils.Utils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;

import javax.sql.DataSource;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TestMysqlSchemaRepositoryIT {
  @ClassRule
  public static MySQLContainer mysql = new MySQLContainer("mysql:5.6").withConfigurationOverride("mysql_gtid_on");

  private static DataSource ds;

  @BeforeClass
  public static void connect() throws Exception {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(mysql.getJdbcUrl());
    hikariConfig.setUsername(mysql.getUsername());
    hikariConfig.setPassword(mysql.getPassword());
    hikariConfig.setAutoCommit(false);
    ds = new HikariDataSource(hikariConfig);
    Utils.runInitScript("schema.sql", ds);
  }

  @Test
  public void shouldReturnAbsentForNonExistingTable() {
    MysqlSchemaRepository repo = new MysqlSchemaRepository(ds);
    assertThat(repo.getTable(new DatabaseAndTable(mysql.getUsername(), "non_existing")).isPresent(), is(false));
  }

  @Test
  public void shouldReturnSchema() {
    Table expected = new TableImpl(mysql.getUsername(), "ALL_TYPES", Arrays.asList(
        new Column("_decimal", MysqlType.DECIMAL),
        new Column("_tinyint", MysqlType.TINY_INT),
        new Column("_smallint", MysqlType.SMALL_INT),
        new Column("_mediumint", MysqlType.MEDIUM_INT),
        new Column("_float", MysqlType.FLOAT),
        new Column("_double", MysqlType.DOUBLE),
        new Column("_timestamp", MysqlType.TIMESTAMP),
        new Column("_bigint", MysqlType.BIGINT),
        new Column("_int", MysqlType.INT),
        new Column("_date", MysqlType.DATE),
        new Column("_time", MysqlType.TIME),
        new Column("_datetime", MysqlType.DATETIME),
        new Column("_year", MysqlType.YEAR),
        new Column("_varchar", MysqlType.VARCHAR),
        new Column("_enum", MysqlType.ENUM),
        new Column("_set", MysqlType.SET),
        new Column("_tinyblob", MysqlType.TINY_BLOB),
        new Column("_mediumblob", MysqlType.MEDIUM_BLOB),
        new Column("_longblob", MysqlType.LONG_BLOB),
        new Column("_blob", MysqlType.BLOB),
        new Column("_text", MysqlType.TEXT),
        new Column("_tinytext", MysqlType.TINY_TEXT),
        new Column("_mediumtext", MysqlType.MEDIUM_TEXT),
        new Column("_longtext", MysqlType.LONG_TEXT)
    ));
    MysqlSchemaRepository repo = new MysqlSchemaRepository(ds);
    assertThat(repo.getTable(new DatabaseAndTable(mysql.getUsername(), "ALL_TYPES")).get(), is(expected));
  }
}
