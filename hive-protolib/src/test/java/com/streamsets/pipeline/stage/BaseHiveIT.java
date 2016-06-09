/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage;

import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.server.HiveServer2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Base Hive Integration Test class that starts HDFS, HMS and HiveServer2.
 */
public abstract class BaseHiveIT {

  private static Logger LOG = LoggerFactory.getLogger(BaseHiveIT.class);

  private static final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";

  private static int MINICLUSTER_BOOT_RETRY = Integer.getInteger("basehiveit.boot.retry", 5);
  private static int MINICLUSTER_BOOT_SLEEP = Integer.getInteger("basehiveit.boot.sleep", 1000);

  // Mini cluster instances
  private static String confDir = "target/" + UUID.randomUUID().toString();
  private static MiniDFSCluster miniDFS;
  private static ExecutorService hiveMetastoreExecutor = Executors.newSingleThreadExecutor();
  private static HiveServer2 hiveServer2;
  private static Connection hiveConnection;
  public static Connection getHiveConnection() {
    return hiveConnection;
  }

  // Network configuration
  private static final String HOSTNAME = "localhost";
  private static final int METASTORE_PORT;
  private static final int HIVE_SERVER_PORT;
  static {
    METASTORE_PORT = NetworkUtils.findAvailablePort();
    HIVE_SERVER_PORT = NetworkUtils.findAvailablePort();
  }

  //TODO: SDC-2988, expose this better.
  private static HiveQueryExecutor hiveQueryExecutor;
  public HiveQueryExecutor getHiveQueryExecutor() {
    return hiveQueryExecutor;
  }

  /**
   * Start all required mini clusters.
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    // Conf dir
    new File(confDir).mkdirs();

    // HDFS
    File minidfsDir = new File("target/minidfs").getAbsoluteFile();
    if (!minidfsDir.exists()) {
      Assert.assertTrue(minidfsDir.mkdirs());
    }
    Set<PosixFilePermission> set = new HashSet<>();
    set.add(PosixFilePermission.OWNER_EXECUTE);
    set.add(PosixFilePermission.OWNER_READ);
    set.add(PosixFilePermission.OWNER_WRITE);
    set.add(PosixFilePermission.OTHERS_READ);
    java.nio.file.Files.setPosixFilePermissions(minidfsDir.toPath(), set);
    System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, minidfsDir.getPath());
    final Configuration conf = new HdfsConfiguration();
    conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
    conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");
    miniDFS = new MiniDFSCluster.Builder(conf).build();
    miniDFS.getFileSystem().setPermission(new Path("/"), FsPermission.createImmutable((short)0777));
    writeConfiguration(miniDFS.getConfiguration(0), confDir + "/core-site.xml");
    writeConfiguration(miniDFS.getConfiguration(0), confDir + "/hdfs-site.xml");

    // Configuration for both HMS and HS2
    final HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
    hiveConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:;databaseName=target/metastore_db;create=true");
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, Utils.format("thrift://{}:{}", HOSTNAME, METASTORE_PORT));
    hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, "localhost");
    hiveConf.setInt(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, HIVE_SERVER_PORT);

    // Hive metastore
    Callable<Void> metastoreService = new Callable<Void>() {
      public Void call() throws Exception {
        try {
          HiveMetaStore.startMetaStore(METASTORE_PORT, ShimLoader.getHadoopThriftAuthBridge(), hiveConf);
          while(true);
        } catch (Throwable e) {
          throw new Exception("Error starting metastore", e);
        }
      }
    };
    hiveMetastoreExecutor.submit(metastoreService);
    NetworkUtils.waitForStartUp(HOSTNAME, METASTORE_PORT, MINICLUSTER_BOOT_RETRY, MINICLUSTER_BOOT_SLEEP);

    // HiveServer 2
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    writeConfiguration(hiveServer2.getHiveConf(), confDir + "/hive-site.xml");
    NetworkUtils.waitForStartUp(HOSTNAME, HIVE_SERVER_PORT, MINICLUSTER_BOOT_RETRY, MINICLUSTER_BOOT_SLEEP);

    // JDBC Connection to Hive
    Class.forName(HIVE_JDBC_DRIVER);
    hiveQueryExecutor = new HiveQueryExecutor(getHiveJdbcUrl(), HadoopSecurityUtil.getLoginUser(conf));
    hiveConnection = hiveQueryExecutor.getConnection();

  }

  /**
   * Write given Hadoop configuration to given file.
   */
  private static void writeConfiguration(Configuration conf, String path) throws Exception{
    File outputFile = new File(path);
    FileOutputStream outputStream = new FileOutputStream((outputFile));
    conf.writeXml(outputStream);
    outputStream.close();
  }

  /**
   * Stop all mini-clusters in the opposite order in which they were started.
   */
  @AfterClass
  public static void cleanUpClass() throws IOException {
    try {
      hiveConnection.close();
    } catch (SQLException e) {
      LOG.warn("Error while closing connection", e);
    }
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }

    hiveMetastoreExecutor.shutdownNow();

    if (miniDFS != null) {
      miniDFS.shutdown();
      miniDFS = null;
    }
  }

  /**
   * Returns HS2 JDBC URL for server started by this test case.
   */
  public static String getHiveJdbcUrl() {
    return Utils.format("jdbc:hive2://{}:{}/default;user={}", HOSTNAME, HIVE_SERVER_PORT, System.getProperty("user.name"));
  }

  public static String getDefaultFS() {
    return Utils.format("hdfs://{}:{}", HOSTNAME, miniDFS.getNameNodePort());
  }

  /**
   * Since we're reusing the same HS2 instance between tests, drop all databases and tables that were created.
   */
  @Before
  public void cleanUpHiveTables() throws Exception {
    // Metadata clean up
    try (
        Statement showDatabases = hiveConnection.createStatement();
        Statement showTables = hiveConnection.createStatement();
        Statement dropStatement = hiveConnection.createStatement();
        ResultSet databases = showDatabases.executeQuery("show databases");
        ResultSet tables = showTables.executeQuery("show tables");
    ){
      // Drop all databases except of "default" that can't be dropped
      while(databases.next()) {
        String db = databases.getString(1);
        if(!"default".equalsIgnoreCase(db)) {
          dropStatement.executeUpdate(Utils.format("drop database `{}` cascade", db));
        }
      }

      // Tables from default have to be dropped separately
      while(tables.next()) {
        String table = tables.getString(1);
        dropStatement.executeUpdate(Utils.format("drop table default.`{}`", table));
      }
    }

    // Filesystem clean up
    miniDFS.getFileSystem().delete(new Path("/user/hive/warehouse"), true);
  }

  /**
   * Runs arbitrary DDL query.
   *
   * Suitable for creating tables, partitions, ...
   */
  public void executeUpdate(String query) throws Exception {
    try (Statement statement = hiveConnection.createStatement()) {
      statement.executeUpdate(query);
    }
  }

  /**
   * Generates HiveConfigBean with pre-filled values that are connecting to HS2 started by this test case.
   */
  public static HiveConfigBean getHiveConfigBean() {
    HiveConfigBean hiveConfigBean = new HiveConfigBean();
    hiveConfigBean.confDir = confDir;
    hiveConfigBean.hiveJDBCDriver = HIVE_JDBC_DRIVER;
    hiveConfigBean.additionalConfigProperties = Collections.emptyMap();
    hiveConfigBean.hiveJDBCUrl = getHiveJdbcUrl();
    return hiveConfigBean;
  }

  /**
   * Validate that table of given name exists in HMS
   */
  public static void assertTableExists(String name) throws Exception {
    Assert.assertTrue(Utils.format("Table {} doesn't exists.", name), hiveQueryExecutor.executeShowTableQuery(name));
  }

  public static abstract class QueryValidator {
    abstract public void validateResultSet(ResultSet rs) throws Exception;
  }

  /**
   * Simple query result validation assertion.
   */
  public static void assertQueryResult(String query, QueryValidator validator) throws Exception {
    try(
        Statement statement = getHiveConnection().createStatement();
        ResultSet rs = statement.executeQuery(query);
    ) {
      validator.validateResultSet(rs);
    }
  }

  /**
   * Validate structure of the result set (column names and types).
   */
  public static void assertResultSetStructure(ResultSet rs, Pair<String, Integer>... columns) throws Exception {
    ResultSetMetaData metaData = rs.getMetaData();
    Assert.assertEquals(Utils.format("Unexpected number of columns"), columns.length, metaData.getColumnCount());
    int i = 1;
    for(Pair<String, Integer> column : columns) {
      Assert.assertEquals(Utils.format("Unexpected name for column {}", i), column.getLeft(), metaData.getColumnName(i));
      Assert.assertEquals(Utils.format("Unexpected type for column {}", i), (int)column.getRight(), metaData.getColumnType(i));
      i++;
    }
  }

  /**
   * Assert structure of given Hive table.
   */
  public static void assertTableStructure(String table, final Pair<String, Integer>... columns) throws Exception {
    assertTableExists(table);
    assertQueryResult(Utils.format("select * from {}", table), new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs, columns);
      }
    });
  }
}
