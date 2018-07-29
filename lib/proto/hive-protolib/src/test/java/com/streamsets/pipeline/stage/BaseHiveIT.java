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
package com.streamsets.pipeline.stage;

import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import com.streamsets.testing.NetworkUtils;
import com.streamsets.testing.SingleForkNoReuseTest;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hive.service.server.HiveServer2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Base Hive Integration Test class that starts HDFS, HMS and HiveServer2.
 */
@Category(SingleForkNoReuseTest.class)
public abstract class BaseHiveIT {

  private static Logger LOG = LoggerFactory.getLogger(BaseHiveIT.class);

  private static final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";

  private static int MINICLUSTER_BOOT_RETRY = Integer.getInteger("basehiveit.boot.retry", 5);
  private static int MINICLUSTER_BOOT_SLEEP = Integer.getInteger("basehiveit.boot.sleep", 5000);

  // Mini cluster instances
  private static String confDir = "target/" + UUID.randomUUID().toString();
  private static MiniDFSCluster miniDFS;
  private static MiniMRClientCluster miniMR;
  private static ExecutorService hiveMetastoreExecutor = Executors.newSingleThreadExecutor();
  private static HiveServer2 hiveServer2;
  private static Connection hiveConnection;
  private static Connection getHiveConnection() {
    return hiveConnection;
  }

  // Network configuration
  private static final String HOSTNAME = "localhost";
  private static int METASTORE_PORT;
  private static int HIVE_SERVER_PORT;
  private static final String WAREHOUSE_DIR = "/user/hive/warehouse";
  private static final String EXTERNAL_DIR = "/user/hive/external";

  private static boolean isHiveInitialized = false;

  /**
   * Start all required mini clusters.
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    // Conf dir
    if (!new File(confDir).mkdirs()) {
      fail("Failed to create config directories.");
    }

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
    miniMR = MiniMRClientClusterFactory.create(BaseHiveIT.class, 1, conf);
    writeConfiguration(miniMR.getConfig(), confDir + "/core-site.xml");
    writeConfiguration(miniMR.getConfig(), confDir + "/hdfs-site.xml");
    writeConfiguration(miniMR.getConfig(), confDir + "/mapred-site.xml");
    writeConfiguration(miniMR.getConfig(), confDir + "/yarn-site.xml");

    // Configuration for both HMS and HS2
    METASTORE_PORT = NetworkUtils.getRandomPort();
    HIVE_SERVER_PORT = NetworkUtils.getRandomPort();
    final HiveConf hiveConf = new HiveConf(miniDFS.getConfiguration(0), HiveConf.class);
    hiveConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:;databaseName=target/metastore_db;create=true");
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, Utils.format("thrift://{}:{}", HOSTNAME, METASTORE_PORT));
    hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, "localhost");
    hiveConf.set("org.jpox.autoCreateSchema", "true");
    hiveConf.set("datanucleus.schema.autoCreateTables", "true");
    hiveConf.set("hive.metastore.schema.verification", "false");
    hiveConf.setInt(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, HIVE_SERVER_PORT);

    // Hive metastore
    Callable<Void> metastoreService = () -> {
      try {
        HiveMetaStore.startMetaStore(METASTORE_PORT, ShimLoader.getHadoopThriftAuthBridge(), hiveConf);
        while(true);
      } catch (Throwable e) {
        throw new Exception("Error starting metastore", e);
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
    hiveConnection = HiveMetastoreUtil.getHiveConnection(
      getHiveJdbcUrl(),
      HadoopSecurityUtil.getLoginUser(conf),
      Collections.emptyList()
    );

    // And finally we're initialized
    isHiveInitialized = true;
  }

  @Before
  public void assumeThatHiveStarted() {
    Assume.assumeTrue("Hive have not started, skipping the test.", isHiveInitialized);
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
      if(hiveConnection != null) {
        hiveConnection.close();
      }
    } catch (SQLException e) {
      LOG.warn("Error while closing connection", e);
    }
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }

    hiveMetastoreExecutor.shutdownNow();

    if(miniMR != null) {
      miniMR.stop();
      miniMR = null;
    }

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

  public static String getDefaultFsUri() {
    return Utils.format("hdfs://{}:{}", HOSTNAME, miniDFS.getNameNodePort());
  }

  public static FileSystem getDefaultFileSystem() throws IOException{
    return miniDFS.getFileSystem();
  }

  public static String getDefaultWareHouseDir() {
    return WAREHOUSE_DIR;
  }

  public static String getExternalWareHouseDir() {
    return EXTERNAL_DIR;
  }

  public static String getConfDir() {
    return confDir;
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
        //Only after hive 2.2.0 we can do show views, so for now doing a drop view if exists on all tables.
        //to delete views as well as drop table on a view will fail.
        dropStatement.executeUpdate(Utils.format("drop view if exists default.`{}`", table));
        dropStatement.executeUpdate(Utils.format("drop table default.`{}`", table));
      }
    }

    // Filesystem clean up
    miniDFS.getFileSystem().delete(new Path(WAREHOUSE_DIR), true);
    miniDFS.getFileSystem().delete(new Path(EXTERNAL_DIR), true);
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
    String sql = HiveQueryExecutor.buildShowTableQuery(name);
    LOG.debug("Executing SQL: {}", sql);
    try (
        Statement statement = hiveConnection.createStatement();
        ResultSet rs = statement.executeQuery(sql)
    ){
      Assert.assertTrue("Table " + name + " doesn't exists.", rs.next());
    } catch (Exception e) {
      LOG.error("Can't verify existence of table", e);
      fail("Can't verify existence of table " + name);
    }
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
  @SafeVarargs
  public static void assertTableStructure(String table, final Pair<String, Integer>... columns) throws Exception {
    assertTableExists(table);
    assertQueryResult(Utils.format("select * from {}", table), new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs, columns);
      }
    });
  }

  /**
   * Assert that location of given table is where expected.
   */
  public static void assertTableLocation(String table, String location) throws Exception {
    assertTableExists(table);

    String sql = Utils.format("desc formatted {}", table);
    LOG.debug("Executing SQL: {}", sql);
    String actualLocation = null;
    try (
        Statement statement = hiveConnection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
    ) {
      while(rs.next()) {
        if(rs.getString("col_name").trim().equals("Location:")) {
          actualLocation =  HiveMetastoreUtil.stripHdfsHostAndPort(rs.getString("data_type"));
          break;
        }
      }

      if(actualLocation == null) {
        fail("Can't find 'Location' tag in the response");
      }

      assertEquals(location, actualLocation);
    } catch (Exception e) {
      LOG.error("Can't verify existence of table", e);
      fail("Can't verify location of table " + table);
    }
  }
}
