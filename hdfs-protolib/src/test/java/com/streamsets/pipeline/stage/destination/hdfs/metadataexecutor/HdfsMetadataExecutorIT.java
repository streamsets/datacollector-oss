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
package com.streamsets.pipeline.stage.destination.hdfs.metadataexecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.hdfs.metadataxecutor.HdfsActionsConfig;
import com.streamsets.pipeline.stage.destination.hdfs.metadataxecutor.HdfsMetadataDExecutor;
import com.streamsets.pipeline.stage.destination.hdfs.metadataxecutor.HdfsMetadataExecutor;
import com.streamsets.pipeline.stage.destination.hdfs.metadataxecutor.HdfsConnectionConfig;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HdfsMetadataExecutorIT {

  @Rule
  public TestName name = new TestName();

  private static MiniDFSCluster miniDFS;
  private static UserGroupInformation fooUgi;
  private static FileSystem fs;
  private static String CONTENT = "This is a test content for HDFS File.";

  private static String baseDir = "target/" + HdfsMetadataExecutorIT.class.getCanonicalName() + "/";
  private static String confDir = baseDir + "conf/";

  Path inputDir;
  Path outputDir;
  Path inputPath;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // Conf dir
    new File(confDir).mkdirs();

    //setting some dummy kerberos settings to be able to test a mis-setting
    System.setProperty("java.security.krb5.realm", "foo");
    System.setProperty("java.security.krb5.kdc", "localhost:0");

    File minidfsDir = new File(baseDir, "minidfs").getAbsoluteFile();
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
    Configuration conf = new HdfsConfiguration();
    conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
    conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");
    fooUgi = UserGroupInformation.createUserForTesting("foo", new String[]{ "all"});
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    FileSystem.closeAll();
    miniDFS = new MiniDFSCluster.Builder(conf).build();
    miniDFS.getFileSystem().setPermission(new Path("/"), FsPermission.createImmutable((short)0777));
    fs = miniDFS.getFileSystem();
    writeConfiguration(miniDFS.getConfiguration(0), confDir + "core-site.xml");
    writeConfiguration(miniDFS.getConfiguration(0), confDir + "hdfs-site.xml");
  }

  @AfterClass
  public static void cleanUpClass() throws IOException {
    if (miniDFS != null) {
      miniDFS.shutdown();
      miniDFS = null;
    }
  }

  @Before
  public void setUpTest() throws IOException {
    UserGroupInformation.setConfiguration(new Configuration());
    inputDir = new Path("/" + name.getMethodName() + "/input/");
    outputDir = new Path("/" + name.getMethodName() + "/output/");

    inputPath = new Path(inputDir, "input.file");
    writeFile(inputPath, "CONTENT");
  }

  @After
  public void cleanUpTest() {
    UserGroupInformation.setConfiguration(new Configuration());
  }

  /**
   * Validate that given file exists and have expected content.
   */
  private void assertFile(Path outputPath, String content) throws IOException {
    Assert.assertTrue("Output file doesn't exists: " + outputPath, fs.exists(outputPath));
    InputStream stream = fs.open(outputPath);

    StringWriter writer = new StringWriter();
    IOUtils.copy(stream, writer, "UTF-8");

    Assert.assertEquals(content, writer.toString());
  }

  /**
   * Validate that target path have expected ownership.
   */
  private void assertOwnership(Path path, String user, String group) throws IOException {
    Assert.assertTrue("File doesn't exists: " + path, fs.exists(path));
    Assert.assertTrue("Not a file: " + path, fs.isFile(path));

    FileStatus[] statuses  = fs.listStatus(path);
    Assert.assertEquals(1, statuses.length);

    FileStatus status = statuses[0];
    Assert.assertNotNull(status);
    Assert.assertEquals(user, status.getOwner());
    Assert.assertEquals(group, status.getGroup());
  }

  /**
   * Validate that target path have expected permissions
   */
  private void assertPermissions(Path path, String perms) throws IOException {
    Assert.assertTrue("File doesn't exists: " + path, fs.exists(path));
    Assert.assertTrue("Not a file: " + path, fs.isFile(path));

    FileStatus[] statuses  = fs.listStatus(path);
    Assert.assertEquals(1, statuses.length);

    FileStatus status = statuses[0];
    Assert.assertNotNull(status);
    Assert.assertEquals(new FsPermission(perms), status.getPermission());
  }

  /**
   * Assert proper event for the changed file.
   */
  private void assertEvent(List<Record> events, Path expectedPath) {
    Assert.assertNotNull(events);
    Assert.assertEquals(1, events.size());

    Record event = events.get(0);
    Assert.assertNotNull(event);
    Assert.assertNotNull(event.get());
    Assert.assertEquals(Field.Type.MAP, event.get().getType());

    Field path = event.get("/filepath");
    Assert.assertNotNull(path);
    Assert.assertEquals(Field.Type.STRING, path.getType());
    Assert.assertEquals(expectedPath.toString(), path.getValueAsString());
  }

  /**
   * Write given content to given path on HDFS
   */
  private void writeFile(Path path, String content) throws IOException {
    OutputStream outputStream = fs.create(path);
    StringReader input = new StringReader(content);
    IOUtils.copy(input, outputStream);
    outputStream.close();
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
   * We have one test record for all operations
   */
  private Record getTestRecord(String outputPath) {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.MAP, ImmutableMap.builder()
      .put("path", Field.create(Field.Type.STRING, inputPath.toString()))
      .put("location", Field.create(Field.Type.STRING, outputPath))
      .put("owner", Field.create(Field.Type.STRING, "darth_vader"))
      .put("group", Field.create(Field.Type.STRING, "empire"))
      .put("perms_octal", Field.create(Field.Type.STRING, "777"))
      .put("perms_string", Field.create(Field.Type.STRING, "a-rwx"))
      .build()
    ));
    return record;
  }

  private Record getTestRecord() {
    return getTestRecord("");
  }

  @Test
  public void testMoveFile() throws Exception {
    Path outputPath = new Path(outputDir, "output.file");

    HdfsConnectionConfig conn = new HdfsConnectionConfig();
    conn.hdfsConfDir = confDir;

    HdfsActionsConfig actions = new HdfsActionsConfig();
    actions.filePath = "${record:value('/path')}";
    actions.shouldMoveFile = true;
    actions.newLocation = "${record:value('/location')}";

    HdfsMetadataExecutor executor = new HdfsMetadataExecutor(conn, actions);

    TargetRunner runner = new TargetRunner.Builder(HdfsMetadataDExecutor.class, executor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(getTestRecord(outputPath.toString())));
    assertEvent(runner.getEventRecords(), outputPath);
    runner.runDestroy();

    assertFile(outputPath, "CONTENT");
  }

  @Test
  public void testChangeOwnership() throws Exception {
    HdfsConnectionConfig conn = new HdfsConnectionConfig();
    conn.hdfsConfDir = confDir;

    HdfsActionsConfig actions = new HdfsActionsConfig();
    actions.filePath = "${record:value('/path')}";
    actions.shouldChangeOwnership = true;
    actions.newOwner = "${record:value('/owner')}";
    actions.newGroup = "${record:value('/group')}";

    HdfsMetadataExecutor executor = new HdfsMetadataExecutor(conn, actions);

    TargetRunner runner = new TargetRunner.Builder(HdfsMetadataDExecutor.class, executor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(getTestRecord()));
    assertEvent(runner.getEventRecords(), inputPath);
    runner.runDestroy();

    assertFile(inputPath, "CONTENT");
    assertOwnership(inputPath, "darth_vader", "empire");
  }

  @Test
  public void testSetPermissionsOctal() throws Exception {
    HdfsConnectionConfig conn = new HdfsConnectionConfig();
    conn.hdfsConfDir = confDir;

    HdfsActionsConfig actions = new HdfsActionsConfig();
    actions.filePath = "${record:value('/path')}";
    actions.shouldSetPermissions = true;
    actions.newPermissions = "${record:value('/perms_octal')}";

    HdfsMetadataExecutor executor = new HdfsMetadataExecutor(conn, actions);

    TargetRunner runner = new TargetRunner.Builder(HdfsMetadataDExecutor.class, executor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(getTestRecord()));
    assertEvent(runner.getEventRecords(), inputPath);
    runner.runDestroy();

    assertFile(inputPath, "CONTENT");
    assertPermissions(inputPath, "777");
  }

  @Test
  public void testSetPermissionsString() throws Exception {
    HdfsConnectionConfig conn = new HdfsConnectionConfig();
    conn.hdfsConfDir = confDir;

    HdfsActionsConfig actions = new HdfsActionsConfig();
    actions.filePath = "${record:value('/path')}";
    actions.shouldSetPermissions = true;
    actions.newPermissions = "${record:value('/perms_string')}";

    HdfsMetadataExecutor executor = new HdfsMetadataExecutor(conn, actions);

    TargetRunner runner = new TargetRunner.Builder(HdfsMetadataDExecutor.class, executor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(getTestRecord()));
    assertEvent(runner.getEventRecords(), inputPath);
    runner.runDestroy();

    assertFile(inputPath, "CONTENT");
    assertPermissions(inputPath, "777");
  }

}
