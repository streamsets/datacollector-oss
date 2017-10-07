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
package com.streamsets.pipeline.stage.destination.hdfs;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.hdfs.util.HdfsTargetUtil;
import com.streamsets.pipeline.stage.destination.hdfs.writer.RecordWriter;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.attribute.PosixFilePermission;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

public class BaseHdfsTargetIT {
  private static MiniDFSCluster miniDFS;
  private static UserGroupInformation fooUgi;

  @BeforeClass
  public static void setUpClass() throws Exception {
    //setting some dummy kerberos settings to be able to test a mis-setting
    System.setProperty("java.security.krb5.realm", "foo");
    System.setProperty("java.security.krb5.kdc", "localhost:0");

    File minidfsDir = new File("target/minidfs").getAbsoluteFile();
    if (!minidfsDir.exists()) {
      Assert.assertTrue(minidfsDir.mkdirs());
    }
    Set<PosixFilePermission> set = new HashSet<PosixFilePermission>();
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
  }

  @AfterClass
  public static void cleanUpClass() throws IOException {
    if (miniDFS != null) {
      miniDFS.shutdown();
      miniDFS = null;
    }
  }

  @Before
  public void setUpTest() {
    UserGroupInformation.setConfiguration(new Configuration());
  }

  @After
  public void cleanUpTest() {
    UserGroupInformation.setConfiguration(new Configuration());
  }

  static class ForTestHdfsTarget extends HdfsDTarget {
    @Override
    protected Target createTarget() {
      return new HdfsTarget(hdfsTargetConfigBean) {
        @Override
        public void write(Batch batch) throws StageException {
        }
      };
    }
  }

  @Test
  public void getGetHdfsConfiguration() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
                                                               OnRecordError.TO_ERROR, null));
      Assert.assertNotNull(target.getHdfsConfiguration());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testGetHdfsConfigurationWithResources() throws Exception {
    File resourcesDir = new File("target", UUID.randomUUID().toString());
    File fooDir = new File(resourcesDir, "foo");
    Assert.assertTrue(fooDir.mkdirs());
    Files.write("<configuration><property><name>xx</name><value>XX</value></property></configuration>",
                new File(fooDir, "core-site.xml"), StandardCharsets.UTF_8);
    Files.write("<configuration><property><name>yy</name><value>YY</value></property></configuration>",
                new File(fooDir, "hdfs-site.xml"), StandardCharsets.UTF_8);
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.hdfsTargetConfigBean.hdfsConfDir = fooDir.getName();
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
                                                               OnRecordError.TO_ERROR,
                                                               resourcesDir.getAbsolutePath()));
      Configuration conf = target.getHdfsConfiguration();
      Assert.assertEquals("XX", conf.get("xx"));
      Assert.assertEquals("YY", conf.get("yy"));
    } finally {
      target.destroy();
    }

    // Provide HDFS config dir as an absolute path
    File absoluteFilePath = new File(new File("target", UUID.randomUUID().toString()), "foo");
    Assert.assertTrue(absoluteFilePath.mkdirs());
    Files.write("<configuration><property><name>zz</name><value>ZZ</value></property></configuration>",
      new File(absoluteFilePath, "core-site.xml"), StandardCharsets.UTF_8);
    Files.write("<configuration><property><name>aa</name><value>AA</value></property></configuration>",
      new File(absoluteFilePath, "hdfs-site.xml"), StandardCharsets.UTF_8);

    dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.hdfsTargetConfigBean.hdfsConfDir = absoluteFilePath.getAbsolutePath();
    target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
                                                               OnRecordError.TO_ERROR,
                                                               resourcesDir.getAbsolutePath()));
      Configuration conf = target.getHdfsConfiguration();
      Assert.assertEquals("ZZ", conf.get("zz"));
      Assert.assertEquals("AA", conf.get("aa"));
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testHdfsConfigs() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
                                                               OnRecordError.TO_ERROR, null));
      Assert.assertEquals("X", target.getHdfsConfiguration().get("x"));
    } finally {
      target.destroy();
    }
  }

  private void testDir(String dir, String lateDir, boolean ok) throws StageException {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.hdfsTargetConfigBean.dirPathTemplate = dir;
    dTarget.hdfsTargetConfigBean.lateRecordsDirPathTemplate = lateDir;
    dTarget.hdfsTargetConfigBean.lateRecordsAction = LateRecordsAction.SEND_TO_LATE_RECORDS_FILE;
    dTarget.hdfsTargetConfigBean.hdfsUser = "foo";
    dTarget.hdfsTargetConfigBean.hdfsPermissionCheck = true;
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, target)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    try {
      List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
      Assert.assertEquals((ok) ? 0 : 1 , issues.size());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testDirValidity() throws Exception {
    //valid dirs
    testDir("/foo", "/foo", true);
    testDir("/foo/${YY()}", "/foo/bar-${YY()}", true);

    //non absolute dir
    testDir("foo", "/foo", false);
    testDir("/foo", "foo", false);

    FileSystem fs = miniDFS.getFileSystem();
    fs.mkdirs(new Path("/bar"));

    //no permissions
    testDir("/bar/foo", "/foo", false);
    testDir("/foo", "/bar/foo", false);
    testDir("/bar/foo/${YY()}", "/foo/${YY()}", false);
    testDir("/foo/${YY()}", "/bar/foo/${YY()}", false);
  }

  @Test
  public void testNoCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      Target.Context context = ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
        OnRecordError.TO_ERROR, null);
      target.init(null, context);
      Assert.assertNull(target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testDefinedCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.hdfsTargetConfigBean.compression = CompressionMode.GZIP;
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      Target.Context context = ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
        OnRecordError.TO_ERROR, null);
      target.init(null, context);
      Assert.assertEquals(CompressionMode.GZIP.getCodec(), target.getCompressionCodec().getClass());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testSDCSnappyCompressionCodec() throws Exception {
    Assert.assertNotNull(CompressionMode.SNAPPY.getCodec().getField("SDC"));
    CompressionCodec codec = CompressionMode.SNAPPY.getCodec().newInstance();
    Assert.assertNotNull(codec.getCompressorType().getField("SDC"));
    Assert.assertNotNull(codec.getDecompressorType().getField("SDC"));
  }

  @Test
  public void testCustomCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.hdfsTargetConfigBean.compression = CompressionMode.OTHER;
    dTarget.hdfsTargetConfigBean.otherCompression = DeflateCodec.class.getName();
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      Target.Context context = ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
        OnRecordError.TO_ERROR, null);
      target.init(null, context);
      Assert.assertEquals(DeflateCodec.class, target.getCompressionCodec().getClass());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testInvalidCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.hdfsTargetConfigBean.compression = CompressionMode.OTHER;
    dTarget.hdfsTargetConfigBean.otherCompression = String.class.getName();
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    Target.Context context = ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
      OnRecordError.TO_ERROR, null);
    Assert.assertEquals(1, target.init(null, context).size());
  }

  @Test
  public void testUnknownCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.hdfsTargetConfigBean.compression = CompressionMode.OTHER;
    dTarget.hdfsTargetConfigBean.otherCompression = "foo";
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    Target.Context context = ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
      OnRecordError.TO_ERROR, null);
    Assert.assertEquals(1, target.init(null, context).size());
  }

  @Test
  public void testLateRecordsLimitSecs() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
                                                               OnRecordError.TO_ERROR, null));
      target.getBatchTime();
      Assert.assertEquals(3600, target.getLateRecordLimitSecs());
    } finally {
      target.destroy();
    }

    dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.hdfsTargetConfigBean.lateRecordsLimit = "${1 * MINUTES}";
    target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
                                                               OnRecordError.TO_ERROR, null));
      Assert.assertEquals(60, target.getLateRecordLimitSecs());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testTimeDriverElEvalNow() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
                                                               OnRecordError.TO_ERROR, null));
      target.getBatchTime();
      Record record = RecordCreator.create();
      target.write((Batch)null); //forcing a setBatchTime()
      Date now = target.setBatchTime();
      Assert.assertEquals(now, target.getRecordTime(record));
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testTimeDriverElEvalRecordValue() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.hdfsTargetConfigBean.timeDriver = "${record:value('/')}";
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
                                                               OnRecordError.TO_ERROR, null));
      Date date = new Date();
      Record record = RecordCreator.create();
      record.set(Field.createDatetime(date));
      Thread.sleep(1); // so batch time is later than date for sure
      target.write((Batch) null); //forcing a setBatchTime()
      Assert.assertEquals(date, target.getRecordTime(record));
    } finally {
      target.destroy();
    }
  }

  private void testUser(String user, String expectedOwner) throws Exception {
    final Path dir = new Path("/" + UUID.randomUUID().toString());
    if (user.isEmpty()) {
      miniDFS.getFileSystem().mkdirs(dir);
    } else {
      fooUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          miniDFS.getFileSystem().mkdirs(dir);
          return null;
        }
      });
    }

    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .hdfsUri(miniDFS.getFileSystem().getUri().toString())
      .hdfsUser(user)
      .dirPathTemplate(dir.toString())
      .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
      .lateRecordsDirPathTemplate(dir.toString())
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();
    try {
      Date date = new Date();
      Record record = RecordCreator.create();
      record.set(Field.createDatetime(date));
      runner.runWrite(Arrays.asList(record));
    } finally {
      runner.runDestroy();
    }
    FileStatus[] status = miniDFS.getFileSystem().listStatus(dir);
    Assert.assertEquals(1, status.length);
    Assert.assertEquals(expectedOwner, status[0].getOwner());
  }


  @Test
  public void testRegularUser() throws Exception {
    testUser("", System.getProperty("user.name"));
  }

  @Test
  public void testProxyUser() throws Exception {
    testUser("foo", "foo");
  }

  @Test
  public void testCustomFrequency() throws Exception {
    final Path dir = new Path("/" + UUID.randomUUID().toString());
      miniDFS.getFileSystem().mkdirs(dir);

    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .hdfsUri(miniDFS.getFileSystem().getUri().toString())
      .hdfsUser("")
      .dirPathTemplate(dir.toString() + "/${YY()}${MM()}${DD()}${hh()}${every(10, mm())}")
      .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
      .lateRecordsDirPathTemplate(dir.toString())
      .timeDriver("${record:value('/')}")
      .lateRecordsLimit("${30 * MINUTES}")
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    runner.runInit();
    try {
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.setTime(new Date());

      // 3 records which are exactly 10 mins apart, they always fall in 2 frequency ranges
      Record record1 = RecordCreator.create();
      record1.set(Field.createDatetime(calendar.getTime()));
      calendar.add(Calendar.MINUTE, - 5);
      Record record2 = RecordCreator.create();
      record2.set(Field.createDatetime(calendar.getTime()));
      calendar.add(Calendar.MINUTE, - 5);
      Record record3 = RecordCreator.create();
      record3.set(Field.createDatetime(calendar.getTime()));

      runner.runWrite(Arrays.asList(record1, record2, record3));
    } finally {
      runner.runDestroy();
    }
    FileStatus[] status = miniDFS.getFileSystem().listStatus(dir);
    Assert.assertEquals(2, status.length);
  }

  @Test
  public void testWriteBatch() throws Exception{
    final Path dir = new Path("/" + UUID.randomUUID().toString());

    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .hdfsUri(miniDFS.getFileSystem().getUri().toString())
      .hdfsUser("")
      .dirPathTemplate(dir.toString())
      .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
      .lateRecordsDirPathTemplate(dir.toString())
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    runner.runInit();
    Date date = new Date();
    Date recordDate = new Date(date.getTime());
    Record record1 = RecordCreator.create();
    record1.set(Field.create("a"));
    Record record2 = RecordCreator.create();
    record2.set(Field.create("z"));
    runner.runWrite(Arrays.asList(record1, record2));
    RecordWriter recordWriter =
      ((HdfsTarget)( runner.getStage())).getCurrentWriters().get(date, recordDate, record1);
    Assert.assertEquals(2, recordWriter.getRecords());
    Assert.assertTrue(recordWriter.getLength() > 2);
    Assert.assertTrue(recordWriter.isTextFile());
  }

  private void configure(HdfsDTarget target) {

    HdfsTargetConfigBean hdfsTargetConfigBean = new HdfsTargetConfigBean();
    hdfsTargetConfigBean.hdfsUri = miniDFS.getURI().toString();
    hdfsTargetConfigBean.hdfsConfigs = ImmutableList.of(
      new HadoopConfigBean("x", "X")
    );
    hdfsTargetConfigBean.timeZoneID = "UTC";
    hdfsTargetConfigBean.dirPathTemplate = "/${YYYY()}";
    hdfsTargetConfigBean.lateRecordsDirPathTemplate = "";
    hdfsTargetConfigBean.compression = CompressionMode.NONE;
    hdfsTargetConfigBean.otherCompression = null;
    hdfsTargetConfigBean.timeDriver = "${time:now()}";
    hdfsTargetConfigBean.lateRecordsLimit = "3600";
    hdfsTargetConfigBean.dataFormat = DataFormat.DELIMITED;
    hdfsTargetConfigBean.hdfsUser = "";

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.csvFileFormat = CsvMode.CSV;
    dataGeneratorFormatConfig.csvHeader = CsvHeader.IGNORE_HEADER;
    dataGeneratorFormatConfig.charset = "UTF-8";
    hdfsTargetConfigBean.dataGeneratorFormatConfig = dataGeneratorFormatConfig;

    target.hdfsTargetConfigBean = hdfsTargetConfigBean;
  }
}
