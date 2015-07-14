/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.google.common.io.Files;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

public class TestBaseHdfsTarget {
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

  private void configure(HdfsDTarget target) {
    target.hdfsUri = miniDFS.getURI().toString();
    target.hdfsConfigs = new HashMap<>();
    target.hdfsConfigs.put("x", "X");
    target.timeZoneID = "UTC";
    target.dirPathTemplate = "${YYYY()}";
    target.lateRecordsDirPathTemplate = "";
    target.compression = CompressionMode.NONE;
    target.otherCompression = null;
    target.timeDriver = "${time:now()}";
    target.lateRecordsLimit = "3600";
    target.dataFormat = DataFormat.DELIMITED;
    target.csvFileFormat = CsvMode.CSV;
    target.csvHeader = CsvHeader.IGNORE_HEADER;
    target.charset = "UTF-8";
    target.hdfsUser = "";
  }

  static class ForTestHdfsTarget extends HdfsDTarget {
    @Override
    protected Target createTarget() {
      return new HdfsTarget(
          hdfsUri,
          hdfsUser,
          hdfsKerberos,
          hdfsConfDir,
          hdfsConfigs,
          uniquePrefix,
          dirPathTemplate,
          timeZoneID,
          timeDriver,
          maxRecordsPerFile,
          maxFileSize,
          compression,
          otherCompression,
          fileType,
          keyEl,
          seqFileCompressionType,
          lateRecordsLimit,
          lateRecordsAction,
          lateRecordsDirPathTemplate,
          dataFormat,
          charset,
          csvFileFormat,
          csvHeader,
          csvReplaceNewLines,
          jsonMode,
          textFieldPath,
          textEmptyLineIfNull,
          null
      ) {
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
      target.validateConfigs(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
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
    dTarget.hdfsConfDir = fooDir.getName();
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.validateConfigs(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
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
    dTarget.hdfsConfDir = absoluteFilePath.getAbsolutePath();
    target = (HdfsTarget) dTarget.createTarget();
    try {
      target.validateConfigs(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
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
      target.validateConfigs(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
        OnRecordError.TO_ERROR, null));
      Assert.assertEquals("X", target.getHdfsConfiguration().get("x"));
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testNoCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      Target.Context context = ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
        OnRecordError.TO_ERROR, null);
      target.validateConfigs(null, context);
      Assert.assertNull(target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testDefinedCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.compression = CompressionMode.GZIP;
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      Target.Context context = ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
        OnRecordError.TO_ERROR, null);
      target.validateConfigs(null, context);
      Assert.assertEquals(CompressionMode.GZIP.getCodec(), target.getCompressionCodec().getClass());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testCustomCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.compression = CompressionMode.OTHER;
    dTarget.otherCompression = DeflateCodec.class.getName();
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      Target.Context context = ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
        OnRecordError.TO_ERROR, null);
      target.validateConfigs(null, context);
      Assert.assertEquals(DeflateCodec.class, target.getCompressionCodec().getClass());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testInvalidCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.compression = CompressionMode.OTHER;
    dTarget.otherCompression = String.class.getName();
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    Target.Context context = ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
      OnRecordError.TO_ERROR, null);
    Assert.assertEquals(1, target.validateConfigs(null, context).size());
  }

  @Test
  public void testUnknownCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.compression = CompressionMode.OTHER;
    dTarget.otherCompression = "foo";
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    Target.Context context = ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
      OnRecordError.TO_ERROR, null);
    Assert.assertEquals(1, target.validateConfigs(null, context).size());
  }

  @Test
  public void testLateRecordsLimitSecs() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.validateConfigs(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
        OnRecordError.TO_ERROR, null));
      target.getBatchTime();
      Assert.assertEquals(3600, target.getLateRecordLimitSecs());
    } finally {
      target.destroy();
    }

    dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.lateRecordsLimit = "${1 * MINUTES}";
    target = (HdfsTarget) dTarget.createTarget();
    try {
      target.validateConfigs(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
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
      target.validateConfigs(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
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
    dTarget.timeDriver = "${record:value('/')}";
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.validateConfigs(null, ContextInfoCreator.createTargetContext(HdfsDTarget.class, "n", false,
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
    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addConfiguration("hdfsUri", miniDFS.getFileSystem().getUri().toString())
        .addConfiguration("hdfsUser", user)
        .addConfiguration("hdfsKerberos", false)
        .addConfiguration("hdfsConfDir", "")
        .addConfiguration("hdfsConfigs", new HashMap<>())
        .addConfiguration("uniquePrefix", "foo")
        .addConfiguration("dirPathTemplate", dir.toString())
        .addConfiguration("timeZoneID", "UTC")
        .addConfiguration("fileType", HdfsFileType.TEXT)
        .addConfiguration("keyEl", "${uuid()}")
        .addConfiguration("compression", CompressionMode.NONE)
        .addConfiguration("seqFileCompressionType", HdfsSequenceFileCompressionType.BLOCK)
        .addConfiguration("maxRecordsPerFile", 1)
        .addConfiguration("maxFileSize", 1)
        .addConfiguration("timeDriver", "${record:value('/')}")
        .addConfiguration("lateRecordsLimit", "${30 * MINUTES}")
        .addConfiguration("lateRecordsAction", LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
        .addConfiguration("lateRecordsDirPathTemplate", dir.toString())
        .addConfiguration("dataFormat", DataFormat.SDC_JSON)
        .addConfiguration("csvFileFormat", null)
        .addConfiguration("csvReplaceNewLines", false)
        .addConfiguration("charset", "UTF-8")
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
    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addConfiguration("hdfsUri", miniDFS.getFileSystem().getUri().toString())
        .addConfiguration("hdfsUser", "")
        .addConfiguration("hdfsKerberos", false)
        .addConfiguration("hdfsConfDir", "")
        .addConfiguration("hdfsConfigs", new HashMap<>())
        .addConfiguration("uniquePrefix", "foo")
        .addConfiguration("dirPathTemplate", dir.toString() + "/${YY()}${MM()}${DD()}${hh()}${every(10, mm())}")
        .addConfiguration("timeZoneID", "UTC")
        .addConfiguration("fileType", HdfsFileType.TEXT)
        .addConfiguration("keyEl", "${uuid()}")
        .addConfiguration("compression", CompressionMode.NONE)
        .addConfiguration("seqFileCompressionType", HdfsSequenceFileCompressionType.BLOCK)
        .addConfiguration("maxRecordsPerFile", 1)
        .addConfiguration("maxFileSize", 1)
        .addConfiguration("timeDriver", "${record:value('/')}")
        .addConfiguration("lateRecordsLimit", "${30 * MINUTES}")
        .addConfiguration("lateRecordsAction", LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
        .addConfiguration("lateRecordsDirPathTemplate", dir.toString())
        .addConfiguration("dataFormat", DataFormat.SDC_JSON)
        .addConfiguration("csvFileFormat", null)
        .addConfiguration("csvReplaceNewLines", false)
        .addConfiguration("charset", "UTF-8")
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


}
