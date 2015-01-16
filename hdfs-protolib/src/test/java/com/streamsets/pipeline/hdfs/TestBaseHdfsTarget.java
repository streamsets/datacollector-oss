/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
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
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class TestBaseHdfsTarget {
  private static MiniDFSCluster miniDFS;

  @BeforeClass
  public static void setUpClass() throws IOException {
    // setting some dummy kerberos settings to be able to test a mis-setting
    System.setProperty("java.security.krb5.realm", "foo");
    System.setProperty("java.security.krb5.kdc", "localhost:0");

    File minidfsDir = new File("target/minidfs").getAbsoluteFile();
    if (!minidfsDir.exists()) {
      Assert.assertTrue(minidfsDir.mkdirs());
    }
    System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, minidfsDir.getPath());
    Configuration conf = new HdfsConfiguration();
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    miniDFS = new MiniDFSCluster.Builder(conf).build();
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

  private void configure(BaseHdfsTarget target) {
    target.hdfsUri = miniDFS.getURI().toString();
    target.hdfsConfigs = new HashMap<String, String>();
    target.hdfsConfigs.put("x", "X");
    target.timeZoneID = "UTC";
    target.dirPathTemplate = "${YYYY}";
    target.lateRecordsDirPathTemplate = "";
    target.compression = CompressionMode.NONE.name();
    target.timeDriver = "time:now()";
    target.lateRecordsLimit = "1 * HOURS";
  }

  @Test
  public void testGetFileSystem() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    try {
      target.init();
      Assert.assertNotNull(target.getFileSystem());
      Assert.assertEquals(miniDFS.getURI(), target.getFileSystem().getUri());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testHdfsConfigs() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    try {
      target.init();
      Assert.assertEquals("X", target.getFileSystem().getConf().get("x"));
      Assert.assertEquals(miniDFS.getURI(), target.getFileSystem().getUri());
    } finally {
      target.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testKerberosWithHdfsConfigSimple() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    target.hdfsKerberos = true;
    target.kerberosKeytab = "/tmp/keytab";
    target.kerberosPrincipal = "sdc/localhost";
    try {
      target.init();
      Assert.assertEquals("X", target.getFileSystem().getConf().get("x"));
      Assert.assertEquals(miniDFS.getURI(), target.getFileSystem().getUri());
    } finally {
      target.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testConfigSimpleWithHdfsConfigKerberos() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    target.hdfsKerberos = false;
    try {
      target.init();
      Assert.assertEquals("X", target.getFileSystem().getConf().get("x"));
      Assert.assertEquals(miniDFS.getURI(), target.getFileSystem().getUri());
    } finally {
      target.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testKerberosWithHdfsConfigKerberos() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    target.hdfsKerberos = true;
    target.kerberosKeytab = "/tmp/keytab";
    target.kerberosPrincipal = "sdc/localhost";
    try {
      target.init();
      Assert.assertEquals("X", target.getFileSystem().getConf().get("x"));
      Assert.assertEquals(miniDFS.getURI(), target.getFileSystem().getUri());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testTimeZone() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    try {
      target.init();
      Assert.assertNotNull(target.getTimeZone());
      Assert.assertEquals("UTC", target.getTimeZone().getID());
    } finally {
      target.destroy();
    }
  }

  private Date getFixedDate(TimeZone tz) {
    Calendar calendar = Calendar.getInstance(tz);
    calendar.set(Calendar.YEAR, 2015);
    calendar.set(Calendar.MONTH, 0);
    calendar.set(Calendar.DAY_OF_MONTH, 15);
    calendar.set(Calendar.HOUR_OF_DAY, 21);
    calendar.set(Calendar.MINUTE, 25);
    calendar.set(Calendar.SECOND, 05);
    return calendar.getTime();
  }
  @Test
  public void testGetELVarsForTime() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    try {
      target.init();
      Map<String, Object> map = target.getELVarsForTime(getFixedDate(target.getTimeZone()));
      Assert.assertEquals("2015", map.get("YYYY"));
      Assert.assertEquals("15", map.get("YY"));
      Assert.assertEquals("01", map.get("MM"));
      Assert.assertEquals("15", map.get("DD"));
      Assert.assertEquals("21", map.get("hh"));
      Assert.assertEquals("25", map.get("mm"));
      Assert.assertEquals("05", map.get("ss"));
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testDirPathElEval() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    try {
      target.init();
      Assert.assertNotNull(target.getPathElEvaluator());
      Map<String, Object> map = target.getELVarsForTime(getFixedDate(target.getTimeZone()));
      ELEvaluator.Variables vars = new ELEvaluator.Variables(map, null);
      Assert.assertEquals("2015/15/01/15/21/25/05",
                          target.getPathElEvaluator().eval(vars, "${YYYY}/${YY}/${MM}/${DD}/${hh}/${mm}/${ss}"));
      Record record = RecordCreator.create();
      record.set(Field.create("field"));
      ELRecordSupport.setRecordInContext(vars, record);
      Assert.assertEquals("field", target.getPathElEvaluator().eval(vars, "${record:value('/')}"));
    } finally {
      target.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testInvalidPathTemplate() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    target.dirPathTemplate = "${";
    try {
      target.init();
      Assert.assertNotNull(target.getTimeZone());
      Assert.assertEquals("UTC", target.getTimeZone().getID());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testNoCompressionCodec() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    try {
      target.init();
      Assert.assertNull(target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testDefinedCompressionCodec() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    target.compression = CompressionMode.GZIP.name();
    try {
      target.init();
      Assert.assertEquals(CompressionMode.GZIP.getCodec(), target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testCustomCompressionCodec() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    target.compression = DeflateCodec.class.getName();
    try {
      target.init();
      Assert.assertEquals(DeflateCodec.class, target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testInvalidCompressionCodec() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    target.compression = String.class.getName();
    try {
      target.init();
      Assert.assertEquals(DeflateCodec.class, target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testUnknownCompressionCodec() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    target.compression = "foo";
    try {
      target.init();
      Assert.assertEquals(DeflateCodec.class, target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testLateRecordsLimitSecs() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    try {
      target.init();
      Assert.assertEquals(3600, target.getLateRecordLimitSecs());
    } finally {
      target.destroy();
    }

    target = new BaseHdfsTarget() {};
    configure(target);
    target.lateRecordsLimit = "1 * MINUTES";
    try {
      target.init();
      Assert.assertEquals(60, target.getLateRecordLimitSecs());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testTimeDriverElEvalNow() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    try {
      target.init();
      Assert.assertNotNull(target.getTimeDriverElEval());
      ELEvaluator.Variables vars = new ELEvaluator.Variables(null, null);
      target.setTimeNowInContext(vars);
      Date now = (Date) vars.getContextVariable(target.TIME_NOW_CONTEXT_VAR);
      Assert.assertEquals(now, target.getTimeDriverElEval().eval(vars, target.timeDriver));
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testTimeDriverElEvalRecordValue() throws Exception {
    BaseHdfsTarget target = new BaseHdfsTarget() {};
    configure(target);
    target.timeDriver = "record:value('/')";
    try {
      target.init();
      Assert.assertNotNull(target.getTimeDriverElEval());
      Record record = RecordCreator.create();
      Date date = new Date();
      record.set(Field.createDatetime(date));
      ELEvaluator.Variables vars = new ELEvaluator.Variables(null, null);
      Thread.sleep(1);
      target.setTimeNowInContext(vars);
      ELRecordSupport.setRecordInContext(vars, record);
      Assert.assertEquals(date, target.getTimeDriverElEval().eval(vars, target.timeDriver));
    } finally {
      target.destroy();
    }
  }

}
