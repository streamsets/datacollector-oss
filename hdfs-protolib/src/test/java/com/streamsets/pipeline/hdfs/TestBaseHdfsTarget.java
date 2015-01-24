/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.hadoop.conf.Configuration;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

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
    target.timeDriver = "${time:now()}";
    target.lateRecordsLimit = "${1 * HOURS}";
    target.csvFileFormat = CsvFileMode.CSV;
    target.dataFormat = HdfsDataFormat.CSV;
    target.cvsFieldPathToNameMappingConfigList = new ArrayList<>();
  }

  static class ForTestHdfsTarget extends BaseHdfsTarget {
    @Override
    public void processBatch(Batch batch) throws StageException {
    }
  }

  @Test
  public void getGetHdfsConfiguration() throws Exception {
    BaseHdfsTarget target = new ForTestHdfsTarget();
    configure(target);
    try {
      target.init();
      Assert.assertNotNull(target.getHdfsConfiguration());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testHdfsConfigs() throws Exception {
    BaseHdfsTarget target = new ForTestHdfsTarget();
    configure(target);
    try {
      target.init();
      Assert.assertEquals("X", target.getHdfsConfiguration().get("x"));
    } finally {
      target.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testKerberosConfig() throws Exception {
    BaseHdfsTarget target = new ForTestHdfsTarget();
    configure(target);
    target.hdfsKerberos = true;
    target.kerberosKeytab = "/tmp/keytab";
    target.kerberosPrincipal = "sdc/localhost";
    try {
      target.init();
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testNoCompressionCodec() throws Exception {
    BaseHdfsTarget target = new ForTestHdfsTarget();
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
    BaseHdfsTarget target = new ForTestHdfsTarget();
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
    BaseHdfsTarget target = new ForTestHdfsTarget();
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
    BaseHdfsTarget target = new ForTestHdfsTarget();
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
    BaseHdfsTarget target = new ForTestHdfsTarget();
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
    BaseHdfsTarget target = new ForTestHdfsTarget();
    configure(target);
    try {
      target.init();
      target.getBatchTime();
      Assert.assertEquals(3600, target.getLateRecordLimitSecs());
    } finally {
      target.destroy();
    }

    target = new ForTestHdfsTarget();
    configure(target);
    target.lateRecordsLimit = "${1 * MINUTES}";
    try {
      target.init();
      Assert.assertEquals(60, target.getLateRecordLimitSecs());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testTimeDriverElEvalNow() throws Exception {
    BaseHdfsTarget target = new ForTestHdfsTarget();
    configure(target);
    try {
      target.init();
      target.getBatchTime();
      Record record = RecordCreator.create();
      ELEvaluator.Variables vars = new ELEvaluator.Variables(null, null);
      target.write(null); //forcing a setBatchTime()
      Date now = target.getBatchTime();
      Assert.assertEquals(now, target.getRecordTime(record));
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testTimeDriverElEvalRecordValue() throws Exception {
    BaseHdfsTarget target = new ForTestHdfsTarget();
    configure(target);
    target.timeDriver = "${record:value('/')}";
    try {
      target.init();
      Date date = new Date();
      Record record = RecordCreator.create();
      record.set(Field.createDatetime(date));
      ELEvaluator.Variables vars = new ELEvaluator.Variables(null, null);
      Thread.sleep(1); // so batch time is later than date for sure
      target.processBatch(null); //forcing a setBatchTime()
      Assert.assertEquals(date, target.getRecordTime(record));
    } finally {
      target.destroy();
    }
  }

}
