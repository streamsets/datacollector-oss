/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

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
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
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
import java.util.List;

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

  private void configure(HdfsDTarget target) {
    target.hdfsUri = miniDFS.getURI().toString();
    target.hdfsConfigs = new HashMap<String, String>();
    target.hdfsConfigs.put("x", "X");
    target.timeZoneID = "UTC";
    target.dirPathTemplate = "${YYYY}";
    target.lateRecordsDirPathTemplate = "";
    target.compression = CompressionMode.NONE.name();
    target.timeDriver = "${time:now()}";
    target.lateRecordsLimit = "${1 * HOURS}";
    target.dataFormat = DataFormat.DELIMITED;
    target.csvFileFormat = CsvMode.CSV;
    target.csvHeader = CsvHeader.IGNORE_HEADER;
  }

  static class ForTestHdfsTarget extends HdfsDTarget {
    @Override
    protected Target createTarget() {
      return new HdfsTarget(
          hdfsUri,
          hdfsKerberos,
          kerberosPrincipal,
          kerberosKeytab,
          hdfsConfigs,
          uniquePrefix,
          dirPathTemplate,
          timeZoneID,
          timeDriver,
          maxRecordsPerFile,
          maxFileSize,
          compression,
          fileType,
          keyEl,
          seqFileCompressionType,
          lateRecordsLimit,
          lateRecordsAction,
          lateRecordsDirPathTemplate,
          dataFormat,
          csvFileFormat,
          csvHeader,
          cvsReplaceNewLines,
          jsonMode,
          textFieldPath,
          textEmptyLineIfNull
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
      target.validateConfigs(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      Assert.assertNotNull(target.getHdfsConfiguration());
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
      target.validateConfigs(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      Assert.assertEquals("X", target.getHdfsConfiguration().get("x"));
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testKerberosConfig() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    dTarget.hdfsKerberos = true;
    dTarget.kerberosKeytab = "/tmp/keytab";
    dTarget.kerberosPrincipal = "sdc/localhost";
    configure(dTarget);
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    List<Stage.ConfigIssue> issues =
        target.validateConfigs(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("HADOOPFS_01"));
  }

  @Test
  public void testNoCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      Assert.assertNull(target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testDefinedCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.compression = CompressionMode.GZIP.name();
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      Assert.assertEquals(CompressionMode.GZIP.getCodec(), target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testCustomCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.compression = DeflateCodec.class.getName();
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      Assert.assertEquals(DeflateCodec.class, target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testInvalidCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.compression = String.class.getName();
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      Assert.assertEquals(DeflateCodec.class, target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testUnknownCompressionCodec() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    dTarget.compression = "foo";
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      Assert.assertEquals(DeflateCodec.class, target.getCompressionCodec());
    } finally {
      target.destroy();
    }
  }

  @Test
  public void testLateRecordsLimitSecs() throws Exception {
    HdfsDTarget dTarget = new ForTestHdfsTarget();
    configure(dTarget);
    HdfsTarget target = (HdfsTarget) dTarget.createTarget();
    try {
      target.validateConfigs(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
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
      target.validateConfigs(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
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
      target.validateConfigs(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      target.getBatchTime();
      Record record = RecordCreator.create();
      ELEvaluator.Variables vars = new ELEvaluator.Variables(null, null);
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
      target.validateConfigs(null, ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      Date date = new Date();
      Record record = RecordCreator.create();
      record.set(Field.createDatetime(date));
      ELEvaluator.Variables vars = new ELEvaluator.Variables(null, null);
      Thread.sleep(1); // so batch time is later than date for sure
      target.write((Batch) null); //forcing a setBatchTime()
      Assert.assertEquals(date, target.getRecordTime(record));
    } finally {
      target.destroy();
    }
  }

}
