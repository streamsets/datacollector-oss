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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;

public class BaseHdfsTargetIT {

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

  private void configure(HdfsDTarget target) {

    HdfsTargetConfigBean hdfsTargetConfigBean = new HdfsTargetConfigBean();
    hdfsTargetConfigBean.hdfsUri = "hdfs://localhost";
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
