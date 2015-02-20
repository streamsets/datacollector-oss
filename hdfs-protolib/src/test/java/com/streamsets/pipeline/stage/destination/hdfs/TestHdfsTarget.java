/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestHdfsTarget {
  private static String testDir;

  @BeforeClass
  public static void setUpClass() {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    testDir = dir.getAbsolutePath();
  }

  private String getTestDir() {
    return testDir;
  }

  @Test
  public void testTarget() throws Exception {
    TargetRunner runner = new TargetRunner.Builder(HdfsTarget.class)
        .addConfiguration("hdfsUri", "file:///")
        .addConfiguration("hdfsKerberos", false)
        .addConfiguration("hdfsConfigs", new HashMap<>())
        .addConfiguration("uniquePrefix", "foo")
        .addConfiguration("dirPathTemplate", getTestDir() + "/hdfs/${YYYY}${MM}${DD}${hh}${mm}${record:value('/a')}")
        .addConfiguration("timeZoneID", "UTC")
        .addConfiguration("fileType", HdfsFileType.TEXT)
        .addConfiguration("keyEl", "${uuid()}")
        .addConfiguration("compression", "NONE")
        .addConfiguration("seqFileCompressionType", HdfsSequenceFileCompressionType.BLOCK)
        .addConfiguration("maxRecordsPerFile", 5)
        .addConfiguration("maxFileSize", 0)
        .addConfiguration("timeDriver", "${record:value('/time')}")
        .addConfiguration("lateRecordsLimit", "${30 * MINUTES}")
        .addConfiguration("lateRecordsAction", LateRecordsAction.SEND_TO_ERROR)
        .addConfiguration("lateRecordsDirPathTemplate", "")
        .addConfiguration("dataFormat", DataFormat.SDC_JSON)
        .addConfiguration("csvFileFormat", null)
        .addConfiguration("cvsFieldPathToNameMappingConfigList", new ArrayList<>())
        .addConfiguration("onRecordError", OnRecordError.STOP_PIPELINE)
        .addConfiguration("replaceNewLines", false)
        .build();
    runner.runInit();
    List<Record> records = new ArrayList<>();
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("time", Field.createDatetime(new Date()));
    map.put("a", Field.create("x"));
    record.set(Field.create(map));
    records.add(record);

    record = RecordCreator.create();
    map.put("a", Field.create("y"));
    record.set(Field.create(map));
    records.add(record);

    record = RecordCreator.create();
    map = new HashMap<>();
    map.put("time", Field.createDatetime(new Date(System.currentTimeMillis() - 1 * 60 * 1000)));
    map.put("a", Field.create("x"));
    record.set(Field.create(map));
    records.add(record);

    record = RecordCreator.create();
    map = new HashMap<>();
    map.put("time", Field.createDatetime(new Date(System.currentTimeMillis() - 2 * 60 * 1000)));
    map.put("a", Field.create("x"));
    record.set(Field.create(map));
    records.add(record);

    runner.runWrite(records);

    runner.runDestroy();
  }

}
