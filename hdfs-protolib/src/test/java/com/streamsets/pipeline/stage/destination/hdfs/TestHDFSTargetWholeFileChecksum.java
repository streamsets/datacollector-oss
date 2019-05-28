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

import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.ChecksumAlgorithm;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.hdfs.util.HdfsTargetUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

@RunWith(Parameterized.class)
public class TestHDFSTargetWholeFileChecksum {
  private static final String TEXT = "This is a sample file with some text";

  private String testDir;
  private URI uri;
  private ChecksumAlgorithm checksumAlgorithm;

  @Parameterized.Parameters(name = "Checksum Algorithm: {0}")
  public static Object[] data() throws Exception {
    return ChecksumAlgorithm.values();
  }

  public TestHDFSTargetWholeFileChecksum(ChecksumAlgorithm checksumAlgorithm) {
    this.checksumAlgorithm = checksumAlgorithm;
  }

  @Before
  public void before() {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    testDir = dir.getAbsolutePath();
    try {
      uri = new URI("file:///");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    System.setProperty("HADOOP_USER_NAME", System.getProperty("user.name"));
  }

  private String getTestDir() {
    return testDir;
  }


  private void verifyChecksum(String checksum) {
    String calculatedChecksum =
        HashingUtil.getHasher(checksumAlgorithm.getHashType()).hashString(TEXT, Charset.defaultCharset()).toString();
    Assert.assertEquals(calculatedChecksum, checksum);
  }

  @Test
  public void testWholeFileEventRecords() throws Exception {
    java.nio.file.Path filePath = Paths.get(getTestDir() + "/source_testWholeFileEventRecords.txt");
    Files.write(filePath, TEXT.getBytes());

    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
        .hdfsUri(uri.toString())
        .dirPathTemplate(getTestDir())
        .timeDriver("${time:now()}")
        .dataForamt(DataFormat.WHOLE_FILE)
        .fileType(HdfsFileType.WHOLE_FILE)
        .fileNameEL("${record:value('"+ FileRefUtil.FILE_INFO_FIELD_PATH +"/filename')}")
        .maxRecordsPerFile(1)
        .maxFileSize(0)
        .uniquePrefix("sdc-")
        .idleTimeout("-1")
        .wholeFileExistsAction(WholeFileExistsAction.TO_ERROR)
        .includeChecksumInTheEvents(true)
        .checksumAlgorithm(checksumAlgorithm)
        .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
        .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    runner.runInit();

    try {
      Record record = TestHDFSTargetWholeFile.getFileRefRecordForFile(filePath);
      runner.runWrite(Collections.singletonList(record));
      Assert.assertEquals(0, runner.getErrorRecords().size());

      //One whole file event
      Assert.assertEquals(1, runner.getEventRecords().size());

      Iterator<EventRecord> eventRecordIterator = runner.getEventRecords().iterator();
      while (eventRecordIterator.hasNext()) {
        Record eventRecord = eventRecordIterator.next();
        String type = eventRecord.getHeader().getAttribute("sdc.event.type");
        Assert.assertEquals(WholeFileProcessedEvent.WHOLE_FILE_WRITE_FINISH_EVENT, type);
        Assert.assertTrue(eventRecord.has("/" + WholeFileProcessedEvent.CHECKSUM_ALGORITHM));
        Assert.assertTrue(eventRecord.has("/" + WholeFileProcessedEvent.CHECKSUM));
        Assert.assertEquals(checksumAlgorithm.name(), eventRecord.get("/" + WholeFileProcessedEvent.CHECKSUM_ALGORITHM).getValueAsString());
        verifyChecksum(eventRecord.get("/" + WholeFileProcessedEvent.CHECKSUM).getValueAsString());
      }

    } finally {
      runner.runDestroy();
    }
  }

}
