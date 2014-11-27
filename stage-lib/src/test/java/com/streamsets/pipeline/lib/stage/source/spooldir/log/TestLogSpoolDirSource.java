/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.stage.source.spooldir.log;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.List;
import java.util.UUID;

public class TestLogSpoolDirSource {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private final static String LINE1 = "1234567890";
  private final static String LINE2 = "A1234567890";

  private File createLogFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    IOUtils.write(LINE1 + "\n", writer);
    IOUtils.write(LINE2, writer);
    writer.close();
    return f;
  }

  private Source.Context getMockContext() {
    Source.Context context = Mockito.mock(Source.Context.class);
    Mockito.when(context.createMeter(Mockito.anyString())).thenReturn(new Meter());
    Mockito.when(context.createCounter(Mockito.anyString())).thenReturn(new Counter());
    Mockito.when(context.createRecord(Mockito.anyString())).thenReturn(Mockito.mock(Record.class));
    return context;
  }

  @Test
  public void testProduceFullFile() throws Exception {
    LogSpoolDirSource source = new LogSpoolDirSource() {
      @Override
      protected Source.Context getContext() {
        return getMockContext();
      }
    };
    source.postProcessing = DirectorySpooler.FilePostProcessing.ARCHIVE.toString();
    source.filePattern = "file-[0-9].log";
    source.maxSpoolFiles = 10;
    source.spoolDir = createTestDir();
    source.archiveDir = createTestDir();
    source.retentionTimeMins = 10;
    source.initialFileToProcess = null;
    source.poolingTimeoutSecs = 0;
    source.maxLogLineLength = 10;
    source.init();
    try {
      BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

      Assert.assertEquals(-1, source.produce(createLogFile(), 0, 10, batchMaker));

      Mockito.verify(batchMaker, Mockito.times(2)).addRecord(Mockito.any(Record.class));
      ArgumentCaptor<Record> records = ArgumentCaptor.forClass(Record.class);
      Mockito.verify(batchMaker, Mockito.times(2)).addRecord(records.capture());
      List<Record> list = records.getAllValues();
      ArgumentCaptor<Field> field = ArgumentCaptor.forClass(Field.class);
      Mockito.verify(list.get(0)).set(field.capture());
      Assert.assertEquals(LINE1, field.getValue().getValue());
      field = ArgumentCaptor.forClass(Field.class);
      Mockito.verify(list.get(1)).set(field.capture());
      Assert.assertEquals(LINE2.substring(0, 10), field.getValue().getValue());
    } finally {
      source.destroy();
    }
  }

  @Test
  public void testProduceLessThanFile() throws Exception {
    LogSpoolDirSource source = new LogSpoolDirSource() {
      @Override
      protected Source.Context getContext() {
        return getMockContext();
      }
    };
    source.postProcessing = DirectorySpooler.FilePostProcessing.ARCHIVE.toString();
    source.filePattern = "file-[0-9].log";
    source.maxSpoolFiles = 10;
    source.spoolDir = createTestDir();
    source.archiveDir = createTestDir();
    source.retentionTimeMins = 10;
    source.initialFileToProcess = null;
    source.poolingTimeoutSecs = 0;
    source.maxLogLineLength = 10;
    source.init();
    try {
      BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

      //reads first line
      long offset = source.produce(createLogFile(), 0, 1, batchMaker);
      Assert.assertEquals(11, offset);

      ArgumentCaptor<Record> records = ArgumentCaptor.forClass(Record.class);
      Mockito.verify(batchMaker, Mockito.times(1)).addRecord(records.capture());
      List<Record> list = records.getAllValues();
      ArgumentCaptor<Field> field = ArgumentCaptor.forClass(Field.class);
      Mockito.verify(list.get(0)).set(field.capture());
      Assert.assertEquals(LINE1, field.getValue().getValue());


      //reads second line
      Mockito.reset(batchMaker);
      offset = source.produce(createLogFile(), offset, 1, batchMaker);
      Assert.assertEquals(22, offset);

      records = ArgumentCaptor.forClass(Record.class);
      Mockito.verify(batchMaker, Mockito.times(1)).addRecord(records.capture());
      list = records.getAllValues();
      field = ArgumentCaptor.forClass(Field.class);
      Mockito.verify(list.get(0)).set(field.capture());
      Assert.assertEquals(LINE2.substring(0, 10), field.getValue().getValue());

      //reads EOF
      Mockito.reset(batchMaker);
      offset = source.produce(createLogFile(), offset, 1, batchMaker);
      Assert.assertEquals(-1, offset);

      Mockito.verify(batchMaker, Mockito.times(0)).addRecord(Mockito.any(Record.class));
    } finally {
      source.destroy();
    }
  }

}
