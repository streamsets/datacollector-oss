/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.lib.util.avroorc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TestOrcTimestamp {

  @Test
  public void testTimestampWriteAndRead() throws IOException {
    Configuration conf = new Configuration();
    TypeDescription schema = TypeDescription.fromString("struct<ts:timestamp>");

    final String tempPath = String.format("%s/output-%d.orc",
        System.getProperty("java.io.tmpdir"),
        Clock.systemUTC().millis()
    );

    Writer writer = OrcFile.createWriter(new Path(tempPath),
        OrcFile.writerOptions(conf).setSchema(schema)
    );

    VectorizedRowBatch batch = schema.createRowBatch();
    TimestampColumnVector tsVec = (TimestampColumnVector) batch.cols[0];

    LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(1516160557, 123456789, ZoneOffset.UTC);
    Timestamp timestamp = Timestamp.from(localDateTime.toInstant(ZoneOffset.UTC));
    tsVec.set(0, timestamp);
    batch.size++;

    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();

    Reader reader = OrcFile.createReader(
        new Path(tempPath),
        OrcFile.readerOptions(conf)
    );

    RecordReader rows = reader.rows();
    VectorizedRowBatch readBatch = reader.getSchema().createRowBatch();

    while (rows.nextBatch(readBatch)) {
      for(int r = 0; r < readBatch.size; ++r) {
        TimestampColumnVector tsReadVec = (TimestampColumnVector) readBatch.cols[0];
        final long millis = tsReadVec.time[r];
        final int nanos = tsReadVec.nanos[r];

        int k = 3;

      }
    }
    rows.close();

  }
}
