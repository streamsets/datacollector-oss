/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.generator.wholefile;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public class WholeFileDataGenerator implements DataGenerator {
  private static final String FILE_REF = "/fileRef";
  private static final String FILE_INFO = "/fileInfo";
  private static final List<String> MANDATORY_FIELD_PATHS = ImmutableList.of(FILE_REF, FILE_INFO);
  private final Stage.Context context;
  private final OutputStream outputStream;

  WholeFileDataGenerator(
      Stage.Context context,
      OutputStream os
  ) throws IOException {
    this.context = context;
    this.outputStream = os;
  }

  private void validateRecord(Record record) throws DataGeneratorException {
    for (String fieldPath : MANDATORY_FIELD_PATHS) {
      if (!record.has(fieldPath)) {
        throw new DataGeneratorException(Errors.WHOLE_FILE_GENERATOR_ERROR_0, fieldPath);
      }
    }
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    validateRecord(record);
    FileRef fileRef = record.get(FILE_REF).getValueAsFileRef();
    int bufferSize = fileRef.getBufferSize();
    boolean canUseDirectByteBuffer = fileRef.getSupportedStreamClasses().contains(ReadableByteChannel.class);
    if (canUseDirectByteBuffer) {
      WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
      try (ReadableByteChannel readableByteChannel = fileRef.createInputStream(context, ReadableByteChannel.class)){
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
        while ((readableByteChannel.read(buffer)) > 0) {
          //Flip to use the buffer from 0 to position.
          buffer.flip();
          while (buffer.hasRemaining()) {
            writableByteChannel.write(buffer);
          }
          //Compact the buffer for reuse.
          buffer.clear();
        }
      }
    } else {
      byte[] b = new byte[bufferSize];
      try (InputStream stream = fileRef.createInputStream(context, InputStream.class)) {
        IOUtils.copyLarge(stream, outputStream, b);
      }
    }
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
  }
}
