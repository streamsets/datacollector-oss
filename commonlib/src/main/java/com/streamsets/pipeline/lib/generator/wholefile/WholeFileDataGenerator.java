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
package com.streamsets.pipeline.lib.generator.wholefile;

import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.ChecksumAlgorithm;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

final class WholeFileDataGenerator implements DataGenerator {
  private final ProtoConfigurableEntity.Context context;
  private final OutputStream outputStream;
  private final boolean includeChecksumInTheEvents;
  private final ChecksumAlgorithm checksumAlgorithm;
  private final StreamCloseEventHandler<?> streamCloseEventHandler;

  WholeFileDataGenerator(
      ProtoConfigurableEntity.Context context,
      OutputStream os,
      boolean includeChecksumInTheEvents,
      ChecksumAlgorithm checksumAlgorithm,
      StreamCloseEventHandler<?> streamCloseEventHandler
  ) throws IOException {
    this.context = context;
    this.outputStream = os;
    this.includeChecksumInTheEvents = includeChecksumInTheEvents;
    this.checksumAlgorithm = checksumAlgorithm;
    this.streamCloseEventHandler = streamCloseEventHandler;
  }

  private void validateRecord(Record record) throws DataGeneratorException {
    try {
      FileRefUtil.validateWholeFileRecord(record);
    } catch (IllegalArgumentException e) {
      throw new DataGeneratorException(Errors.WHOLE_FILE_GENERATOR_ERROR_0, e);
    }
  }

  private <T extends AutoCloseable> T getReadableStream(FileRef fileRef, Class<T> streamClass) throws IOException {
    return FileRefUtil.getReadableStream(
        context,
        fileRef,
        streamClass,
        includeChecksumInTheEvents,
        checksumAlgorithm,
        streamCloseEventHandler
    );
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    validateRecord(record);
    FileRef fileRef = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef();
    int bufferSize = fileRef.getBufferSize();
    boolean canUseDirectByteBuffer = fileRef.getSupportedStreamClasses().contains(ReadableByteChannel.class);
    if (canUseDirectByteBuffer) {
      //Don't have to close this here, because generate.close will call output stream close
      WritableByteChannel writableByteChannel = Channels.newChannel(outputStream); //NOSONAR
      try (ReadableByteChannel readableByteChannel = getReadableStream(fileRef, ReadableByteChannel.class)){
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
      try (InputStream stream = getReadableStream(fileRef, InputStream.class)) {
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
