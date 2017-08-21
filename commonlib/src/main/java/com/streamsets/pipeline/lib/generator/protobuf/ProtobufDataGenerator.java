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
package com.streamsets.pipeline.lib.generator.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.util.ProtobufTypeUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

public class ProtobufDataGenerator implements DataGenerator {

  private final OutputStream outputStream;
  private final Descriptors.Descriptor descriptor;
  private final boolean isDelimited;
  private boolean closed;
  private final Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap;
  private final Map<String, Object> defaultValueMap;

  public ProtobufDataGenerator(
      OutputStream outputStream,
      Descriptors.Descriptor descriptor,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Map<String, Object> defaultValueMap,
      boolean isDelimited
  ) {
    this.outputStream = outputStream;
    this.descriptor = descriptor;
    this.messageTypeToExtensionMap = messageTypeToExtensionMap;
    this.defaultValueMap = defaultValueMap;
    this.isDelimited = isDelimited;
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    DynamicMessage message = ProtobufTypeUtil.sdcFieldToProtobufMsg(
        record,
        descriptor,
        messageTypeToExtensionMap,
        defaultValueMap
    );
    if (isDelimited) {
      message.writeDelimitedTo(outputStream);
    } else {
      message.writeTo(outputStream);
    }
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    outputStream.close();
  }
}
