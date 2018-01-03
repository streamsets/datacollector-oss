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
package com.streamsets.pipeline.lib.parser.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.io.OverrunInputStream;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.util.ProtobufTypeUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

public class ProtobufDataParser extends AbstractDataParser {

  private static final String OFFSET_SEPARATOR = "::";

  private boolean eof;
  private final ProtoConfigurableEntity.Context context;
  private final DynamicMessage.Builder builder;
  private final OverrunInputStream inputStream;
  private final String messageId;
  private final Descriptors.Descriptor descriptor;
  // this map holds extensions that are defined for each of the message types present in the all the file descriptors
  // that is accessible via the configured Protobuf descriptor file
  private final Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap;
  private final ExtensionRegistry extensionRegistry;
  private final boolean isDelimited;

  public ProtobufDataParser(
      ProtoConfigurableEntity.Context context,
      String messageId,
      Descriptors.Descriptor descriptor,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      ExtensionRegistry extensionRegistry,
      InputStream inputStream,
      String readerOffset,
      int maxObjectLength,
      boolean isDelimited
  ) throws IOException, Descriptors.DescriptorValidationException, DataParserException {
    this.context = context;
    this.inputStream = new OverrunInputStream(inputStream, maxObjectLength, true);
    this.messageId = messageId;
    this.messageTypeToExtensionMap = messageTypeToExtensionMap;
    this.extensionRegistry = extensionRegistry;
    this.descriptor = descriptor;
    this.builder = DynamicMessage.newBuilder(descriptor);
    this.isDelimited = isDelimited;

    // skip to the required location
    if (readerOffset != null && !readerOffset.isEmpty() && !readerOffset.equals("0")) {
      int offset = Integer.parseInt(readerOffset);
      this.inputStream.skip(offset);
    }
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    DynamicMessage message;
    long pos = inputStream.getPos();
    inputStream.resetCount();
    if (!isDelimited) {
      if (!eof) {
        builder.mergeFrom(inputStream, extensionRegistry);
        // Set EOF since non-delimited can only contain a single message.
        eof = true;
      } else {
        return null;
      }
    } else {
      if (!builder.mergeDelimitedFrom(inputStream, extensionRegistry)) {
        // No more messages to process in this stream.
        eof = true;
        return null;
      }
    }
    message = builder.build();
    // If the message does not contain required fields then the above call throws UninitializedMessageException
    // with a message similar to the following:
    // com.google.protobuf.UninitializedMessageException: Message missing required fields: phone[0].type
    builder.clear();
    Record record = context.createRecord(messageId + OFFSET_SEPARATOR + pos);
    record.set(ProtobufTypeUtil.protobufToSdcField(record, "", descriptor, messageTypeToExtensionMap, message));
    return record;
  }

  @Override
  public String getOffset() throws DataParserException {
    return eof ? String.valueOf(-1) : String.valueOf(inputStream.getPos());
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
