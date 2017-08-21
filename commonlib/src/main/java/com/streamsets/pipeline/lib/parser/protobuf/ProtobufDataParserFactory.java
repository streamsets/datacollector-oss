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

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.Errors;
import com.streamsets.pipeline.lib.util.ProtobufConstants;
import com.streamsets.pipeline.lib.util.ProtobufTypeUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ProtobufDataParserFactory extends DataParserFactory {

  public static final Map<String, Object> CONFIGS;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ProtobufConstants.PROTO_DESCRIPTOR_FILE_KEY, ProtobufConstants.PROTO_FILE_LOCATION_DEFAULT);
    configs.put(ProtobufConstants.MESSAGE_TYPE_KEY, ProtobufConstants.MESSAGE_TYPE_DEFAULT);
    configs.put(ProtobufConstants.DELIMITED_KEY, ProtobufConstants.DELIMITED_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = ImmutableSet.of();

  private final String protoDescriptorFile;
  private final String messageType;
  private final Descriptors.Descriptor descriptor;
  // this map holds extensions that are defined for each of the message types present in the all the file descriptors
  // that is accessible via the configured Protobuf descriptor file
  private final Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap;
  private final ExtensionRegistry extensionRegistry;
  private final Map<String, Object> defaultValueMap;
  private final boolean isDelimited;

  public ProtobufDataParserFactory(Settings settings) throws StageException {
    super(settings);
    this.protoDescriptorFile = settings.getConfig(ProtobufConstants.PROTO_DESCRIPTOR_FILE_KEY);
    this.messageType = settings.getConfig(ProtobufConstants.MESSAGE_TYPE_KEY);
    this.isDelimited = settings.getConfig(ProtobufConstants.DELIMITED_KEY);
    messageTypeToExtensionMap = new HashMap<>();
    defaultValueMap = new HashMap<>();
    // Get the descriptor for the expected message type
    descriptor = ProtobufTypeUtil.getDescriptor(
      settings.getContext(),
      protoDescriptorFile,
      messageType,
      messageTypeToExtensionMap,
      defaultValueMap
    );

    // Build the extension registry based on the cached extension map
    extensionRegistry = ExtensionRegistry.newInstance();
    for(Map.Entry<String, Set<Descriptors.FieldDescriptor>> e : messageTypeToExtensionMap.entrySet()) {
      Set<Descriptors.FieldDescriptor> value = e.getValue();
      for (Descriptors.FieldDescriptor f : value) {
        extensionRegistry.add(f);
      }
    }
  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    try {
      return new ProtobufDataParser(
          getSettings().getContext(),
          id,
          descriptor,
          messageTypeToExtensionMap,
          extensionRegistry,
          is,
          offset,
          getSettings().getOverRunLimit(),
          isDelimited
      );
    } catch (IOException | Descriptors.DescriptorValidationException e) {
      throw new DataParserException(Errors.DATA_PARSER_01, e.toString(), e);
    }
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    throw new UnsupportedOperationException();
  }

}
