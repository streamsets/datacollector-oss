/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.parser.protobuf;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.Errors;
import com.streamsets.pipeline.lib.util.ProtobufTypeUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ProtobufDataParserFactory extends DataParserFactory {

  static final String KEY_PREFIX = "protobuf.";
  public static final String PROTO_DESCRIPTOR_FILE_KEY = KEY_PREFIX + "proto.descriptor.file";
  static final String PROTO_FILE_LOCATION_DEFAULT = "";
  public static final String MESSAGE_TYPE_KEY = KEY_PREFIX + "message.type";
  static final String MESSAGE_TYPE_DEFAULT = "";

  public static final Map<String, Object> CONFIGS;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(PROTO_DESCRIPTOR_FILE_KEY, PROTO_FILE_LOCATION_DEFAULT);
    configs.put(MESSAGE_TYPE_KEY, MESSAGE_TYPE_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of();

  private final String protoDescriptorFile;
  private final String messageType;
  private final Descriptors.Descriptor descriptor;
  // this map holds extensions that are defined for each of the message types present in the all the file descriptors
  // that is accessible via the configured Protobuf descriptor file
  private final Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap;
  // this map holds all the dependencies that a given file descriptor has.
  // This cached map will be looked up while building FileDescriptor instances
  private final Map<String, Set<Descriptors.FileDescriptor>> fileDescriptorDependentsMap;
  // All encountered FileDescriptor instances cached based on their name.
  private final Map<String, Descriptors.FileDescriptor> fileDescriptorMap;
  private final ExtensionRegistry extensionRegistry;

  public ProtobufDataParserFactory(Settings settings) throws IOException, Descriptors.DescriptorValidationException {
    super(settings);
    this.protoDescriptorFile = settings.getConfig(PROTO_DESCRIPTOR_FILE_KEY);
    this.messageType = settings.getConfig(MESSAGE_TYPE_KEY);

    Stage.Context context = settings.getContext();

    // Build the FileDescriptorSet from the configured protobuf descriptor file
    FileInputStream fin = new FileInputStream(new File(context.getResourcesDirectory(), protoDescriptorFile));
    DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(fin);

    // Iterate over all the file descriptor set computed above and cache dependencies and all encountered
    // file descriptors
    fileDescriptorDependentsMap = new HashMap<>();
    fileDescriptorMap = new HashMap<>();
    ProtobufTypeUtil.getAllFileDescriptors(set, fileDescriptorDependentsMap, fileDescriptorMap);

    // Get the descriptor for the expected message type
    descriptor = ProtobufTypeUtil.getDescriptor(set, fileDescriptorMap, protoDescriptorFile, messageType);

    // Compute and cache all extensions defined for each message type
    messageTypeToExtensionMap = ProtobufTypeUtil.getAllExtensions(fileDescriptorMap);

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
          getSettings().getOverRunLimit()
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
