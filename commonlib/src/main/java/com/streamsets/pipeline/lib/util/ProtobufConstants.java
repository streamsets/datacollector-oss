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
package com.streamsets.pipeline.lib.util;

public class ProtobufConstants {

  public static final String KEY_PREFIX = "protobuf.";
  public static final String PROTO_DESCRIPTOR_FILE_KEY = KEY_PREFIX + "proto.descriptor.file";
  public static final String PROTO_FILE_LOCATION_DEFAULT = "";
  public static final String MESSAGE_TYPE_KEY = KEY_PREFIX + "message.type";
  public static final String MESSAGE_TYPE_DEFAULT = "";
  public static final String DELIMITED_KEY = KEY_PREFIX + "delimited";
  public static final boolean DELIMITED_DEFAULT = true;

  private ProtobufConstants() {}
}
