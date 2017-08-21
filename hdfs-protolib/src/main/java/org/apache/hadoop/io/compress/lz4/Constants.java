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
package org.apache.hadoop.io.compress.lz4;

public class Constants {
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

  public static final String IO_COMPRESSION_CODEC_LZ4_UNSAFE_KEY = "sdc.io.compression.codec.lz4.unsafe";

  public static final boolean IO_COMPRESSION_CODEC_LZ4_UNSAFE_DEFAULT = true;

  private Constants() {}
}
