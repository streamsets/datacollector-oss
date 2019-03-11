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
package com.streamsets.pipeline.lib.converter;

public class AvroParquetConstants {

  /**
   * Name of the compression codec that should be passed down to Parquet writer.
   */
  public final static String COMPRESSION_CODEC_NAME = AvroParquetConstants.class.getCanonicalName() + ".compression_codec";

  /**
   * Row group size that will be passed down to Parquet writer.
   */
  public final static String ROW_GROUP_SIZE = AvroParquetConstants.class.getCanonicalName() + ".row_group_size";

  /**
   * Page size that will be passed down to Parquet writer.
   */
  public final static String PAGE_SIZE = AvroParquetConstants.class.getCanonicalName() + ".page_size";

  /**
   * Dictionary page size that will be passed down to Parquet writer.
   */
  public final static String DICTIONARY_PAGE_SIZE = AvroParquetConstants.class.getCanonicalName() + ".dictionary_page_size";

  /**
   * Max padding size that will be passed down to Parquet writer.
   */
  public final static String MAX_PADDING_SIZE = AvroParquetConstants.class.getCanonicalName() + ".max_padding_size";

  /**
   * Prefix that will be used for temporary file that is just being converted.
   */
  public final static String TMP_PREFIX = ".avro_to_parquet_tmp_conversion_";

  /**
   * Timezone that will be used when converting timestamps in AvroWriteSupportInt96
   */
  public final static String TIMEZONE = AvroParquetConstants.class.getCanonicalName() + ".data_timezone";
}
