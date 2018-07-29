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

package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert;

import com.streamsets.pipeline.lib.converter.AvroParquetConstants;

public abstract class AvroConversionCommonConstants {
  /**
   * Full path to input avro file that needs to be converted.
   */
  public final static String INPUT_FILE = AvroParquetConstants.class.getCanonicalName() + ".input";
  /**
   * Directory into which the converted parquet file should be stored.
   */
  public final static String OUTPUT_DIR = AvroParquetConstants.class.getCanonicalName() + ".output";
  /**
   * Whether or not to keep input file after the conversion is done.
   */
  public final static String KEEP_INPUT_FILE = AvroParquetConstants.class.getCanonicalName() + ".keep_input_file";
  /**
   * Overwrite tmp file - if the tmp file exists it will be removed.
   */
  public final static String OVERWRITE_TMP_FILE = AvroParquetConstants.class.getCanonicalName() + ".overwrite_tmp_file";
}
