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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroorc;

public class AvroOrcConstants {

  /**
   * Size of the ORC writer batch
   */
  public final static String ORC_BATCH_SIZE = AvroOrcConstants.class.getCanonicalName() + ".orc_batch_size";

  /**
   * Prefix that will be used for temporary file that is just being converted.
   */
  public final static String TMP_PREFIX = ".avro_to_orc_tmp_conversion_";
}
