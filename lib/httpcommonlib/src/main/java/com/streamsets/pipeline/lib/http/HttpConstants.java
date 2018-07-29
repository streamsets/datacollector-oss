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
package com.streamsets.pipeline.lib.http;

// copied from com.streamsets.pipeline.stage.destination.sdcipc.Constants
public interface HttpConstants {

  String X_SDC_APPLICATION_ID_HEADER = "X-SDC-APPLICATION-ID";
  String SDC_APPLICATION_ID_QUERY_PARAM = "sdcApplicationId";

  String X_SDC_PING_HEADER = "X-SDC-PING";
  String X_SDC_PING_VALUE = "ping";

  String X_SDC_COMPRESSION_HEADER = "X-SDC-COMPRESSION";
  String SNAPPY_COMPRESSION = "snappy";

  String GZIP_COMPRESSION = "gzip";

  String CONTENT_ENCODING_HEADER = "Content-Encoding";

}
