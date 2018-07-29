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
package com.streamsets.pipeline.stage.destination.http;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.config.DataFormat;

public class DataFormatChooserValues extends BaseEnumChooserValues<DataFormat> {

  public DataFormatChooserValues() {
    super(
        DataFormat.AVRO,
        DataFormat.BINARY,
        DataFormat.DELIMITED,
        DataFormat.JSON,
        DataFormat.PROTOBUF,
        DataFormat.SDC_JSON,
        DataFormat.TEXT
    );
  }

}
