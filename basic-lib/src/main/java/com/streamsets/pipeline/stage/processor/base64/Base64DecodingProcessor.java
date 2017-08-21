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
package com.streamsets.pipeline.stage.processor.base64;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.common.DataFormatErrors;
import org.apache.commons.codec.binary.Base64;

public class Base64DecodingProcessor extends Base64BaseProcesssor {

  public Base64DecodingProcessor(String originFieldPath, String resultFieldPath) {
    super(originFieldPath, resultFieldPath);
  }

  @Override
  protected Field processField(Record record, byte[] fieldData) throws OnRecordErrorException {
    if (!Base64.isBase64(fieldData)) {
      throw new OnRecordErrorException(DataFormatErrors.DATA_FORMAT_302, record.toString());
    }
    return Field.create(Base64.decodeBase64(fieldData));
  }
}
