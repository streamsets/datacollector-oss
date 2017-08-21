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

import org.apache.commons.codec.binary.Base64;
import com.streamsets.pipeline.api.Record;
import org.junit.Assert;

import static com.streamsets.pipeline.stage.processor.base64.Utils.*;

public class TestBase64EncodingProcessor extends Base64TestBase {

  public TestBase64EncodingProcessor() {
    super(new Base64EncodingProcessor(ORIGINAL_PATH, RESULT_PATH, false), Base64EncodingDProcessor.class,
        "testString".getBytes());
  }

  @Override
  protected void verify(Record record) {
    Assert.assertArrayEquals(Base64.encodeBase64("testString".getBytes()),
        record.get(RESULT_PATH).getValueAsByteArray());
  }

  // Never used for this -- all byte arrays are valid inputs.
  @Override
  protected byte[] getInvalidData() {
    return new byte[0];
  }

}
