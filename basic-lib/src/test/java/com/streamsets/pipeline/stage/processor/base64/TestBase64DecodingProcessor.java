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

import com.streamsets.pipeline.api.base.OnRecordErrorException;
import org.apache.commons.codec.binary.Base64;
import com.streamsets.pipeline.api.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import static com.streamsets.pipeline.stage.processor.base64.Utils.ORIGINAL_PATH;
import static com.streamsets.pipeline.stage.processor.base64.Utils.RESULT_PATH;

public class TestBase64DecodingProcessor extends Base64TestBase{

  public TestBase64DecodingProcessor() {
    super(new Base64DecodingProcessor(ORIGINAL_PATH, RESULT_PATH), Base64DecodingDProcessor.class,
        Base64.encodeBase64("testString".getBytes()));
  }

  @Test
  public void testInvalidFieldDataDiscard() throws Exception {
    doTestInvalidFieldDiscard(false, true);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidFieldDataStopPipeLine() throws Exception {
    doTestInvalidFieldStopPipeLine(false, true);
  }

  @Test
  public void testInvalidFieldDataToError() throws Exception {
    doTestInvalidFieldToError(false, true);
  }

  @Override
  protected void verify(Record record) {
    Assert.assertArrayEquals("testString".getBytes(), record.get(RESULT_PATH).getValueAsByteArray());
  }

  @Override
  protected byte[] getInvalidData() {
    byte[] random = new byte[10];
    new Random().nextBytes(random);
    return random;
  }
}
