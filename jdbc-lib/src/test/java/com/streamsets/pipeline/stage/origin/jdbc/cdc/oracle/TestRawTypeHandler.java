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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import com.streamsets.pipeline.api.StageException;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Test;

public class TestRawTypeHandler {

  @Test
  public void testRawField() throws DecoderException, StageException {
    String input = "HEXTORAW('e4d152ad')";
    byte[] expected = Hex.decodeHex("e4d152ad".toCharArray());
    byte[] parsed = RawTypeHandler.parseRaw("don't care", input, 2932);
    Assert.assertArrayEquals(expected, parsed);
  }

  @Test(expected = UnsupportedFieldTypeException.class)
  public void testRawFieldWithoutFunction() throws DecoderException, StageException {
    String input = "e4d152ad";
    RawTypeHandler.parseRaw("don't care", input, 2932);
  }

  @Test
  public void testInvalidHex() throws DecoderException, StageException {
    String input = "HEXTORAW('e4dyu152ad')";
    try {
      RawTypeHandler.parseRaw("don't care", input, 2932);
      Assert.fail();
    } catch (StageException e) {
      Assert.assertEquals(e.getErrorCode().getCode(), "JDBC_204");
    }
  }

  @Test
  public void testNull() throws DecoderException, StageException {
    Assert.assertNull(RawTypeHandler.parseRaw("don't care", null, 2932));
  }

}
