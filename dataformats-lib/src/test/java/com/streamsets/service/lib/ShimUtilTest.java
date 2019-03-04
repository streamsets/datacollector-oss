/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.service.lib;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.service.dataformats.DataGeneratorException;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.api.service.dataformats.RecoverableDataParserException;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ShimUtilTest {

  private static Throwable cause = new Throwable();
  private static Record record = RecordCreator.create();

  @Test
  public void testConvertParserException() {
    DataParserException converted = ShimUtil.convert(
      new com.streamsets.pipeline.lib.parser.DataParserException(
        Errors.DATAFORMAT_0001,
        "1",
        "2",
        cause
      )
    );

    assertEquals(Errors.DATAFORMAT_0001, converted.getErrorCode());
    assertArrayEquals(new Object[]{"1", "2", cause}, converted.getParams());
    assertEquals(cause, converted.getCause());
  }

  @Test
  public void testConvertRecoverableParserException() {
    RecoverableDataParserException converted = (RecoverableDataParserException) ShimUtil.convert(
      new com.streamsets.pipeline.lib.parser.RecoverableDataParserException(
        record,
        Errors.DATAFORMAT_0001,
        "1",
        "2",
        cause
      )
    );

    assertEquals(Errors.DATAFORMAT_0001, converted.getErrorCode());
    assertEquals(record, converted.getUnparsedRecord());
    assertArrayEquals(new Object[]{"1", "2", cause}, converted.getParams());
    assertEquals(cause, converted.getCause());
  }

  @Test
  public void testConvertGeneratorException() {
    DataGeneratorException converted = ShimUtil.convert(
      new com.streamsets.pipeline.lib.generator.DataGeneratorException(
        Errors.DATAFORMAT_0001,
        "1",
        "2",
        cause
      )
    );

    assertEquals(Errors.DATAFORMAT_0001, converted.getErrorCode());
    assertArrayEquals(new Object[]{"1", "2", cause}, converted.getParams());
    assertEquals(cause, converted.getCause());
  }
}
