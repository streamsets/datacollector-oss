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
package com.streamsets.pipeline.lib.operation;

import org.junit.Assert;
import org.junit.Test;

public class TestOperationType {

  @Test
  public void testGetLabelFromIntCode() throws Exception {
    Assert.assertEquals("INSERT", OperationType.getLabelFromIntCode(1));
    Assert.assertEquals("DELETE", OperationType.getLabelFromIntCode(2));
    Assert.assertEquals("UPDATE", OperationType.getLabelFromIntCode(3));
    Assert.assertEquals("UPSERT", OperationType.getLabelFromIntCode(4));
    // Invalid parameter
    String test = OperationType.getLabelFromIntCode(13);
    Assert.assertEquals("UNSUPPORTED", test);
  }

  @Test
  public void testGetLabelFromStringCode() throws Exception {
    Assert.assertEquals("INSERT", OperationType.getLabelFromStringCode("1"));
    Assert.assertEquals("DELETE", OperationType.getLabelFromStringCode("2"));
    Assert.assertEquals("UPDATE", OperationType.getLabelFromStringCode("3"));
    Assert.assertEquals("UPSERT", OperationType.getLabelFromStringCode("4"));

    // Invalid parameter
    try {
      OperationType.getLabelFromStringCode("abc");
      Assert.fail();
    } catch (NumberFormatException ex){
      // pass
    }

    String test = OperationType.getLabelFromStringCode("100");
    Assert.assertEquals("UNSUPPORTED", test);
  }

  @Test
  public void testGetCodeFromLabel() throws Exception {
    Assert.assertEquals(1, OperationType.getCodeFromLabel("INSERT"));
    Assert.assertEquals(2, OperationType.getCodeFromLabel("DELETE"));
    Assert.assertEquals(3, OperationType.getCodeFromLabel("UPDATE"));
    Assert.assertEquals(4, OperationType.getCodeFromLabel("UPSERT"));
  }
}
