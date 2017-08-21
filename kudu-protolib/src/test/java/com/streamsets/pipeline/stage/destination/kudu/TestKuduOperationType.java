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
package com.streamsets.pipeline.stage.destination.kudu;

import org.junit.Test;
import org.junit.Assert;

public class TestKuduOperationType {

  @Test
  public void TestconvertToIntCode() throws Exception {
    int insert = KuduOperationType.convertToIntCode("1");
    Assert.assertEquals(1, insert);

    int delete = KuduOperationType.convertToIntCode("2");
    Assert.assertEquals(2, delete);

    int unsupported1 = KuduOperationType.convertToIntCode("10");
    Assert.assertEquals(-1, unsupported1);

    int unsupported2 = KuduOperationType.convertToIntCode("-10");
    Assert.assertEquals(-1, unsupported2);

    try {
      KuduOperationType.convertToIntCode("insert");
      Assert.fail();
    } catch (NumberFormatException ex) {
      // pass
    }

    try {
      KuduOperationType.convertToIntCode("0.5");
      Assert.fail();
    } catch (NumberFormatException ex) {
      // pass
    }
  }
}
