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
package com.streamsets.pipeline.lib.jdbc;

import org.junit.Test;
import org.junit.Assert;

public class TestJDBCOperationType {

  @Test
  public void testConvertToIntCode(){
    Assert.assertEquals(1, JDBCOperationType.convertToIntCode("1"));
    Assert.assertEquals(2, JDBCOperationType.convertToIntCode("2"));
    Assert.assertEquals(3, JDBCOperationType.convertToIntCode("3"));

    try {
      // INSERT should cause NumberFormatException
      JDBCOperationType.convertToIntCode("INSERT");
      Assert.fail();
    } catch (NumberFormatException ex){
      // pass
    } catch (Exception ex) {
      Assert.fail("Wrong exception:" + ex.getMessage());
    }

    try { // 10 is not supported operation code by JDBC destination
      JDBCOperationType.convertToIntCode("10");
      Assert.fail();
    } catch (UnsupportedOperationException ex){
      // pass
    } catch (Exception ex) {
      Assert.fail("Wrong exception:" + ex.getMessage());
    }
  }
}
