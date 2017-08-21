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

import com.streamsets.pipeline.lib.operation.OperationType;
import org.junit.Test;
import org.junit.Assert;

public class TestOracleCDCOperationCode {

  @Test
  public void testConvertFromOracleToSDCCode(){
    Assert.assertEquals(
        OperationType.INSERT_CODE,
        OracleCDCOperationCode.convertFromOracleToSDCCode(String.valueOf(OracleCDCOperationCode.INSERT_CODE))
    );

    Assert.assertEquals(
        OperationType.DELETE_CODE,
        OracleCDCOperationCode.convertFromOracleToSDCCode(String.valueOf(OracleCDCOperationCode.DELETE_CODE))
    );

    Assert.assertEquals(
        OperationType.UPDATE_CODE,
        OracleCDCOperationCode.convertFromOracleToSDCCode(String.valueOf(OracleCDCOperationCode.UPDATE_CODE))
    );

    Assert.assertEquals(
        OperationType.UPDATE_CODE,
        OracleCDCOperationCode.convertFromOracleToSDCCode(String.valueOf(OracleCDCOperationCode.SELECT_FOR_UPDATE_CODE))
    );

    try {
      OracleCDCOperationCode.convertFromOracleToSDCCode("INSERT");
    } catch (NumberFormatException ex){
       // pass
    } catch (Exception ex) {
      Assert.fail("Wrong exception is thrown:" + ex.getMessage());
    }

    try {
      OracleCDCOperationCode.convertFromOracleToSDCCode(String.valueOf(OracleCDCOperationCode.DDL_CODE));
    } catch (UnsupportedOperationException ex){
      // pass
    } catch (Exception ex) {
      Assert.fail("Wrong exception is thrown:" + ex.getMessage());
    }
  }
}
