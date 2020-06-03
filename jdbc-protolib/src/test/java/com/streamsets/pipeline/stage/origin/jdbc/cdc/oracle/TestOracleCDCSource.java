/*
 * Copyright 2018 StreamSets Inc.
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

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public class TestOracleCDCSource {

  @Test
  public void testExclusionPattern() {
    OracleCDCConfigBean configBean = new OracleCDCConfigBean();
    OracleCDCSource stage = new OracleCDCSource(null, configBean);

    Pattern pattern1 = stage.createRegexFromSqlLikePattern("%PATT.ERN%");
    Assert.assertFalse(pattern1.matcher("PATTERN1").matches());
    Assert.assertTrue(pattern1.matcher("MY_PATT.ERN_23").matches());

    Pattern pattern2 = stage.createRegexFromSqlLikePattern("PATT!!._ERN%");
    Assert.assertFalse(pattern2.matcher("PATT!!.ERN").matches());
    Assert.assertTrue(pattern2.matcher("PATT!!.?ERN_ACCEPTED").matches());
  }
}
