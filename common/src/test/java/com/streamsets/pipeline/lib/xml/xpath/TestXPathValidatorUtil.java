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
package com.streamsets.pipeline.lib.xml.xpath;

import com.streamsets.pipeline.lib.xml.Constants;
import org.junit.Assert;
import org.junit.Test;

public class TestXPathValidatorUtil {

  @Test
  public void testGetXPathValidationError() {

    String result1 = XPathValidatorUtil.getXPathValidationError("a/b/c");
    Assert.assertTrue(result1.startsWith(Constants.ERROR_XPATH_MUST_START_WITH_SEP));

    String result2 = XPathValidatorUtil.getXPathValidationError("");
    Assert.assertTrue(result2.startsWith(Constants.ERROR_EMPTY_EXPRESSION));

    String result3 = XPathValidatorUtil.getXPathValidationError("/y^x");
    Assert.assertTrue(result3.startsWith(Constants.ERROR_INVALID_ELEMENT_NAME_PREFIX));

    String result4 = XPathValidatorUtil.getXPathValidationError("/a/b//d");
    Assert.assertTrue(result4.startsWith(Constants.ERROR_DESCENDENT_OR_SELF_NOT_SUPPORTED));

    String result5 = XPathValidatorUtil.getXPathValidationError("/a/b[xyz='mbx']");
    Assert.assertTrue(result5.startsWith(Constants.ERROR_INVALID_PREDICATE_PREFIX));

    String result6 = XPathValidatorUtil.getXPathValidationError("/a/b[@x%yz='mbx']");
    Assert.assertTrue(result6.startsWith(Constants.ERROR_INVALID_ATTRIBUTE_PREFIX));

    String result20 = XPathValidatorUtil.getXPathValidationError("/a/b/c");
    Assert.assertNull(result20);

  }
}
