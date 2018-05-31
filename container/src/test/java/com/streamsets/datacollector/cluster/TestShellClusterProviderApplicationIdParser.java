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
package com.streamsets.datacollector.cluster;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;

public class TestShellClusterProviderApplicationIdParser {


  @Test
  public void testInvalidYARNAppId() throws Exception {
    Matcher matcher;
    matcher = ShellClusterProvider.YARN_APPLICATION_ID_REGEX.
      matcher("");
    Assert.assertFalse(matcher.find());
    matcher = ShellClusterProvider.YARN_APPLICATION_ID_REGEX.
      matcher("application_1429587312661_0024");
    Assert.assertFalse(matcher.find());
    matcher = ShellClusterProvider.YARN_APPLICATION_ID_REGEX.
      matcher("_application_1429587312661_0024_");
    Assert.assertFalse(matcher.find());
    matcher = ShellClusterProvider.YARN_APPLICATION_ID_REGEX.
      matcher(" pplication_1429587312661_0024 ");
    Assert.assertFalse(matcher.find());
    matcher = ShellClusterProvider.YARN_APPLICATION_ID_REGEX.
      matcher(" application_1429587312661_00a24 ");
    Assert.assertFalse(matcher.find());
  }

  @Test
  public void testValidYARNAppId() throws Exception {
    Matcher matcher;
    matcher = ShellClusterProvider.YARN_APPLICATION_ID_REGEX.
      matcher("15/04/21 21:15:20 INFO Client: Application report for application_1429587312661_0024 (state: RUNNING)");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = ShellClusterProvider.YARN_APPLICATION_ID_REGEX.
      matcher(" application_1429587312661_0024 ");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = ShellClusterProvider.YARN_APPLICATION_ID_REGEX.
      matcher("\tapplication_1429587312661_0024\t");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = ShellClusterProvider.YARN_APPLICATION_ID_REGEX.
      matcher(" application_11111111111111111_9999924 ");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_11111111111111111_9999924", matcher.group(1));
    matcher = ShellClusterProvider.YARN_APPLICATION_ID_REGEX.
      matcher("\tapplication_1429587312661_0024");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
  }
}
