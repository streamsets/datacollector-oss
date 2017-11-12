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
package com.streamsets.datacollector.classpath;

import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

public class DependencyParserTest {

  @Test
  public void testParseJarName() {
    Optional<Dependency> parsedOptional = DependencyParser.parseJarName("source/test-0.1.jar", "test-0.1.jar");
    Assert.assertTrue(parsedOptional.isPresent());

    Dependency parsed = parsedOptional.get();
    Assert.assertNotNull(parsed);
    Assert.assertEquals("source/test-0.1.jar", parsed.getSourceName());
    Assert.assertEquals("test", parsed.getName());
    Assert.assertEquals("0.1", parsed.getVersion());
  }

  @Test
  public void testParseUrl() throws MalformedURLException {
    URL url = new URL("file:///home/streamsets/test-0.1.jar");
    Optional<Dependency> parsedOptional = DependencyParser.parseURL(url);
    Assert.assertTrue(parsedOptional.isPresent());

    Dependency parsed = parsedOptional.get();
    Assert.assertNotNull(parsed);
    Assert.assertEquals("file:/home/streamsets/test-0.1.jar", parsed.getSourceName());
    Assert.assertEquals("test", parsed.getName());
    Assert.assertEquals("0.1", parsed.getVersion());
  }

}
