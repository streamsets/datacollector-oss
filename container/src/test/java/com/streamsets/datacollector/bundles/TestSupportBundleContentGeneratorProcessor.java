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
package com.streamsets.datacollector.bundles;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSupportBundleContentGeneratorProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TestSupportBundleContentGeneratorProcessor.class);

  @Test
  public void testSerializingCustomGenerator() throws Exception {
    InputStream generatorResource = Thread.currentThread().getContextClassLoader().getResourceAsStream(SupportBundleContentGeneratorProcessor.RESOURCE_NAME);
    assertNotNull(generatorResource);

    BufferedReader reader = new BufferedReader(new InputStreamReader(generatorResource));
    String className;
    boolean found = false;
    while((className = reader.readLine()) != null) {
      LOG.debug("Found class: " + className);
      if(className.equals("com.streamsets.datacollector.bundles.content.SimpleGenerator")) {
        found = true;
        break;
      }
    }

    assertTrue(found);
  }
}
