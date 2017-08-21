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
package com.streamsets.pipeline.stage.processor.xmlflattener;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class TestXMLFlatteningProcessorUpgrader {

  @Test
  public void testV1ToV2() throws StageException {
    XMLFlatteningProcessorUpgrader upgrader = new XMLFlatteningProcessorUpgrader();
    List<Config> upgraded = upgrader.upgrade(null, "xml", "xmll", 1, 2, new LinkedList<Config>());
    Assert.assertEquals(2, upgraded.size());
    Assert.assertEquals("keepOriginalFields", upgraded.get(0).getName());
    Assert.assertEquals(true, upgraded.get(0).getValue());
    Assert.assertEquals("newFieldOverwrites", upgraded.get(1).getName());
    Assert.assertEquals(false, upgraded.get(1).getValue());
  }

  @Test
  public void testV2ToV3() throws StageException {
    XMLFlatteningProcessorUpgrader upgrader = new XMLFlatteningProcessorUpgrader();
    List<Config> upgraded = upgrader.upgrade(null, "xml", "xmll", 2, 3, new LinkedList<Config>());
    Assert.assertEquals(1, upgraded.size());
    Assert.assertEquals("outputField", upgraded.get(0).getName());
    Assert.assertEquals("", upgraded.get(0).getValue());
  }

}
