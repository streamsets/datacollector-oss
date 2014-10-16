/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.container;

import org.junit.Assert;
import org.junit.Test;

public class TestModuleInfo {

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor1() {
    new ModuleInfo(null, null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor2() {
    new ModuleInfo("n", null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor3() {
    new ModuleInfo("n", "v", null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor4() {
    new ModuleInfo("n", "v", "d", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyName() {
    new ModuleInfo("", "v", "d", "n");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyInstanceName() {
    new ModuleInfo("n", "v", "d", "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidName() {
    new ModuleInfo("pipeline", "v", "d", "n");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidInstanceName() {
    new ModuleInfo("n", "v", "d", "pipeline");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCharInName() {
    new ModuleInfo("n:n", "v", "d", "n");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCharInInstanceName() {
    new ModuleInfo("n", "v", "d", "n:n");
  }

  @Test
  public void testModuleInfo() {
    ModuleInfo info = new ModuleInfo("n", "v", "d", "in");
    Assert.assertEquals("n", info.getName());
    Assert.assertEquals("v", info.getVersion());
    Assert.assertEquals("d", info.getDescription());
    Assert.assertEquals("in", info.getInstanceName());
  }
}
