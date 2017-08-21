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
package com.streamsets.datacollector.runner;

import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.Issues;

import org.junit.Assert;
import org.junit.Test;

public class TestPipelineRuntimeException {

  @Test
  @SuppressWarnings("unchecked")
  public void testConstructor1() {
    Issues issues = new Issues();
    PipelineRuntimeException ex = new PipelineRuntimeException(issues);
    Assert.assertEquals(issues, ex.getIssues());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConstructor2() {
    PipelineRuntimeException ex = new PipelineRuntimeException(ContainerError.CONTAINER_0151, "foo");
    Assert.assertNull(ex.getIssues());
  }

  @Test
  public void testErrorMessage() {
    Assert.assertNotNull(ContainerError.CONTAINER_0151.getMessage());
  }

}
