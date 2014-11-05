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
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.util.Issue;
import jersey.repackaged.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestPipelineRuntimeException {

  @Test
  @SuppressWarnings("unchecked")
  public void testConstructor1() {
    List<Issue> issues = ImmutableList.of();
    PipelineRuntimeException ex = new PipelineRuntimeException(PipelineRuntimeException.ERROR.PIPELINE_CONFIGURATION,
                                                               issues);
    Assert.assertSame(issues, ex.getIssues());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConstructor2() {
    PipelineRuntimeException ex = new PipelineRuntimeException(PipelineRuntimeException.ERROR.PIPELINE_BUILD,
                                                               "foo");
    Assert.assertTrue(ex.getIssues().isEmpty());
  }

  @Test
  public void testErrorMessage() {
    Assert.assertNotNull(PipelineRuntimeException.ERROR.PIPELINE_BUILD.getMessageTemplate());
  }

}
