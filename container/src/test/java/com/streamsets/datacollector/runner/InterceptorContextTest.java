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
package com.streamsets.datacollector.runner;

import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.BlobStore;
import com.streamsets.pipeline.api.Processor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertNull;

public class InterceptorContextTest {

  private BlobStore blobStore;
  private Configuration configuration;
  private String stageInstanceName;
  private InterceptorContext context;

  @Before
  public void setUp() {
    this.blobStore = Mockito.mock(BlobStore.class);
    this.configuration = Mockito.mock(Configuration.class);
    this.stageInstanceName = "stageInstance";

    this.context = new InterceptorContext(blobStore, configuration, stageInstanceName);
  }

  @Test
  public void testCreateStageGuardAllowed() throws Exception {
    context.setAllowCreateStage(true);
    assertNull(context.createStage(null, Processor.class));
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateStageGuardNotAllowed() throws Exception {
    context.setAllowCreateStage(false);
    context.createStage(null, Processor.class);
  }
}
