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
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.Stage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class TestBaseStage {
  private Stage.Info info = Mockito.mock(Stage.Info.class);
  private Stage.Context context = Mockito.mock(Stage.Context.class);
  private boolean inited;

  public class TBaseStage extends BaseStage<Stage.Context> {

    @Override
    protected List<ConfigIssue> init() {
      List<ConfigIssue> issues = super.init();
      Assert.assertEquals(info, getInfo());
      Assert.assertEquals(context, getContext());
      inited = true;
      return issues;
    }

  }

  @Before
  public void before() {
    inited = false;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBaseStage() throws Exception {
    Stage stage = new TBaseStage();
    stage.init(info, context);
    Assert.assertTrue(inited);
    stage.destroy();
  }

}
