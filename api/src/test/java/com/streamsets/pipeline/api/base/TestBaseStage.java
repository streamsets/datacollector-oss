/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Stage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBaseStage {
  private Stage.Info info = Mockito.mock(Stage.Info.class);
  private Stage.Context context = Mockito.mock(Stage.Context.class);
  private boolean inited;

  public class TBaseStage extends BaseStage<Stage.Context> {

    @Override
    protected void init() throws StageException {
      super.init();
      Assert.assertEquals(info, getInfo());
      Assert.assertEquals(context, getContext());
      inited = true;
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
    stage.validateConfigs(info, context);
    Assert.assertTrue(inited);
    stage.destroy();
  }

}
