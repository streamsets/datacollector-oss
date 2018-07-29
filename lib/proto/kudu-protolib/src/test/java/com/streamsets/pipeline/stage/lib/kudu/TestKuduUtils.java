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
package com.streamsets.pipeline.stage.lib.kudu;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Stage.Context;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduException;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    AsyncKuduClient.class,
})
@PowerMockIgnore({ "javax.net.ssl.*" })
public class TestKuduUtils {

  private static final String KUDU_MASTER = "localhost:7051";
  private AsyncKuduClient client;

  @Before
  public void setup() {
    client = new AsyncKuduClient.AsyncKuduClientBuilder(KUDU_MASTER).build();
  }

  @After
  public void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testCheckConnection() throws Exception{
    PowerMockito.stub(
        PowerMockito.method(AsyncKuduClient.class, "getTablesList"))
        .toThrow(PowerMockito.mock(KuduException.class));

    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Context context = getContext();
    KuduUtils.checkConnection(client, context, KUDU_MASTER, issues);
    Assert.assertEquals(1, issues.size());
    Stage.ConfigIssue issue = issues.get(0);
    Assert.assertTrue(issues.get(0).toString().contains(Errors.KUDU_00.name()));
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
  }
}
