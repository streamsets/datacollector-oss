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
package com.streamsets.datacollector.restapi;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.DPMInfoJson;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Singleton;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.io.IOException;

import static org.mockito.Mockito.mock;

public class TestAdminResource extends JerseyTest {

  @Test
  public void testEnableDPM() throws IOException {
    DPMInfoJson dpmInfo = new DPMInfoJson();
    dpmInfo.setBaseURL("http://dpmbaseURL");
    dpmInfo.setUserID("admin@admin");
    dpmInfo.setUserPassword("admin@admin");
    dpmInfo.setOrganization("admin");
    dpmInfo.setLabels(ImmutableList.of("l1", "l2"));
    boolean exceptionTriggered = false;
    Response response = null;
    try {
      response = target("/v1/system/enableDPM")
          .request()
          .header("X-Requested-By", "SDC")
          .post(Entity.json(dpmInfo));
    } catch (Exception e) {
      exceptionTriggered = true;
    } finally {
      if (response != null) {
        response.close();
      }
    }
    Assert.assertTrue(exceptionTriggered);

    // test for null check
    exceptionTriggered = false;
    response = null;
    try {
      response = target("/v1/system/enableDPM")
          .request()
          .header("X-Requested-By", "SDC")
          .post(null);
    } catch (ProcessingException e) {
      Assert.assertTrue(e.getCause().getMessage().contains("DPMInfo cannot be null"));
      exceptionTriggered = true;
    } finally {
      if (response != null) {
        response.close();
      }
    }
    Assert.assertTrue(exceptionTriggered);
  }

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(new AdminResourceConfig());
        register(AdminResource.class);
      }
    };
  }

  static class AdminResourceConfig extends AbstractBinder {
    @Override
    protected void configure() {
      bindFactory(RuntimeInfoTestInjector.class).to(RuntimeInfo.class);
      bindFactory(ConfigurationTestInjector.class).to(Configuration.class);
      bindFactory(UserGroupManagerTestInjector.class).to(UserGroupManager.class);
      bindFactory(SupportBundleTestInjector.class).to(SupportBundleManager.class);
    }
  }


  public static class UserGroupManagerTestInjector implements Factory<UserGroupManager> {
    @Singleton
    @Override
    public UserGroupManager provide() {
      UserGroupManager userGroupManager = mock(UserGroupManager.class);
      return userGroupManager;
    }

    @Override
    public void dispose(UserGroupManager userGroupManager) {
    }

  }


  public static class SupportBundleTestInjector implements Factory<SupportBundleManager> {
    @Singleton
    @Override
    public SupportBundleManager provide() {
      SafeScheduledExecutorService service = mock(SafeScheduledExecutorService.class);
      Configuration configuration = mock(Configuration.class);
      PipelineStoreTask pipelineStoreTask = mock(PipelineStoreTask.class);
      PipelineStateStore stateStore = mock(PipelineStateStore.class);
      SnapshotStore snapshotStore = mock(SnapshotStore.class);
      BlobStoreTask blobStore = mock(BlobStoreTask.class);
      RuntimeInfo runtimeInfo = mock(RuntimeInfo.class);
      BuildInfo buildInfo = mock(BuildInfo.class);
      StatsCollector statsCollector = mock(StatsCollector.class);
      return new SupportBundleManager(
        service,
        configuration,
        pipelineStoreTask,
        stateStore,
        snapshotStore,
        blobStore,
        runtimeInfo,
        buildInfo,
        statsCollector
      );
    }

    @Override
    public void dispose(SupportBundleManager manager) {
    }
  }
}

