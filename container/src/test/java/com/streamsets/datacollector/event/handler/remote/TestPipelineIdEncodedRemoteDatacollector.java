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

package com.streamsets.datacollector.event.handler.remote;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.event.handler.DataCollector;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.lib.security.acl.dto.Acl;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class TestPipelineIdEncodedRemoteDatacollector {

  @Test
  public void testSavePipeline() throws Exception {
    RemoteDataCollector rc = Mockito.mock(RemoteDataCollector.class);
    PipelineIdEncodedRemoteDatacollector dc = Mockito.spy(new PipelineIdEncodedRemoteDatacollector(rc));
    dc.savePipeline("user", "name:foo", "rev", "desc", null, null, null, null, null, new HashMap<>());

    Map<String, Object> so = new HashMap<>();
    so.put(RemoteDataCollector.SCH_GENERATED_PIPELINE_NAME, "name:foo");
    so.put(RemoteDataCollector.IS_REMOTE_PIPELINE, true);
    Mockito.verify(rc, Mockito.times(1)).savePipeline(
        Mockito.eq("user"),
        Mockito.eq("name__foo"),
        Mockito.eq("rev"),
        Mockito.eq("desc"),
        Mockito.isNull(SourceOffset.class),
        Mockito.isNull(PipelineConfiguration.class),
        Mockito.isNull(RuleDefinitions.class),
        Mockito.isNull(Acl.class),
        Mockito.eq(so),
        Mockito.anyMap()
    );
  }


  @Test
  public void testStartPipeline() throws Exception {
    RemoteDataCollector rc = Mockito.mock(RemoteDataCollector.class);
    PipelineIdEncodedRemoteDatacollector dc = Mockito.spy(new PipelineIdEncodedRemoteDatacollector(rc));
    dc.start(null, "name:foo", "rev", new HashSet(Arrays.asList("all")));

    Mockito.verify(rc, Mockito.times(1)).start(
        Mockito.isNull(Runner.StartPipelineContext.class),
        Mockito.eq("name__foo"),
        Mockito.eq("rev"),
        Mockito.eq(new HashSet(Arrays.asList("all")))
    );
  }

  @Test
  public void testStopAndDeletePipeline() throws Exception {
    RemoteDataCollector rc = Mockito.mock(RemoteDataCollector.class);
    PipelineIdEncodedRemoteDatacollector dc = Mockito.spy(new PipelineIdEncodedRemoteDatacollector(rc));
    dc.stopAndDelete("user", "name:foo", "rev", 0);

    Mockito.verify(rc, Mockito.times(1)).stopAndDelete(
        Mockito.eq("user"),
        Mockito.eq("name__foo"),
        Mockito.eq("rev"),
        Mockito.eq(0L)
    );
  }

  @Test
  public void testOtherMethodsCalled() throws Exception {
    RemoteDataCollector rc = Mockito.mock(RemoteDataCollector.class);
    PipelineIdEncodedRemoteDatacollector dc = Mockito.spy(new PipelineIdEncodedRemoteDatacollector(rc));
    dc.init();
    dc.getPipelines();
    dc.getRemotePipelinesWithChanges();
    dc.blobStore("n", "i", 0, "c");
    dc.blobDelete("n", "i");
    dc.blobDelete("n", "i", 0);

    Mockito.verify(rc, Mockito.times(1)).init();
    Mockito.verify(rc, Mockito.times(1)).getPipelines();
    Mockito.verify(rc, Mockito.times(1)).getRemotePipelinesWithChanges();
    Mockito.verify(rc, Mockito.times(1)).blobStore(Mockito.eq("n"),
        Mockito.eq("i"), Mockito.eq(0L), Mockito.eq("c"));
    Mockito.verify(rc, Mockito.times(1)).blobDelete(Mockito.eq("n"),
        Mockito.eq("i"));
    Mockito.verify(rc, Mockito.times(1)).blobDelete(Mockito.eq("n"),
        Mockito.eq("i"), Mockito.eq(0L));

  }

}
