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
package com.streamsets.datacollector.execution.preview.sync;

import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.preview.TestPreviewer;
import com.streamsets.datacollector.execution.runner.common.PipelineStopReason;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.preview.PreviewPipeline;
import com.streamsets.datacollector.runner.preview.PreviewPipelineRunner;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class TestSyncPreviewer extends TestPreviewer {

  protected Previewer createPreviewer() {
    return new SyncPreviewer(
        ID,
        "test-user",
        NAME,
        REV,
        previewerListener,
        objectGraph,
        Collections.emptyList(),
        p -> null,
        new HashMap<>()
    );
  }

  @Test
  public void testDestroyNotCalledTwice() throws Exception {
    SyncPreviewer previewer = (SyncPreviewer)createPreviewer();
    SyncPreviewer spyPreviewer = Mockito.spy(previewer);
    Pipeline pipeline = Mockito.mock(Pipeline.class);
    Mockito.doReturn(Collections.emptyList()).when(pipeline).init(true);
    Mockito.doNothing().when(pipeline).run(Mockito.anyList());
    Mockito.doNothing().when(pipeline).destroy(Mockito.anyBoolean(), Mockito.any());
    PreviewPipelineRunner previewPipelineRunner = Mockito.mock(PreviewPipelineRunner.class);
    Mockito.doReturn(null).when(previewPipelineRunner).getMetrics();
    Mockito.doReturn(null).when(previewPipelineRunner).getBatchesOutput();
    Mockito.doReturn(previewPipelineRunner).when(pipeline).getRunner();
    PreviewPipeline previewPipeline = new PreviewPipeline("","", pipeline, new Issues());
    Mockito.doReturn(previewPipeline).when(spyPreviewer).buildPreviewPipeline(Mockito.anyInt(), Mockito.anyInt(),
        Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean());
    spyPreviewer.start(1, 1, true, true,"", null,
        -1, false);
    Mockito.verify(pipeline, Mockito.times(1)).destroy(true, PipelineStopReason.FINISHED);
    // Check if preview returns non empty issue list
    Mockito.doReturn(Arrays.asList(Mockito.mock(Issue.class))).when(pipeline).init(true);
    try {
      spyPreviewer.start(1, 1, true, true,"", null,
          -1, false);
    } catch (Exception e) {
      // expected as issues is non empty
    }
    // check that destroy is still called, total times its called should be 2
    Mockito.verify(pipeline, Mockito.times(2)).destroy(true, PipelineStopReason.FINISHED);
  }
}
