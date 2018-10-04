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
package com.streamsets.pipeline.stage.origin.windows;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.stage.origin.windows.wineventlog.WinEventLogConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WindowsEventLogSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(WindowsEventLogSource.class);

  public WindowsEventLogSource(CommonConfigBean commonConf, WinEventLogConfigBean winEventLogConf) {
  }

  @Override
  public List<ConfigIssue> init() {
      return super.init();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    throw new UnsupportedOperationException("Windows Event Log Origin Is supported only in " + ExecutionMode.EDGE.name());
  }

}
