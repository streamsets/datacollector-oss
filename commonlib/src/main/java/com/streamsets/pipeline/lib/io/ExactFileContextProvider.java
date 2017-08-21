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
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PostProcessingOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ExactFileContextProvider extends BaseFileContextProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ExactFileContextProvider.class);

  private final Set<String> fileKeys;


  public ExactFileContextProvider(
      List<MultiFileInfo> fileInfos,
      Charset charset,
      int maxLineLength,
      PostProcessingOptions postProcessing,
      String archiveDir,
      FileEventPublisher eventPublisher,
      boolean inPreviewMode
  ) throws IOException {
    super();
    fileContexts = new ArrayList<>();
    fileKeys = new LinkedHashSet<>();
    for (MultiFileInfo dirInfo : fileInfos) {
      fileContexts.add(new FileContext(dirInfo, charset, maxLineLength, postProcessing, archiveDir, eventPublisher, inPreviewMode));
      if (fileKeys.contains(dirInfo.getFileKey())) {
        throw new IOException(Utils.format("File '{}' already specified, it cannot be added more than once",
            dirInfo.getFileKey()));
      }
      fileKeys.add(dirInfo.getFileKey());
    }
    LOG.debug("Opening files: {}", getFileKeys());
  }

  private Set<String> getFileKeys() {
    return fileKeys;
  }

  @Override
  public void close() {
    LOG.debug("Closing files: {}", fileKeys);
    //Close File Contexts
    super.close();
  }

}
