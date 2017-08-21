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
import com.streamsets.pipeline.lib.util.FileContextProviderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseFileContextProvider implements FileContextProvider{
  private static final Logger LOG = LoggerFactory.getLogger(BaseFileContextProvider.class);

  protected List<FileContext> fileContexts;
  private int startingIdx;
  private int currentIdx;
  private int loopIdx;

  BaseFileContextProvider() {
    startingIdx = 0;
  }

  /**
   * Sets the file offsets to use for the next read. To work correctly, the last return offsets should be used or
   * an empty <code>Map</code> if there is none.
   * <p/>
   * If a reader is already live, the corresponding set offset is ignored as we cache all the contextual information
   * of live readers.
   *
   * @param offsets directory offsets.
   * @throws IOException thrown if there was an IO error while preparing file offsets.
   */
  @Override
  public void setOffsets(Map<String, String> offsets) throws IOException{
    Utils.checkNotNull(offsets, "offsets");
    // retrieve file:offset for each directory
    for (FileContext fileContext : fileContexts) {
      String offset = offsets.get(fileContext.getMultiFileInfo().getFileKey());
      LiveFile file = null;
      long fileOffset = 0;
      if (offset != null && !offset.isEmpty()) {
        file = FileContextProviderUtil.getRefreshedLiveFileFromFileOffset(offset);
        fileOffset = FileContextProviderUtil.getLongOffsetFromFileOffset(offset);
      }
      fileContext.setStartingCurrentFileName(file);
      fileContext.setStartingOffset(fileOffset);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Setting offset: directory '{}', file '{}', offset '{}'",
            fileContext.getMultiFileInfo().getFileFullPath(), file, fileOffset);
      }
    }
    currentIdx = startingIdx;
  }

  @Override
  public Map<String, Long> getOffsetsLag(Map<String, String> offsetMap) throws IOException {
    Map<String, Long> offsetLagMap = new HashMap<String, Long>();
    for (FileContext fileContext : fileContexts) {
      String fileKey = fileContext.getMultiFileInfo().getFileKey();
      if (fileContext.hasReader()) {
        //Whatever does not have offsets (i.e empty offsets) mean their live (current) file is null
        //they will appear in pending files.
        if (offsetMap.containsKey(fileKey) && !offsetMap.get(fileKey).isEmpty()) {
          offsetLagMap.put(fileKey, FileContextProviderUtil.getOffsetLagForFile(offsetMap.get(fileKey)));
        }
     }
    }
    return offsetLagMap;
  }

  @Override
  public Map<String, Long> getPendingFiles() throws IOException{
    Map<String, Long> pendingFiles = new HashMap<String, Long>();
    for (FileContext context : fileContexts) {
      pendingFiles.put(context.getMultiFileInfo().getFileKey(), context.getPendingFiles());
    }
    return pendingFiles;
  }


  @Override
  public Map<String, String> getOffsets() throws IOException {
    Map<String, String> map = new HashMap<>();
    // produce file:offset for each directory taking into account a current reader and its file state.
    for (FileContext fileContext : fileContexts) {
      LiveFile file;
      long fileOffset;
      if (!fileContext.hasReader()) {
        file = fileContext.getStartingCurrentFileName();
        fileOffset = fileContext.getStartingOffset();
      } else if (fileContext.getReader().hasNext()) {
        file = fileContext.getReader().getLiveFile();
        fileOffset = fileContext.getReader().getOffset();
      } else {
        file = fileContext.getReader().getLiveFile();
        fileOffset = Long.MAX_VALUE;
      }

      String offset = (file == null) ? "" : FileContextProviderUtil.createFileOffsetString(fileOffset, file);
      map.put(fileContext.getMultiFileInfo().getFileKey(), offset);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Reporting offset: directory '{}', pattern: '{}', file '{}', offset '{}'",
            fileContext.getMultiFileInfo().getFileFullPath(),
            fileContext.getRollMode().getPattern(),
            file,
            fileOffset
        );
      }
    }
    startingIdx = getAndIncrementIdx();
    loopIdx = 0;
    return map;
  }

  @Override
  public FileContext next() {
    loopIdx++;
    return fileContexts.get(getAndIncrementIdx());
  }

  @Override
  public boolean didFullLoop() {
    return loopIdx >= fileContexts.size();
  }

  @Override
  public void startNewLoop() {
    LOG.trace("startNewLoop()");
    loopIdx = 0;
  }

  @Override
  public void close() {
    for (FileContext fileContext : fileContexts) {
      fileContext.close();
    }
  }

  @Override
  public void purge() {
    //NOP
  }


  private int getAndIncrementIdx() {
    int idx = currentIdx;
    incrementIdx();
    return idx;
  }

  private int incrementIdx() {
    currentIdx = (fileContexts.isEmpty()) ? 0 : (currentIdx + 1) % fileContexts.size();
    return currentIdx;
  }

  protected void resetCurrentAndStartingIdx() {
    currentIdx = 0;
    startingIdx = 0;
  }

}
