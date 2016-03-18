/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.FileRollMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.util.DirectoryPathCreationWatcher;
import com.streamsets.pipeline.lib.util.GlobFilePathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

public class GlobFileContextProvider extends BaseFileContextProvider {
  private static final Logger LOG = LoggerFactory.getLogger(GlobFileContextProvider.class);

  private static class GlobFileInfo implements Closeable {
    private final MultiFileInfo globFileInfo;
    private final FileFinder fileFinder;
    private final Path finderPath;

    // if scan interval is zero the GlobFileInfo will work synchronously and it won't require an executor
    public GlobFileInfo(MultiFileInfo globFileInfo, ScheduledExecutorService executor, int scanIntervalSecs) {
      this.globFileInfo = globFileInfo;
      finderPath = Paths.get(globFileInfo.getFileFullPath());
      this.fileFinder = (scanIntervalSecs == 0) ? new SynchronousFileFinder(finderPath)
          : new AsynchronousFileFinder(finderPath, scanIntervalSecs, executor);
    }

    public MultiFileInfo getFileInfo(Path path) {
      return new MultiFileInfo(globFileInfo, path.toString());
    }

    public Set<Path> find() throws IOException {
      return fileFinder.find();
    }

    public boolean forget(MultiFileInfo multiFileInfo) {
      return fileFinder.forget(Paths.get(multiFileInfo.getFileFullPath()));
    }

    @Override
    public void close() throws IOException {
      fileFinder.close();
    }

    @Override
    public String toString() {
      return Utils.format("GlobFileInfo [finderPath='{}']", finderPath);
    }
  }

  private final List<GlobFileInfo> globFileInfos;
  private final Charset charset;
  private final int maxLineLength;
  private final PostProcessingOptions postProcessing;
  private final String archiveDir;
  private final FileEventPublisher eventPublisher;
  private int scanIntervalSecs;


  private boolean allowForLateDirectoryCreation;
  private Map<Path, MultiFileInfo> nonExistingPaths = new HashMap<Path, MultiFileInfo>();
  private ScheduledExecutorService executor;

  DirectoryPathCreationWatcher directoryWatcher;

  public GlobFileContextProvider(
      boolean allowForLateDirectoryCreation,
      List<MultiFileInfo> fileInfos,
      int scanIntervalSecs,
      Charset charset,
      int maxLineLength,
      PostProcessingOptions postProcessing,
      String archiveDir,
      FileEventPublisher eventPublisher) throws IOException {
    super();
    // if scan interval is zero the GlobFileInfo will work synchronously and it won't require an executor
    globFileInfos = new CopyOnWriteArrayList<GlobFileInfo>();
    fileContexts = new ArrayList<>();
    executor = (scanIntervalSecs == 0) ? null : new SafeScheduledExecutorService(fileInfos.size() / 3 + 1, "FileFinder");
    this.allowForLateDirectoryCreation = allowForLateDirectoryCreation;
    this.scanIntervalSecs = scanIntervalSecs;

    for (MultiFileInfo fileInfo : fileInfos) {
      if (!checkForNonExistingPath(fileInfo)) {
        if (fileInfo.getFileRollMode() == FileRollMode.PATTERN) {
          // for periodic pattern roll mode we don't support wildcards
          fileContexts.add(new FileContext(fileInfo, charset, maxLineLength, postProcessing, archiveDir, eventPublisher));
        } else {
          globFileInfos.add(new GlobFileInfo(fileInfo, executor, scanIntervalSecs));
        }
      }
    }

    if (allowForLateDirectoryCreation && !nonExistingPaths.isEmpty()) {
      directoryWatcher = new DirectoryPathCreationWatcher(nonExistingPaths.keySet());
      if (executor == null) {
        executor = new SafeScheduledExecutorService(1, "Directory Creation Watcher");
      }
      executor.submit(directoryWatcher);
    }

    this.charset = charset;
    this.maxLineLength = maxLineLength;
    this.postProcessing = postProcessing;
    this.archiveDir = archiveDir;
    this.eventPublisher = eventPublisher;

    LOG.debug("Created");
  }

  private boolean checkForNonExistingPath(MultiFileInfo multiFileInfo) {
    if (allowForLateDirectoryCreation) {
      Path parentPath = Paths.get(multiFileInfo.getFileFullPath()).getParent();
      Path pathToSearchFor = parentPath;
      if (multiFileInfo.getFileRollMode() != FileRollMode.PATTERN) {
        pathToSearchFor = GlobFilePathUtil.getPivotPath(parentPath);
      }
      if (!Files.exists(pathToSearchFor)) {
        nonExistingPaths.put(pathToSearchFor, multiFileInfo);
        return true;
      }
    }
    return false;
  }
  
  private void findCreatedDirectories() throws IOException{
    if (allowForLateDirectoryCreation && !nonExistingPaths.isEmpty()) {
      Path processingPath;
      while ((processingPath = directoryWatcher.getCompletedPaths().poll()) != null) {
        MultiFileInfo fileInfo = nonExistingPaths.get(processingPath);
        if (fileInfo.getFileRollMode() != FileRollMode.PATTERN) {
          Path fileParentPath = Paths.get(fileInfo.getFileFullPath()).getParent();
          Path pathToSearchFor = GlobFilePathUtil.getPivotPath(fileParentPath);
          if (processingPath.equals(pathToSearchFor)) {
            globFileInfos.add(new GlobFileInfo(fileInfo, executor, scanIntervalSecs));
          }
        } else {
          fileContexts.add(new FileContext(fileInfo, charset, maxLineLength, postProcessing, archiveDir, eventPublisher));
        }
        nonExistingPaths.remove(processingPath);
      }
    }
  }

  private Map<FileContext, GlobFileInfo> fileToGlobFile = new HashMap<>();

  private void findNewFileContexts() throws IOException {
    //Thread fail safe
    Iterator<GlobFileInfo> iterator = globFileInfos.iterator();
    while (iterator.hasNext()) {
      GlobFileInfo globfileInfo = iterator.next();
      Set<Path> found = globfileInfo.find();
      for (Path path : found) {
        FileContext fileContext = new FileContext(globfileInfo.getFileInfo(path), charset, maxLineLength,
            postProcessing, archiveDir, eventPublisher);
        fileContexts.add(fileContext);
        fileToGlobFile.put(fileContext, globfileInfo);
        LOG.debug("Found '{}'", fileContext);
      }
    }
  }

  @Override
  public void purge() {
    Iterator<FileContext> iterator = fileContexts.iterator();
    boolean purgedAtLeastOne = false;
    while (iterator.hasNext()) {
      FileContext fileContext = iterator.next();
      if (!fileContext.isActive()) {
        fileContext.close();
        iterator.remove();
        if (fileToGlobFile.containsKey(fileContext)) {
          fileToGlobFile.get(fileContext).forget(fileContext.getMultiFileInfo());
        }
        LOG.debug("Removed '{}'", fileContext);
        purgedAtLeastOne = true;
      }
    }
    if (purgedAtLeastOne) {
      //reset loop counter to be within boundaries.
      resetCurrentAndStartingIdx();
      startNewLoop();
    }
  }

  /**
   * Sets the file offsets to use for the next read. To work correctly, the last return offsets should be used or
   * an empty <code>Map</code> if there is none.
   * <p/>
   * If a reader is already live, the corresponding set offset is ignored as we cache all the contextual information
   * of live readers.
   *
   * @param offsets directory offsets.
   * @throws java.io.IOException thrown if there was an IO error while preparing file offsets.
   */
  @Override
  public void setOffsets(Map<String, String> offsets) throws IOException {
    Utils.checkNotNull(offsets, "offsets");
    LOG.trace("setOffsets()");

    // We look for created directory paths here
    findCreatedDirectories();

    // we look for new files only here
    findNewFileContexts();

    // we purge file only here
    purge();

    super.setOffsets(offsets);

    startNewLoop();
  }

  @Override
  public void close() {
    LOG.debug("Closed");
    if (executor != null) {
      executor.shutdownNow();
    }
    for (GlobFileInfo globFileInfo : globFileInfos) {
      try {
        globFileInfo.close();
      } catch (IOException ex) {
        LOG.warn("Could not close '{}': {}", globFileInfo, ex.toString(), ex);
      }
    }
    //Close File Contexts
    super.close();
  }

}
