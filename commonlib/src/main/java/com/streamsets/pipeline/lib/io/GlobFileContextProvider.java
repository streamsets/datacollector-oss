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
import com.streamsets.pipeline.config.FileRollMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.util.GlobFilePathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
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
      //For Periodic Pattern Roll mode, we will just use the parent path of
      //the file for globbing and we will filter just the directories.
      this.finderPath =  (globFileInfo.getFileRollMode() == FileRollMode.PATTERN)?
          Paths.get(globFileInfo.getFileFullPath()).getParent()
          : Paths.get(globFileInfo.getFileFullPath());

      FileFilterOption filterOption = (globFileInfo.getFileRollMode() == FileRollMode.PATTERN) ?
          FileFilterOption.FILTER_DIRECTORIES_ONLY
          : FileFilterOption.FILTER_REGULAR_FILES_ONLY;

      this.fileFinder = (scanIntervalSecs == 0) ? new SynchronousFileFinder(finderPath, filterOption)
          : new AsynchronousFileFinder(finderPath, scanIntervalSecs, executor, filterOption);
    }

    public MultiFileInfo getFileInfo(Path path) {
      //For Periodic Pattern Roll Mode we will only watch for path of the parent
      //Once we resolve the filepath for parent
      //we will attach the final file name to the path to do the periodic pattern match.
      if (globFileInfo.getFileRollMode() == FileRollMode.PATTERN)  {
        return new MultiFileInfo(
            globFileInfo,
            path.toString() + File.separatorChar + Paths.get(globFileInfo.getFileFullPath()).getFileName()
        );
      } else {
        return new MultiFileInfo(
            globFileInfo,
            path.toString()
        );
      }
    }

    public Set<Path> find() throws IOException {
      return fileFinder.find();
    }

    public boolean forget(MultiFileInfo multiFileInfo) {
      return (multiFileInfo.getFileRollMode() == FileRollMode.PATTERN) ?
          fileFinder.forget(Paths.get(multiFileInfo.getFileFullPath()).getParent()) :
          fileFinder.forget(Paths.get(multiFileInfo.getFileFullPath()));
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
  private boolean inPreviewMode;


  private boolean allowForLateDirectoryCreation;
  private Map<Path, MultiFileInfo> nonExistingPaths = new HashMap<Path, MultiFileInfo>();
  private ScheduledExecutorService executor;
  private DirectoryPathCreationWatcher directoryWatcher = null;

  public GlobFileContextProvider(
      boolean allowForLateDirectoryCreation,
      List<MultiFileInfo> fileInfos,
      int scanIntervalSecs,
      Charset charset,
      int maxLineLength,
      PostProcessingOptions postProcessing,
      String archiveDir,
      FileEventPublisher eventPublisher,
      boolean inPreviewMode) throws IOException {
    super();
    // if scan interval is zero the GlobFileInfo will work synchronously and it won't require an executor
    globFileInfos = new CopyOnWriteArrayList<GlobFileInfo>();
    fileContexts = new ArrayList<>();

    this.allowForLateDirectoryCreation = allowForLateDirectoryCreation;
    this.scanIntervalSecs = scanIntervalSecs;
    this.charset = charset;
    this.maxLineLength = maxLineLength;
    this.postProcessing = postProcessing;
    this.archiveDir = archiveDir;
    this.eventPublisher = eventPublisher;
    this.inPreviewMode = inPreviewMode;

    executor = (scanIntervalSecs == 0) ? null :
        new SafeScheduledExecutorService(fileInfos.size() / 3 + 1, "File Finder");

    for (MultiFileInfo fileInfo : fileInfos) {
      if (!checkForNonExistingPath(fileInfo)) {
        addToContextOrGlobFileInfo(fileInfo);
      }
    }

    if (allowForLateDirectoryCreation && !nonExistingPaths.isEmpty()) {
      directoryWatcher = new DirectoryPathCreationWatcher(nonExistingPaths.keySet(), this.scanIntervalSecs);
    }

    LOG.debug("Created");
  }

  private void addToContextOrGlobFileInfo(MultiFileInfo fileInfo) throws IOException {
    //Make sure if it is a periodic pattern roll mode and there is no globbing in the parent path
    //if so add it to globFileInfo
    if (fileInfo.getFileRollMode() == FileRollMode.PATTERN
        && !GlobFilePathUtil.hasGlobWildcard(fileInfo.getFileFullPath().replaceAll("\\$\\{"+"PATTERN"+"\\}", "")))
    {
      fileContexts.add(
          new FileContext(
              fileInfo,
              charset,
              maxLineLength,
              postProcessing,
              archiveDir,
              eventPublisher,
              inPreviewMode
          )
      );
    } else {
      //If scanIntervalSecs == 0, the GlobFile Info doc says it is synchronous it does not need a executor.
      globFileInfos.add(new GlobFileInfo(fileInfo, (scanIntervalSecs == 0)? null : executor, scanIntervalSecs));
    }
  }

  private boolean checkForNonExistingPath(MultiFileInfo multiFileInfo) throws IOException {
    Path pathToSearchFor = GlobFilePathUtil.getPivotPath(Paths.get(multiFileInfo.getFileFullPath()).getParent());
    boolean exists = Files.exists(pathToSearchFor);
    if (!exists) {
      if (!allowForLateDirectoryCreation) {
        throw new IOException(Utils.format("Path does not exist:{}", pathToSearchFor));
      } else {
        nonExistingPaths.put(pathToSearchFor, multiFileInfo);
        return true;
      }
    }
    return false;
  }

  private void findCreatedDirectories() throws IOException{
    if (allowForLateDirectoryCreation && !nonExistingPaths.isEmpty()) {
      for (Path foundPath : directoryWatcher.find()) {
        MultiFileInfo fileInfo = nonExistingPaths.get(foundPath);
        addToContextOrGlobFileInfo(fileInfo);
        nonExistingPaths.remove(foundPath);
        LOG.debug("Found Path '{}'", foundPath);
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
        FileContext fileContext = new FileContext(
            globfileInfo.getFileInfo(path),
            charset,
            maxLineLength,
            postProcessing,
            archiveDir,
            eventPublisher,
            inPreviewMode
        );
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

    if (directoryWatcher != null) {
      directoryWatcher.close();
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
