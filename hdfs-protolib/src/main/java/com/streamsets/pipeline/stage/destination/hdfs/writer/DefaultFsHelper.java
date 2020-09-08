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
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import com.streamsets.pipeline.lib.hdfs.common.Errors;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

final class DefaultFsHelper implements FsHelper {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultFsHelper.class);

  // we use/reuse Path as they are expensive to create (it increases the performance by at least 3%)
  private final Path tempFilePath;
  private final LoadingCache<String, Path> dirPathCache;
  private final Stage.Context context;
  private final String uniquePrefix;
  private final RecordWriterManager recordWriterManager;
  private final ConcurrentLinkedQueue<Path> closedPaths;


  DefaultFsHelper(Stage.Context context,
                  String uniquePrefix,
                  ConcurrentLinkedQueue<Path> closedPaths,
                  RecordWriterManager recordWriterManager
  ) {
    this.recordWriterManager = recordWriterManager;
    this.context = context;
    this.uniquePrefix = uniquePrefix;
    tempFilePath = new Path(recordWriterManager.getTempFileName());
    this.closedPaths = closedPaths;
    dirPathCache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.MINUTES).build(
        new CacheLoader<String, Path>() {
          @Override
          public Path load(String key) throws Exception {
            return new Path(key, tempFilePath);
          }
        });
  }

  @Override
  public Path getPath(FileSystem fs, Date recordDate, Record record) throws StageException, IOException {
    // runUuid is fixed for the current pipeline run. it avoids collisions with other SDCs running the same/similar
    // pipeline
    try {
      return dirPathCache.get(recordWriterManager.getDirPath(recordDate, record));
    } catch (ExecutionException ex) {
      if (ex.getCause() instanceof StageException) {
        throw (StageException) ex.getCause();
      } else{
        throw new StageException(Errors.HADOOPFS_24, ex.toString(), ex);
      }
    }
  }

  @Override
  public void commitOldFiles(FileSystem fs) throws StageException, IOException {
    if (context.getLastBatchTime() > 0) {
      for (String glob : recordWriterManager.getGlobs()) {
        LOG.debug("Looking for uncommitted files using glob '{}'", glob);
        FileStatus[] globStatus = fs.globStatus(new Path(glob));
        if (globStatus != null) {
          for (FileStatus status : globStatus) {
            LOG.debug("Found uncommitted file '{}'", status.getPath());
            recordWriterManager.renameToFinalName(fs, status.getPath());
          }
        }
      }
    }
  }

  @Override
  public void handleAlreadyExistingFile(FileSystem fs, Path tempPath) throws StageException, IOException {
    Path path = recordWriterManager.renameToFinalName(fs, tempPath);
    LOG.warn("Path[{}] - Found previous file '{}', committing it", tempPath, path);
  }

  @Override
  public Path renameAndGetPath(FileSystem fs, Path inPath) throws IOException, StageException {
    Path tempPath = Path.getPathWithoutSchemeAndAuthority(inPath);

    Path finalPath =  new Path(tempPath.getParent(), (StringUtils.isEmpty(uniquePrefix) ? "" : (uniquePrefix + "_") ) + UUID.randomUUID().toString() + recordWriterManager.getExtension());
    if (!fs.rename(tempPath, finalPath)) {
      throw new IOException(Utils.format("Could not rename '{}' to '{}'", tempPath, finalPath));
    }
    // Store closed path so that we can generate event for it later
    closedPaths.add(finalPath);
    return finalPath;
  }

  @Override
  public OutputStream create(FileSystem fs, Path path) throws IOException {
    return new HflushableWrapperOutputStream(fs.create(path, false));
  }

  @Override
  public StreamCloseEventHandler<?> getStreamCloseEventHandler() {
    return null;
  }
}
