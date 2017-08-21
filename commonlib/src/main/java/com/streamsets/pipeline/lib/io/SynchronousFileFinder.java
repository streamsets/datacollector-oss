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
import com.streamsets.pipeline.lib.util.GlobFilePathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * It finds files matching the specified glob path pattern. Except for '**' all glob wildcards are supported at
 * any depth.
 * <p/>
 * This is a synchronous implementation.
 */
public class SynchronousFileFinder extends FileFinder {
  private final static Logger LOG = LoggerFactory.getLogger(SynchronousFileFinder.class);

  private final Path globPath;
  private final Path pivotPath;
  private final Path wildcardPath;
  private final Set<Path> foundPaths;
  private final DirectoryStream.Filter<Path> filter;

  public SynchronousFileFinder(Path globPath, FileFilterOption filterOption) {
    Utils.checkNotNull(globPath, "path");
    Utils.checkArgument(globPath.isAbsolute(), Utils.formatL("Path '{}' must be absolute", globPath));
    Utils.checkArgument(
        !globPath.toString().contains("**"),
        Utils.formatL("Path '{}' canot have double '*' wildcard", globPath)
    );
    this.globPath = globPath;
    pivotPath = GlobFilePathUtil.getPivotPath(globPath);
    wildcardPath = GlobFilePathUtil.getWildcardPath(globPath);
    foundPaths = Collections.synchronizedSet(new HashSet<Path>());
    if (wildcardPath == null) {
      filter = FileFilterOption.getFilter(this.foundPaths, FileFilterOption.NO_FILTER_OPTION);
    } else {
      filter = FileFilterOption.getFilter(this.foundPaths, filterOption);
    }
    LOG.trace("<init>(globPath={})", globPath);
  }


  Path getPivotPath() {
    return pivotPath;
  }

  Path getWildcardPath() {
    return wildcardPath;
  }

  @Override
  public Set<Path> find() throws IOException {
    Set<Path> newFound = new HashSet<>();
    if (getWildcardPath() != null || foundPaths.size() == 0) {
      if (getWildcardPath() == null) {
        if (Files.exists(getPivotPath()) && Files.isRegularFile(getPivotPath())) {
          newFound.add(getPivotPath());
          foundPaths.add(getPivotPath());
        }
      } else {
        try (DirectoryStream<Path> matches = new GlobDirectoryStream(getPivotPath(), getWildcardPath(), filter)) {
          for (Path found : matches) {
            newFound.add(found);
            foundPaths.add(found);
          }
        }
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Found '{}' new files for '{}'", newFound.size(), globPath);
    }
    return newFound;
  }

  @Override
  public boolean forget(Path path) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Forgetting '{}' for '{}'", path, path);
    }
    return foundPaths.remove(path);
  }

  @Override
  public void close() {
  }
}
