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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class GlobDirectoryStream implements DirectoryStream<Path> {
  private static final Logger LOG = LoggerFactory.getLogger(GlobDirectoryStream.class);

  private final AtomicInteger openCounter;
  private final DirectoryStream<Path> directoryStream;
  private final Iterator<Path> iterator;

  public GlobDirectoryStream(Path basePath, Path globPath) throws IOException {
    this(basePath, globPath, new Filter<Path>() {
      @Override
      public boolean accept(Path entry) throws IOException {
        return true;
      }
    }, new AtomicInteger());
  }

  public GlobDirectoryStream(Path basePath, Path globPath, DirectoryStream.Filter<Path> filter) throws IOException {
    this(basePath, globPath, filter, new AtomicInteger());
  }

  public int getOpenCounter() {
    return openCounter.get();
  }

  GlobDirectoryStream(Path basePath, Path globPath, final DirectoryStream.Filter<Path> filter,
      AtomicInteger openCounter) throws IOException {
    Utils.checkNotNull(basePath, "basePath");
    Utils.checkNotNull(globPath, "globPath");
    Utils.checkNotNull(filter, "filter");
    LOG.trace("<init>(basePath={}, globPath={}", basePath, globPath);
    this.openCounter = openCounter;
    if (globPath.getNameCount() == 1) {
      final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + globPath.getName(0).toString());
      DirectoryStream.Filter<Path> globFilter = new Filter<Path>() {
        @Override
        public boolean accept(Path entry) throws IOException {
          return matcher.matches(entry.getFileName()) && filter.accept(entry);
        }
      };
      directoryStream = Files.newDirectoryStream(basePath, globFilter);
      openCounter.incrementAndGet();
      iterator = directoryStream.iterator();
    } else {
      String firstGlob = globPath.getName(0).toString();
      final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + firstGlob);
      DirectoryStream.Filter<Path> globFilter = new Filter<Path>() {
        @Override
        public boolean accept(Path entry) throws IOException {
          return matcher.matches(entry.getFileName());
        }
      };
      DirectoryStream<Path> baseDirectoryStream = Files.newDirectoryStream(basePath, globFilter);
      openCounter.incrementAndGet();
      String newGlobPathName = globPath.getName(1).toString();
      String[] children = new String[globPath.getNameCount() - 2];
      for (int i = 2; i < globPath.getNameCount(); i++) {
        children[i - 2] = globPath.getName(i).toString();
      }
      final Path newGlobPath = Paths.get(newGlobPathName, children);
      directoryStream = new GlobDirectoryStream(baseDirectoryStream, newGlobPath, filter, openCounter);
      openCounter.incrementAndGet();
      iterator = directoryStream.iterator();
    }
  }

  private DirectoryStream<Path> childDirectoryStream = null;

  GlobDirectoryStream(final DirectoryStream<Path> directoryStream, final Path globPath,
      final DirectoryStream.Filter<Path> filter, final AtomicInteger openCounter) throws IOException {
    this.openCounter = openCounter;
    this.directoryStream = directoryStream;
    final Iterator<Path> dirIterator = directoryStream.iterator();
    iterator = new Iterator<Path>() {
      Iterator<Path> it = Collections.emptyIterator();

      @Override
      public boolean hasNext() {
        try {
          while (!it.hasNext() && dirIterator.hasNext()) {
            if (childDirectoryStream != null) {
              childDirectoryStream.close();
              childDirectoryStream = null;
              openCounter.decrementAndGet();
            }
            Path basePath = dirIterator.next();
            if (Files.isDirectory(basePath)) {
              childDirectoryStream = new GlobDirectoryStream(basePath, globPath, filter, openCounter);
              openCounter.incrementAndGet();
              it = childDirectoryStream.iterator();
            }
          }
          boolean hasNext = it.hasNext();
          if (!hasNext) {
            if (childDirectoryStream != null) {
              childDirectoryStream.close();
              childDirectoryStream = null;
              openCounter.decrementAndGet();
            }
          }
          return hasNext;
        } catch (IOException ex) {
          throw  new RuntimeException(ex);
        }
      }

      @Override
      public Path next() {
        Utils.checkState(hasNext(), "Iterator does not have more elements");
        return it.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public Iterator<Path> iterator() {
    return iterator;
  }

  @Override
  public void close() throws IOException {
    try {
      if (childDirectoryStream != null) {
        childDirectoryStream.close();
        childDirectoryStream = null;
        openCounter.decrementAndGet();
      }
    } finally {
      directoryStream.close();
      openCounter.decrementAndGet();
    }
  }

}
