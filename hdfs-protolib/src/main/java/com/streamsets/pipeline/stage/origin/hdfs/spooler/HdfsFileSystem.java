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
package com.streamsets.pipeline.stage.origin.hdfs.spooler;

import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.WrappedFile;
import com.streamsets.pipeline.lib.dirspooler.WrappedFileSystem;
import com.streamsets.pipeline.lib.io.fileref.AbstractSpoolerFileRef;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import static com.streamsets.pipeline.lib.dirspooler.PathMatcherMode.GLOB;
import static com.streamsets.pipeline.lib.dirspooler.PathMatcherMode.REGEX;

public class HdfsFileSystem implements WrappedFileSystem {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsFileSystem.class);
  protected final FileSystem fs;
  private final String filePattern;
  protected final boolean processSubdirectories;
  private PathFilter filter;

  public HdfsFileSystem(String filePattern, PathMatcherMode mode, boolean processSubdirectories, FileSystem fs) {
    this.filePattern = filePattern;
    this.processSubdirectories = processSubdirectories;
    this.fs = fs;

    try {
      if (mode == GLOB) {
        filter = new GlobFilter(filePattern);
      } else if (mode == REGEX) {
        Pattern pattern = Pattern.compile(filePattern);
        filter = path -> pattern.matcher(path.toString()).matches();
      } else {
        throw new IllegalArgumentException("Unrecognized Path Matcher Mode: " + mode.getLabel());
      }
    } catch(IOException e) {
      throw new IllegalArgumentException("Can't create filter pattern: " + e.toString(), e);
    }
  }

  @Override
  public boolean exists(WrappedFile filePath) {
    try {
      return fs.exists(new Path(filePath.getAbsolutePath()));
    } catch (IOException ex) {
      LOG.error("failed to open file: '{}'", filePath.getFileName(), ex);
      return false;
    }
  }

  @Override
  public void delete(WrappedFile filePath) throws IOException {
    fs.delete(new Path(filePath.getAbsolutePath()), true);
  }

  @Override
  public void move(WrappedFile filePath, WrappedFile destFilePath) throws IOException {
    fs.rename(new Path(filePath.getAbsolutePath()), new Path(destFilePath.getAbsolutePath()));
  }

  @Override
  public long getLastModifiedTime(WrappedFile filePath) throws IOException {
    return fs.getFileStatus(new Path(filePath.getAbsolutePath())).getModificationTime();
  }

  @Override
  public long getChangedTime(WrappedFile filePath) throws IOException {
    // hadoop fs does not support changed timestamp
    return 0;
  }

  @Override
  public boolean isDirectory(WrappedFile filePath) {
    try {
      return fs.isDirectory(new Path(filePath.getAbsolutePath()));
    } catch (IOException ex) {
      LOG.error("failed to open file: '{}'", filePath.getFileName(), ex);
      return false;
    }
  }

  @Override
  public void addFiles(WrappedFile dirFile, WrappedFile startingFile, List<WrappedFile> toProcess, boolean includeStartingFile, boolean useLastModified) throws IOException {
    final long scanTime = System.currentTimeMillis();

    PathFilter pathFilter = new PathFilter() {
      @Override
      public boolean accept(Path entry) {
        try {
          FileStatus fileStatus = fs.getFileStatus(entry);
          if (fileStatus.isDirectory()) {
            return false;
          }

          if(!patternMatches(entry.getName())) {
            return false;
          }

          HdfsFile hdfsFile = new HdfsFile(fs, entry);
          // SDC-3551: Pick up only files with mtime strictly less than scan time.
          if (fileStatus.getModificationTime() < scanTime) {
            if (startingFile == null || startingFile.toString().isEmpty()) {
              toProcess.add(hdfsFile);
            } else {
              int compares = compare(hdfsFile, startingFile, useLastModified);
              if (includeStartingFile) {
                if (compares >= 0) {
                  toProcess.add(hdfsFile);
                }
              } else {
                if (compares > 0) {
                  toProcess.add(hdfsFile);
                }
              }
            }
          }
        } catch (IOException ex) {
          LOG.error("Failed to open file {}", entry.toString());
        }
        return false;
      }
    };

    fs.globStatus(new Path(dirFile.getAbsolutePath(), "*"), pathFilter);
  }

  @Override
  public void archiveFiles(WrappedFile archiveDirPath, List<WrappedFile> toProcess, long timeThreshold) throws IOException {
    PathFilter pathFilter = new PathFilter() {
      @Override
      public boolean accept(Path entry) {
        try {
          if(!patternMatches(entry.getName())) {
            return false;
          }

          if (timeThreshold - fs.getFileStatus(entry).getModificationTime() > 0) {
            toProcess.add(new HdfsFile(fs, entry));
          }
        } catch (IOException ex) {
          LOG.debug("Failed to open file {}", entry.toString());
        }
        return false;
      }
    };

    Path path = new Path(archiveDirPath.getAbsolutePath(), "*");
    fs.globStatus(path, pathFilter);

    if (processSubdirectories) {
      fs.globStatus(new Path(path, "*"), pathFilter);
    }
  }

  @Override
  public void addDirectory(WrappedFile dirPath, List<WrappedFile> directories) throws IOException {
    PathFilter pathFilter = new PathFilter() {
      @Override
      public boolean accept(Path entry) {
        try {
          FileStatus fileStatus = fs.getFileStatus(entry);
          if (fileStatus.isDirectory()) {
            if (processSubdirectories) {
              directories.add(new HdfsFile(fs, entry));
              addDirectory(getFile(entry.toString()), directories);
            }
            return false;
          }
        } catch (IOException ex) {
          LOG.error("Failed to open file {}", entry.toString(), ex);
        }
        return false;
      }
    };

    fs.globStatus(new Path(dirPath.getAbsolutePath(), "*"), pathFilter);

    if (!directories.contains(dirPath)) {
      directories.add(dirPath);
    }
  }

  @Override
  public WrappedFile getFile(String filePath) throws IOException {
    if (StringUtils.isEmpty(filePath)) {
      return new HdfsFile(fs, null);
    }
    Path path = new Path(filePath);
    return new HdfsFile(fs, path);
  }

  @Override
  public WrappedFile getFile(String dirPath, String filePath) throws IOException {
    if (isAbsolutePath(dirPath, filePath)) {
      return getFile(filePath);
    }
    if (filePath.startsWith(File.separator)) {
      filePath = filePath.replaceFirst(File.separator, "");
    }
    Path path = new Path(dirPath, filePath);
    return new HdfsFile(fs, path);
  }

  /*
   * Java File.isAbsolute method only checks whether the path begins with /
   * This is not enough since sometimes the method receives relative paths starting with /
   * We want to check whether the filePath already includes the directory path
   */
  private boolean isAbsolutePath(String dirPath, String filePath) {
    return filePath != null && filePath.startsWith(dirPath);
  }

  @Override
  public void mkdirs(WrappedFile filePath) {
    try {
      boolean result = fs.mkdirs(new Path(filePath.getAbsolutePath()));
      if (!result) {
        LOG.error("Could not create directory '{}", filePath.getAbsolutePath());
      }
    } catch (IOException ex) {
      LOG.error("Could not create directory '{}'", filePath.getAbsolutePath(), ex);
    }
  }

  @Override
  public boolean patternMatches(String fileName) {
     return filter.accept(new Path(fileName));
  }

  @Override
  public void handleOldFiles(WrappedFile dirpath, WrappedFile startingFile, boolean useLastModified, List<WrappedFile> toProcess) throws IOException {
    PathFilter pathFilter = new PathFilter() {
      @Override
      public boolean accept(Path entry) {
        if(!patternMatches(entry.getName())) {
          LOG.debug("Ignoring old file '{}' that do not match the file name pattern '{}'", entry.getName(), filePattern);
          return false;
        }

        if (startingFile == null) {
          return false;
        }

        if (compare(new HdfsFile(fs, entry), startingFile, useLastModified) < 0) {
          toProcess.add(new HdfsFile(fs, entry));
        }
        return false;
      }
    };

    Path path = new Path(dirpath.getAbsolutePath(), "*");
    fs.globStatus(path, pathFilter);

    if (processSubdirectories) {
      fs.globStatus(new Path(path, "*"), pathFilter);
    }
  }

  @Override
  public int compare(WrappedFile path1, WrappedFile path2, boolean useLastModified) {
    // why not just check if the file exists? Well, there is a possibility file gets moved/archived/deleted right after
    // that check. In that case we will still fail. So fail, and recover.
    try {
      if (path1 == null || path2 == null) {
        return 1;
      }

      final String filePath1 = path1.getAbsolutePath();
      final String filePath2 = path2.getAbsolutePath();

      if (useLastModified) {
        // if comparing with folder last modified timestamp, always return true
        if (filePath2.isEmpty() || !fs.exists(new Path(filePath2))) {
          return 1;
        }

        if (filePath1.isEmpty() || !fs.exists(new Path(filePath1))) {
          return 1;
        }

        long compares = getLastModifiedTime(path1) - getLastModifiedTime(path2);
        if (compares != 0) {
          return (int) compares;
        }
      }
    } catch (IOException ex) {
      LOG.debug("Starting file may have already been archived.", ex);
      return 1;
    } catch (RuntimeException ex) {
      LOG.error("Error while comparing files", ex);
      throw ex;
    }

    return path1.getAbsolutePath().compareTo(path2.getAbsolutePath());
  }

  @Override
  public Comparator<WrappedFile> getComparator(boolean useLastModified) {
    return new Comparator<WrappedFile>() {
      @Override
      public int compare(WrappedFile file1, WrappedFile file2) {
        try {
          if (useLastModified) {
            // if comparing with folder last modified timestamp, always return true
            if (file2.toString().isEmpty()) {
              return 1;
            }

            if (!exists(file1)) {
              return 1;
            }

            long mtime1 = getLastModifiedTime(file1);
            file1.getCustomMetadata().putIfAbsent(HeaderAttributeConstants.LAST_MODIFIED_TIME, mtime1);
            long mtime2 = getLastModifiedTime(file2);
            file2.getCustomMetadata().putIfAbsent(HeaderAttributeConstants.LAST_MODIFIED_TIME, mtime2);

            int compares = Long.compare(mtime1, mtime2);
            if (compares != 0) {
              return compares;
            }
          }
          return file1.getFileName().compareTo(file2.getFileName());
        } catch (IOException ex) {
          LOG.error("Could not sort files due to IO Exception", ex);
          throw new RuntimeException(ex);
        }
      }
    };
  }

  @Override
  public boolean findDirectoryPathCreationWatcher(List<WrappedFile> spoolDirPath) {
    // TODO: HDFS does not support DirectoryWatcher so returns always true
    return true;
  }

  @Override
  public AbstractSpoolerFileRef.Builder getFileRefBuilder() {
    return new HdfsFileRef.Builder().fileSystem(fs);
  }
}
