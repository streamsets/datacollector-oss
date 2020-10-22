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
package com.streamsets.pipeline.lib.dirspooler;

import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class SpoolDirUtil {
  private SpoolDirUtil() {}
  private final static Logger LOG = LoggerFactory.getLogger(SpoolDirUtil.class);

  private static final String SLASH = "/";
  private static final String ASTERISK = "*";
  private static final String ESCAPED_ASTERISK = "\\*";

  /**
   * True if f1 is "newer" than f2.
   */
  public static boolean compareFiles(WrappedFileSystem fs, WrappedFile f1, WrappedFile f2) {
    if (!fs.exists(f2)) {
      return true;
    }

    try {
      long mtime1 = fs.getLastModifiedTime(f1);
      long mtime2 = fs.getLastModifiedTime(f2);

      long ctime1 = fs.getChangedTime(f1);
      long ctime2 = fs.getChangedTime(f2);

      long time1 = Math.max(mtime1, ctime1);
      long time2 = Math.max(mtime2, ctime2);

      int compares = Long.compare(time1, time2);

      if (compares != 0) {
        return compares > 0;
      }
    } catch (IOException ex) {
      LOG.error("Failed to get ctime: '{}'", f1.getFileName(), ex);
      return false;
    }

    return f1.getAbsolutePath().compareTo(f2.getAbsolutePath()) > 0;
  }

  /*
   * Returns the {@code DataParser} of the file, could be local file and hdfs file
   *
   * @return  the {@code DataParser} of the file
   */
  public static DataParser getParser(
      WrappedFileSystem fs,
      WrappedFile file,
      DataFormat dataFormat,
      DataParserFactory parserFactory,
      String offset,
      int wholeFileMaxObjectLen,
      ELEval rateLimitElEval,
      ELVars rateLimitElVars,
      String rateLimit
  ) throws DataParserException, ELEvalException, IOException {
    DataParser parser;

    switch (dataFormat) {
      case WHOLE_FILE:
        FileRef fileRef = fs.getFileRefBuilder()
            .filePath(file.getAbsolutePath())
            .bufferSize(wholeFileMaxObjectLen)
            .rateLimit(FileRefUtil.evaluateAndGetRateLimit(rateLimitElEval, rateLimitElVars, rateLimit))
            .createMetrics(true)
            .totalSizeInBytes(file.getSize())
            .build();
        parser = parserFactory.getParser(file.getFileName(), file.getFileMetadata(), fileRef);
        break;
      default:
        parser = parserFactory.getParser(file.getFileName(), file.getInputStream(), offset);
    }

    return parser;
  }

  /**
   * Returns true if the directory string contains *
   *
   * @param directory String containing the path to the directory
   * @return true if the directory contains *
   */
  public static boolean isGlobPattern(String directory) {
    return directory.contains(ASTERISK);
  }

  /**
   * Returns the absolute path from the root to the last base directory
   * Meaning the path until the last / before the first *
   *
   * @param directory String containing the path to the directory
   * @return the path truncated to the las complete directory
   */
  public static String truncateGlobPatternDirectory(String directory) {
    String[] absolutePath = directory.split(ESCAPED_ASTERISK);
    String truncatedString = absolutePath[0];

    if (lastCharacterIsAsterisk(truncatedString) != SLASH.toCharArray()[0]) {
      List<String> subDirectories = Arrays.asList(truncatedString.split(SLASH));
      StringBuffer stringBuffer = new StringBuffer();
      stringBuffer.append(String.join(SLASH, subDirectories.subList(0, subDirectories.size() - 1))).append(SLASH);
      truncatedString = stringBuffer.toString();
    }

    LOG.debug(String.format("Checking existence of path: %s", truncatedString));
    return truncatedString;
  }

  private static char lastCharacterIsAsterisk(String truncatedString) {
    char[] charArray = truncatedString.toCharArray();
    return charArray[charArray.length - 1];
  }

  /**
   * Returns a filename to be used by the file field of Offset.
   * An input filename should be absolute, if the base dir contains a glob pattern,
   * the input filename will be returned as is, otherwise a path relative to the base dir is returned.
   * FIXME: This method should always return relative paths. In case of glob patterns it should return a file path
   * relative to the longest part of the base dir that doesn't contain glob patterns.
   *
   * @param absoluteFilename - absolute file name.
   * @param baseDir - base directory for the resulting relative file path.
   * @return filename to be used by offset.
   */
  public static String getFilenameForOffsetFile(final String absoluteFilename, final String baseDir) {
    Utils.checkState(Paths.get(absoluteFilename).isAbsolute(),
        "Full filename is not absolute [" + absoluteFilename + "]");

    if (isGlobPattern(baseDir)) {
      return absoluteFilename;
    } else {
      return absoluteFilename.replaceFirst(baseDir + SpoolDirRunnable.FILE_SEPARATOR, "");
    }
  }

  /**
   * Returns a file whose path is constructed from a relative filename and a base directory to which the filename
   * is relative.
   * //FIXME: When the previous method is fixed this method should be reimplemented and return a file considering that:
   * 1) All filenames are relative
   * 2) If a base dir has a glob pattern, then the file name is relative to the longest part of the base dir
   * that doesn't contain glob patterns.
   *
   * @param fs - file system of the file.
   * @param baseDir - base directory of the file
   * @param filename - path to the file relative to the base dir, can be absolute path if the base dir contains glob patterns.
   * @return A file on the file system.
   * @throws IOException - if we cannot construct a file object for a file on a file system.
   */
  public static WrappedFile getFileFromOffsetFile(
      final WrappedFileSystem fs, final String baseDir, final String filename
  ) throws IOException {
    if (filename.equals(Offset.NULL_FILE)) {
      return fs.getFile(Offset.NULL_FILE);
    }

    if (Paths.get(filename).isAbsolute()) {
      if (!isGlobPattern(baseDir)) {
        LOG.warn("Absolute filename [" + filename + "] requires a glob pattern [" + baseDir + "]");
        return fs.getFile(baseDir, filename);
      }
      return fs.getFile(filename);
    }

    Utils.checkState(!isGlobPattern(baseDir),
        "Glob pattern [" + baseDir + "] is not supported with filename [" + filename + "]");

    return fs.getFile(baseDir, filename);
  }
}