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
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SpoolDirUtil {
  private SpoolDirUtil() {}
  private final static Logger LOG = LoggerFactory.getLogger(SpoolDirUtil.class);

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

    return f1.getFileName().compareTo(f2.getFileName()) > 0;
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
}