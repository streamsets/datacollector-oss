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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.google.common.base.Strings;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvParser;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.junit.Assert;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.UUID;

public class CsvSpoolDirSourceTestUtil {
  private static String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private final static String LINE1 = "A,B";
  private final static String LINE2 = "a,b";
  private final static String LINE3 = "e,f";

  public static File createDelimitedFile() throws IOException {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    writer.write(LINE1 + "\n");
    writer.write(LINE2 + "\n");
    writer.write(LINE3 + "\n");
    writer.close();
    return f;
  }

  public static File createCustomDelimitedFile() throws IOException {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    writer.write("A^!B !^$^A\n");
    writer.close();
    return f;
  }

  public static File createSomeRecordsTooLongFile() throws IOException {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    writer.write("a,b,c,d\n");
    writer.write("e,f,g,h\n");
    writer.write("aaa,bbb,ccc,ddd\n");
    writer.write("i,j,k,l\n");
    writer.write("aa1,bb1,cc1,dd1\n");
    writer.write("aa2,bb2,cc2,dd2\n");
    writer.write("m,n,o,p\n");
    writer.write("q,r,s,t\n");
    writer.write("aa3,bb3,cc3,dd3\n");
    writer.write("aa4,bb5,cc5,dd5\n");
    writer.close();
    return f;
  }

  public static File createCommentFile() throws IOException {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    writer.write("a,b\n");
    writer.write("# This is comment\n");
    writer.write("c,d\n");
    writer.close();
    return f;
  }

  public static File createEmptyLineFile() throws IOException {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    writer.write("a,b\n");
    writer.write("\n");
    writer.write("c,d\n");
    writer.close();
    return f;
  }

  public static SpoolDirConfigBean getConf(
      CsvMode mode,
      CsvHeader header,
      char delimiter,
      char escape,
      char quote,
      boolean commentsAllowed,
      char comment,
      boolean ignoreEmptyLines,
      int maxLen,
      CsvRecordType csvRecordType,
      String filePath,
      String pattern,
      PostProcessingOptions postProcessing,
      int batchSize
  ) {
    DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();
    dataFormatConfig.charset = "UTF-8";
    dataFormatConfig.removeCtrlChars = false;
    dataFormatConfig.csvParser = CsvParser.LEGACY_PARSER;
    dataFormatConfig.csvFileFormat = mode;
    dataFormatConfig.csvHeader = header;
    dataFormatConfig.csvMaxObjectLen = maxLen;
    dataFormatConfig.csvCustomDelimiter = delimiter;
    dataFormatConfig.csvCustomEscape = escape;
    dataFormatConfig.csvCustomQuote = quote;
    dataFormatConfig.csvRecordType = csvRecordType;
    dataFormatConfig.csvEnableComments = commentsAllowed;
    dataFormatConfig.csvCommentMarker = comment;
    dataFormatConfig.csvIgnoreEmptyLines = ignoreEmptyLines;
    dataFormatConfig.onParseError = OnParseError.ERROR;
    dataFormatConfig.maxStackTraceLines = 0;
    dataFormatConfig.compression = Compression.NONE;
    dataFormatConfig.filePatternInArchive = "*";

    return new SpoolBaseSourceBuilder()
        .dataFormat(DataFormat.DELIMITED)
        .dataFormatConfig(dataFormatConfig)
        .spoolDir(Strings.isNullOrEmpty(filePath) ? createTestDir() : filePath)
        .filePattern(Strings.isNullOrEmpty(pattern) ? "file-[0-9].log" : pattern)
        .postProcessing(postProcessing)
        .archiveDir(createTestDir())
        .batchSize(batchSize)
        .getConf();
  }
}
