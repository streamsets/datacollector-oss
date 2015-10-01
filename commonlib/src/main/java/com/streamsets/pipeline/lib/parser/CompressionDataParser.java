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
package com.streamsets.pipeline.lib.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.Compression;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class CompressionDataParser extends AbstractDataParser {

  public static final String ZERO = "0";
  public static final String MINUS_ONE = "-1";
  public static final String PATH_SEPARATOR = "/";

  private final InputStream is;
  private final String id;
  private String offset;
  private final Compression compression;
  private final String compressionFilePattern;

  private final DataParserFactory dataParserFactory;

  public CompressionDataParser(
      String id,
      InputStream is,
      String offset,
      Compression compression,
      String compressionFilePattern,
      DataParserFactory dataParserFactory
  ) {
    this.id = id;
    this.is = is;
    this.offset = offset;
    this.compression = compression;
    this.compressionFilePattern = compressionFilePattern;
    this.dataParserFactory = dataParserFactory;
  }

  private DataParser parser;
  private boolean eof = false;
  private CompressionInput compressionInput;

  @Override
  public Record parse() throws IOException, DataParserException {
    if (compressionInput == null) {
      if(offset == null || offset.isEmpty()) {
        offset = ZERO;
      }
      // first invocation of this method on the parser. Initialize
      compressionInput = new CompressionInputBuilder(compression, compressionFilePattern, is, offset).build();
      offset = compressionInput.getStreamPosition(offset);
    }
    Record record = null;
    while (!eof && record == null) {
      if (parser == null) {
        InputStream nextInputStream = compressionInput.getNextInputStream();
        if (nextInputStream != null) {
          parser = dataParserFactory.getParser(id, nextInputStream, offset);
        } else {
          //reached end of compression/archive stream
          eof = true;
        }
      }
      if (!eof) {
        record = parser.parse();
        if (record == null) {
          // closing the parser will close the underlying stream. Do not close.
          parser = null;
          // for subsequent entries offset always starts at ZERO
          offset = ZERO;
        }
      }
    }
    return record;
  }

  @Override
  public String getOffset() throws IOException, DataParserException {
    if(eof) {
      return MINUS_ONE;
    }
    if(parser != null) {
      return compressionInput.wrapOffset(parser.getOffset());
    }
    return ZERO;
  }

  @Override
  public void close() throws IOException {
    if(parser != null) {
      parser.close();
      parser = null;
    }
    if(compressionInput != null) {
      compressionInput.close();
      compressionInput = null;
    }
    is.close();
  }

  interface CompressionInput {

    public InputStream getNextInputStream() throws IOException;

    public String wrapOffset(String offset) throws IOException;

    public String getStreamPosition(String offset) throws IOException;

    public String wrapRecordId(String recordId);

    public void close() throws IOException;
  }

  @VisibleForTesting
  static class CompressionInputBuilder {

    private final Compression compressionInputFormat;
    private final String compressedFilePattern;
    private final InputStream inputStream;
    private final String offset;

    public CompressionInputBuilder(
        Compression compressionInputFormat,
        String compressedFilePattern,
        InputStream inputStream,
        String offset
    ) {
      this.compressionInputFormat = compressionInputFormat;
      this.compressedFilePattern = compressedFilePattern;
      this.inputStream = inputStream;
      this.offset = offset;
    }

    public CompressionDataParser.CompressionInput build() throws IOException {
      if (compressionInputFormat != null) {
        switch (compressionInputFormat) {
          case NONE:
            return new None(inputStream);
          case COMPRESSED_FILE:
            return new CompressorInput(inputStream);
          case ARCHIVE:
            return new ArchiveInput(compressedFilePattern, new None(inputStream), offset);
          case COMPRESSED_ARCHIVE:
            return new ArchiveInput(compressedFilePattern, new CompressorInput(inputStream), offset);
          default:
            throw new IllegalArgumentException();
        }
      }
      return new None(inputStream);
    }

    @VisibleForTesting
    static class None implements CompressionDataParser.CompressionInput {

      private InputStream inputStream;

      public None(InputStream inputStream) {
        this.inputStream = inputStream;
      }

      @Override
      public String wrapOffset(String offset) {
        return offset;
      }

      @Override
      public String getStreamPosition(String offset) {
        return offset;
      }

      @Override
      public String wrapRecordId(String recordId) {
        return recordId;
      }

      @Override
      public void close() {

      }

      @Override
      public InputStream getNextInputStream() {
        InputStream temp = inputStream;
        inputStream = null;
        return temp;
      }
    }

    @VisibleForTesting
    static class CompressorInput implements CompressionDataParser.CompressionInput {

      private InputStream inputStream;

      public CompressorInput(InputStream inputStream) throws IOException {
        try {
          this.inputStream = new CompressorStreamFactory().createCompressorInputStream(
            new BufferedInputStream(inputStream));
        } catch (CompressorException e) {
          throw new IOException(e);
        }
      }

      @Override
      public String wrapOffset(String offset) {
        return offset;
      }

      @Override
      public InputStream getNextInputStream() {
        InputStream temp = inputStream;
        inputStream = null;
        return temp;
      }

      @Override
      public String getStreamPosition(String offset) {
        return offset;
      }

      @Override
      public String wrapRecordId(String recordId) {
        return recordId;
      }

      @Override
      public void close() throws IOException {
        if(inputStream != null) {
          inputStream.close();
        }
      }
    }

    @VisibleForTesting
    static class ArchiveInput implements CompressionDataParser.CompressionInput {

      public static final String FILE_NAME = "fileName";
      public static final String FILE_OFFSET = "fileOffset";

      private final PathMatcher pathMatcher;
      private ArchiveEntry currentEntry;
      private ArchiveInputStream archiveInputStream;
      private String wrappedOffset;
      private InputStream nextInputStream;
      private CompressionDataParser.CompressionInput compressionInput;
      private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

      public ArchiveInput(
          String compressedFilePattern,
          CompressionDataParser.CompressionInput compressionInput,
          String wrappedOffset
      ) {
        Utils.checkNotNull(compressedFilePattern, "Compressed File Pattern cannot be null");
        Utils.checkNotNull(wrappedOffset, "Offset cannot be null");
        pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + compressedFilePattern);
        this.wrappedOffset = wrappedOffset;
        this.compressionInput = compressionInput;
      }

      @Override
      public String wrapOffset(String offset) throws IOException {
        String fileName = null;
        if (currentEntry != null) {
          fileName = currentEntry.getName();
        }

        Map<String, Object> archiveOffset = new HashMap<>();
        archiveOffset.put(FILE_NAME, fileName);
        archiveOffset.put(FILE_OFFSET, offset);
        return OBJECT_MAPPER.writeValueAsString(archiveOffset);
      }

      @Override
      public InputStream getNextInputStream() throws IOException {

        if(archiveInputStream == null) {
          // Very first call to getNextInputStream, initialize archiveInputStream using the wrappedOffset
          wrappedOffset = wrappedOffset.equals(ZERO) ? wrapOffset(wrappedOffset) : wrappedOffset;
          Map<String, Object> archiveInputOffset = OBJECT_MAPPER.readValue(wrappedOffset, Map.class);
          try {
            archiveInputStream = new ArchiveStreamFactory().createArchiveInputStream(
              new BufferedInputStream(compressionInput.getNextInputStream()));
          } catch (ArchiveException e) {
            throw new IOException(e);
          }
          seekToOffset(archiveInputOffset);
          nextInputStream = archiveInputStream;
        }

        if (nextInputStream == null) {
          // this means reached end of a compressed file within the archive. seek to the next eligible entry
          seekToNextEligibleEntry();
          if (currentEntry != null) {
            // Not end of archive
            nextInputStream = archiveInputStream;
          }
        }
        InputStream temp = nextInputStream;
        nextInputStream = null;
        return temp;
      }

      @Override
      public String getStreamPosition(String offset) throws IOException {
        if(ZERO.equals(offset)) {
          return ZERO;
        }
        Map<String, Object> map = OBJECT_MAPPER.readValue(offset, Map.class);
        return (String)map.get(FILE_OFFSET);
      }

      @Override
      public String wrapRecordId(String recordId) {
        if(currentEntry != null) {
          return recordId + PATH_SEPARATOR + currentEntry.getName();
        }
        return recordId;
      }

      @Override
      public void close() throws IOException {
        if(archiveInputStream != null) {
          archiveInputStream.close();
        }
      }

      private void seekToOffset(Map<String, Object> archiveInputOffset) throws IOException {
        String fileName = (String) archiveInputOffset.get(FILE_NAME);
        long longOffset = Long.parseLong((String) archiveInputOffset.get(FILE_OFFSET));
        currentEntry = archiveInputStream.getNextEntry();
        while (currentEntry != null) {
          if (isEligibleEntry(currentEntry)) {
            // A match is when
            // - it is the first file read within the zip
            // - file that was last processed
            //      - if offset is -1 then return next eligible entry in the archive
            //      - if offset is not -1 then return current entry [the file that was last processed] as it is not
            //        completely read
            if (fileName == null) {
              //match - first file to match pattern within the zip
              break;
            } else if (currentEntry.getName().equals(fileName)) {
              // reached the last processed file
              if (longOffset != -1) {
                // the last processed file is not completely read, return same
                break;
              } else {
                // return next eligible entry in the archive
                seekToNextEligibleEntry();
                break;
              }
            }
          }
          currentEntry = archiveInputStream.getNextEntry();
        }
      }

      private void seekToNextEligibleEntry() throws IOException {
        currentEntry = archiveInputStream.getNextEntry();
        while (currentEntry != null && !isEligibleEntry(currentEntry)) {
          currentEntry = archiveInputStream.getNextEntry();
        }
      }

      private boolean isEligibleEntry(ArchiveEntry currentEntry) {
        return !currentEntry.isDirectory() && pathMatcher.matches(Paths.get(currentEntry.getName()));
      }
    }
  }
}
