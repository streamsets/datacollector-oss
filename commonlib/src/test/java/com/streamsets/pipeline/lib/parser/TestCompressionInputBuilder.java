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
import com.streamsets.pipeline.config.Compression;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Map;
import java.util.UUID;

public class TestCompressionInputBuilder {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testCompressionInput() throws Exception {

    testCompressedFile("gz");
    testCompressedFile("bzip2");
    testCompressedFile("xz");
    testCompressedFile("DEFLATE");
  }

  @Test
  public void testArchiveInput() throws Exception {
    testArchive(ArchiveStreamFactory.TAR);
  }

  private void testCompressedFile(String compressionType) throws Exception {

    //write data into the stream using the specified compression
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    CompressorOutputStream bzip2 = new CompressorStreamFactory().createCompressorOutputStream(compressionType, bOut);
    bzip2.write(new String("StreamSets").getBytes());
    bzip2.close();

    //create compression input
    CompressionDataParser.CompressionInputBuilder compressionInputBuilder =
      new CompressionDataParser.CompressionInputBuilder(Compression.COMPRESSED_FILE, null,
        new ByteArrayInputStream(bOut.toByteArray()), "0");
    CompressionDataParser.CompressionInput input = compressionInputBuilder.build();

    //verify
    Assert.assertNotNull(input);
    Assert.assertEquals("myFile::4567", input.wrapOffset("myFile::4567"));
    Assert.assertEquals("myFile::4567", input.wrapRecordId("myFile::4567"));
    InputStream myFile = input.getNextInputStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(myFile));
    Assert.assertEquals("StreamSets", reader.readLine());
  }

  private void testArchive(String archiveType) throws Exception {
    //create an archive with multiple files, files containing multiple objects
    File dir = new File("target", UUID.randomUUID().toString());
    dir.mkdirs();

    OutputStream archiveOut = new FileOutputStream(new File(dir, "myArchive"));
    ArchiveOutputStream archiveOutputStream = new ArchiveStreamFactory().createArchiveOutputStream(archiveType, archiveOut);

    File inputFile;
    ArchiveEntry archiveEntry;
    FileOutputStream fileOutputStream;
    for(int i = 0; i < 5; i++) {
      String fileName = "file-" + i + ".txt";
      inputFile = new File(dir, fileName);
      fileOutputStream = new FileOutputStream(inputFile);
      IOUtils.write(new String("StreamSets" + i).getBytes(), fileOutputStream);
      fileOutputStream.close();
      archiveEntry = archiveOutputStream.createArchiveEntry(inputFile, fileName);
      archiveOutputStream.putArchiveEntry(archiveEntry);
      IOUtils.copy(new FileInputStream(inputFile), archiveOutputStream);
      archiveOutputStream.closeArchiveEntry();
    }
    archiveOutputStream.finish();
    archiveOut.close();

    //create compression input
    FileInputStream fileInputStream = new FileInputStream(new File(dir, "myArchive"));
    CompressionDataParser.CompressionInputBuilder compressionInputBuilder =
      new CompressionDataParser.CompressionInputBuilder(Compression.ARCHIVE, "*.txt", fileInputStream, "0");
    CompressionDataParser.CompressionInput input = compressionInputBuilder.build();

    // before reading
    Assert.assertNotNull(input);

    // The default wrapped offset before reading any file
    String wrappedOffset = "{\"fileName\": \"myfile\", \"fileOffset\":\"0\"}";

    Map<String, Object> archiveInputOffset = OBJECT_MAPPER.readValue(wrappedOffset, Map.class);
    Assert.assertNotNull(archiveInputOffset);
    Assert.assertEquals("myfile", archiveInputOffset.get("fileName"));
    Assert.assertEquals("0", archiveInputOffset.get("fileOffset"));

    Assert.assertEquals("0", input.getStreamPosition(wrappedOffset));

    // read and check wrapped offset
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(input.getNextInputStream()));
    Assert.assertEquals("StreamSets0", reader.readLine());
    wrappedOffset = input.wrapOffset("4567");
    archiveInputOffset = OBJECT_MAPPER.readValue(wrappedOffset, Map.class);
    Assert.assertNotNull(archiveInputOffset);
    Assert.assertEquals("file-0.txt", archiveInputOffset.get("fileName"));
    Assert.assertEquals("4567", archiveInputOffset.get("fileOffset"));

    String recordIdPattern = "myFile/file-0.txt";
    Assert.assertEquals(recordIdPattern, input.wrapRecordId("myFile"));

  }

}