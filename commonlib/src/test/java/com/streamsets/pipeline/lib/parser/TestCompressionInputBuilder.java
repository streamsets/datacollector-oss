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
package com.streamsets.pipeline.lib.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
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
import java.io.SequenceInputStream;
import java.util.Map;
import java.util.UUID;

public class TestCompressionInputBuilder {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @BeforeClass
  public static void setUpClass() {
    DataCollectorServicesUtils.loadDefaultServices();
  }

  @Test
  public void testCompressionInput() throws Exception {

    testCompressedFile("gz");
    testCompressedFile("bzip2");
    testCompressedFile("xz");
    testCompressedFile("DEFLATE");

    testConcatenatedCompressedFile("gz");
    testConcatenatedCompressedFile("bzip2");
    testConcatenatedCompressedFile("xz");
  }

  @Test
  public void testArchiveInput() throws Exception {
    testArchive(ArchiveStreamFactory.TAR);
  }

  private void testCompressedFile(String compressionType) throws Exception {

    //write data into the stream using the specified compression
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    CompressorOutputStream cOut = new CompressorStreamFactory().createCompressorOutputStream(compressionType, bOut);
    cOut.write("StreamSets".getBytes());
    cOut.close();

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

  private void testConcatenatedCompressedFile(String compressionType) throws Exception {
    ByteArrayOutputStream bytes1 = new ByteArrayOutputStream();
    ByteArrayOutputStream bytes2 = new ByteArrayOutputStream();
    CompressorOutputStream compressed1 = new CompressorStreamFactory()
        .createCompressorOutputStream(compressionType, bytes1);

    CompressorOutputStream compressed2 = new CompressorStreamFactory()
        .createCompressorOutputStream(compressionType, bytes2);

    compressed1.write("line1\n".getBytes());
    compressed1.close();

    compressed2.write("line2".getBytes());
    compressed2.close();

    CompressionDataParser.CompressionInputBuilder compressionInputBuilder =
        new CompressionDataParser.CompressionInputBuilder(
            Compression.COMPRESSED_FILE,
            null,
            new SequenceInputStream(
                new ByteArrayInputStream(bytes1.toByteArray()),
                new ByteArrayInputStream(bytes2.toByteArray())
            ),
            "0"
        );
    CompressionDataParser.CompressionInput input = compressionInputBuilder.build();

    //verify
    Assert.assertNotNull(input);
    Assert.assertEquals("myFile::4567", input.wrapOffset("myFile::4567"));
    Assert.assertEquals("myFile::4567", input.wrapRecordId("myFile::4567"));
    InputStream myFile = input.getNextInputStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(myFile));
    Assert.assertEquals("line1", reader.readLine());
    Assert.assertEquals("line2", reader.readLine());
  }

  @SuppressWarnings("unchecked")
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
      IOUtils.write(("StreamSets" + i).getBytes(), fileOutputStream);
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
    checkHeader(input, "file-0.txt", input.getStreamPosition(wrappedOffset));

    String recordIdPattern = "myFile/file-0.txt";
    Assert.assertEquals(recordIdPattern, input.wrapRecordId("myFile"));

  }

  void checkHeader(CompressionDataParser.CompressionInput input, String fileName, String offset) throws Exception {
    Record record =  RecordCreator.create();
    Record.Header header = record.getHeader();
    input.wrapRecordHeaders(header, offset);
    Assert.assertNotNull(
        record.getHeader().getAttribute(CompressionDataParser.CompressionInputBuilder.ArchiveInput.FILE_PATH_INSIDE_ARCHIVE)
    );
    Assert.assertNotNull(
        record.getHeader().getAttribute(CompressionDataParser.CompressionInputBuilder.ArchiveInput.FILE_NAME_INSIDE_ARCHIVE)
    );
    Assert.assertEquals(
        record.getHeader().getAttribute(CompressionDataParser.CompressionInputBuilder.ArchiveInput.FILE_NAME_INSIDE_ARCHIVE),
        fileName
    );
    Assert.assertNotNull(
        record.getHeader().getAttribute(CompressionDataParser.CompressionInputBuilder.ArchiveInput.FILE_OFFSET_INSIDER_ARCHIVE)
    );
    Assert.assertEquals(
        record.getHeader().getAttribute(CompressionDataParser.CompressionInputBuilder.ArchiveInput.FILE_OFFSET_INSIDER_ARCHIVE),
        offset
    );
  }

}
