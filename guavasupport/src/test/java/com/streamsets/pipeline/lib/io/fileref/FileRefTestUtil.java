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
package com.streamsets.pipeline.lib.io.fileref;

import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public final class FileRefTestUtil {
  public static final String TEXT = "This is a sample whole file data format test text";
  private static Random r = new Random();

  private FileRefTestUtil() {}

  public static FileRef getLocalFileRef(
      File testDir,
      boolean createMetrics,
      String checksum,
      HashingUtil.HashType checksumAlgorithm
  ) {
    return getLocalFileRefWithCustomFile(
        new File(getSourceFilePath(testDir)),
        createMetrics,
        checksum,
        checksumAlgorithm
    );
  }

  public static FileRef getLocalFileRefWithCustomFile(
      File file,
      boolean createMetrics,
      String checksum,
      HashingUtil.HashType checksumAlgorithm
  ) {
    AbstractSpoolerFileRef.Builder builder = new LocalFileRef.Builder()
        .filePath(file.getAbsolutePath())
        //To force multiple reads from the file.
        .bufferSize(TEXT.getBytes().length / 2)
        .totalSizeInBytes(file.length());
    if (checksum != null) {
      builder.verifyChecksum(true)
          .checksum(checksum)
          .checksumAlgorithm(checksumAlgorithm);
    }
    if (createMetrics) {
      builder.createMetrics(true);
    } else {
      builder.createMetrics(false);
    }
    return builder.build();
  }

  public static String getSourceFilePath(File testDir) {
    return testDir.getAbsolutePath() + "/source.txt";
  }

  public static void writePredefinedTextToFile(File testDir) throws Exception {
    Files.write(
        Paths.get(getSourceFilePath(testDir)),
        TEXT.getBytes(),
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE_NEW
    );
  }

  public static Map<String, Object> getFileMetadata(File testDir) throws IOException {
    return getFileMetadataWithCustomFile(new File(getSourceFilePath(testDir)));
  }

  public static Map<String, Object> getFileMetadataWithCustomFile(File file) throws IOException {
    String attributesToRead =
        file.toPath().getFileSystem().supportedFileAttributeViews().contains("posix")? "posix:*" : "*";
    Map<String, Object> metadata = new HashMap<>(Files.readAttributes(file.toPath(), attributesToRead));
    metadata.put("filename", file.getName());
    metadata.put("file", file.getAbsolutePath());
    return metadata;
  }

  public static int randomReadMethodsWithInputStream(InputStream is) throws Exception {
    int i = r.nextInt(2);
    byte[] b = new byte[12];
    switch (i) {
      case 0:
        return (is.read() == -1)? -1 : 1;
      case 1:
        return is.read(b);
      case 2:
        int num = r.nextInt(5);
        return is.read(b, num, num + r.nextInt(5));
      default:
        throw new IllegalArgumentException("Should match one of the cases");
    }
  }

  public static void checkFileContent(InputStream is1, InputStream is2) throws Exception {
    int totalBytesRead1 = 0, totalBytesRead2 = 0;
    int a = 0, b = 0;
    while (a != -1 || b != -1) {
      totalBytesRead1 = ((a = is1.read()) != -1)? totalBytesRead1 + 1 : totalBytesRead1;
      totalBytesRead2 = ((b = is2.read()) != -1)? totalBytesRead2 + 1 : totalBytesRead2;
      Assert.assertEquals(a, b);
    }
    Assert.assertEquals(totalBytesRead1, totalBytesRead2);
  }

}
