/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.io.fileref;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.lib.hashing.HashingUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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
  ) throws IOException {
    LocalFileRef.Builder builder = new LocalFileRef.Builder()
        .filePath(getSourceFilePath(testDir))
        //To force multiple reads from the file.
        .bufferSize(TEXT.getBytes().length / 2)
        .totalSizeInBytes(Files.size(Paths.get(getSourceFilePath(testDir))));
    if (checksum != null) {
      builder.verifyChecksum(true)
          .checksum(checksum)
          .checksumAlgorithm(checksumAlgorithm);
    }
    if (createMetrics) {
      builder.createMetrics(true);
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
    String attributesToRead =
        Paths.get(getSourceFilePath(testDir)).getFileSystem().supportedFileAttributeViews().contains("posix")? "posix:*" : "*";
    Map<String, Object> metadata = new HashMap<>(Files.readAttributes(Paths.get(getSourceFilePath(testDir)), attributesToRead));
    metadata.put("filename", Paths.get(getSourceFilePath(testDir)).getFileName().toString());
    metadata.put("file", getSourceFilePath(testDir));
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

  public static Field createFieldForMetadata(Object metadataObject) {
    if (metadataObject instanceof Boolean) {
      return Field.create((Boolean) metadataObject);
    } else if (metadataObject instanceof Character) {
      return Field.create((Character) metadataObject);
    } else if (metadataObject instanceof Byte) {
      return Field.create((Byte) metadataObject);
    } else if (metadataObject instanceof Short) {
      return Field.create((Short) metadataObject);
    } else if (metadataObject instanceof Integer) {
      return Field.create((Integer) metadataObject);
    } else if (metadataObject instanceof Long) {
      return Field.create((Long) metadataObject);
    } else if (metadataObject instanceof Float) {
      return Field.create((Float) metadataObject);
    } else if (metadataObject instanceof Double) {
      return Field.create((Double) metadataObject);
    } else if (metadataObject instanceof Date) {
      return Field.createDatetime((Date) metadataObject);
    } else if (metadataObject instanceof BigDecimal) {
      return Field.create((BigDecimal) metadataObject);
    } else if (metadataObject instanceof String) {
      return Field.create((String) metadataObject);
    } else if (metadataObject instanceof byte[]) {
      return Field.create((byte[]) metadataObject);
    } else if (metadataObject instanceof Collection) {
      Iterator iterator = ((Collection) metadataObject).iterator();
      List<Field> fields = new ArrayList<>();
      while (iterator.hasNext()) {
        fields.add(createFieldForMetadata(iterator.next()));
      }
      return Field.create(fields);
    } else if (metadataObject instanceof Map) {
      boolean isListMap = (metadataObject instanceof LinkedHashMap);
      Map<String, Field> fieldMap = isListMap ? new LinkedHashMap<String, Field>() : new HashMap<String, Field>();
      Map map = (Map) metadataObject;
      for (Object key : map.keySet()) {
        fieldMap.put(key.toString(), createFieldForMetadata(map.get(key)));
      }
      return isListMap ? Field.create(Field.Type.LIST_MAP, fieldMap) : Field.create(fieldMap);
    } else {
      return Field.create(metadataObject.toString());
    }
  }
}
