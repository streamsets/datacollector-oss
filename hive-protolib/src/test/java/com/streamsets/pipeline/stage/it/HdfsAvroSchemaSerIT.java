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
package com.streamsets.pipeline.stage.it;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore
public class HdfsAvroSchemaSerIT extends BaseHiveMetadataPropagationIT{
  private static Logger LOG = LoggerFactory.getLogger(ColdStartIT.class);
  private static final String DATABASE = "default";
  private static final String TABLE = "sample";
  private static final String AVRO_SCHEMA_SERIALIZATION_PREFIX =
      String.format(HiveMetastoreUtil.AVRO_SCHEMA +"_%s_%s", DATABASE, TABLE);
  private static final String ABSOLUTE_PATH = "absoluteLocation";
  private static final String SCHEMA_FOLDER = "schemaFolder";


  private void run(String location, List<Record> records) throws Exception{
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .external(false)
        .database(DATABASE)
        .table(TABLE)
        .build();
    HiveMetastoreTargetBuilder builder = new HiveMetastoreTargetBuilder()
        .storedAsAvro(false);
    if (!location.isEmpty()) {
      builder.schemFolderLocation(location);
    }
    HiveMetastoreTarget target = builder.build();
    processRecords(processor, target, records);
  }

  private List<Record> getRecords() {
    Record record = RecordCreator.create();
    record.getHeader().setAttribute("/database", "db");
    record.getHeader().setAttribute("/table", "sample");
    Map<String, Field> fieldMap = new HashMap<>();
    fieldMap.put("stringField", Field.create("string"));
    fieldMap.put("intField", Field.create(1));
    record.set(Field.create(fieldMap));
    return ImmutableList.of(record);
  }

  private void verifySerializationLocation(String location) throws IOException {
    FileSystem fs = BaseHiveIT.getDefaultFileSystem();
    Path path = new Path(BaseHiveIT.getDefaultFsUri() + location);
    Assert.assertTrue("Location does not exist:" + location, fs.exists(path));
    boolean found = false;
    RemoteIterator<LocatedFileStatus> fsIterator =  fs.listFiles(path, false);
    while (!found || fsIterator.hasNext()) {
      LocatedFileStatus status = fsIterator.next();
      LOG.info("Found file: " + status.getPath().getName());
      found = status.getPath().getName().startsWith(AVRO_SCHEMA_SERIALIZATION_PREFIX);
    }
    fs.delete(path, true);
    Assert.assertTrue("Avro schema file not found in the location " + location, found);
  }

  private String getDefaultRootTblLocation() {
    return "/user/hive/warehouse"
        + HiveMetastoreUtil.SEP
        + TABLE
        + HiveMetastoreUtil.SEP;
  }

  @Test
  public void defaultPathSer() throws Exception{
    run("", getRecords());
    verifySerializationLocation(getDefaultRootTblLocation() + ".schemas");
  }

  @Test
  public void testRelativePathSer() throws Exception {
    run(SCHEMA_FOLDER, getRecords());
    verifySerializationLocation(getDefaultRootTblLocation() + SCHEMA_FOLDER);
  }

  @Test
  public void testAbsolutePathSer() throws Exception {
    String location = HiveMetastoreUtil.SEP + ABSOLUTE_PATH
        + HiveMetastoreUtil.SEP
        + DATABASE + HiveMetastoreUtil.SEP
        + TABLE + HiveMetastoreUtil.SEP
        + SCHEMA_FOLDER;
    run(location, getRecords());
    verifySerializationLocation(location);
  }

  @Test
  public void testRelativePathSerWithEL() throws Exception {
    String location = "${str:concat('a', 'b')}";
    run(location, getRecords());
    verifySerializationLocation(getDefaultRootTblLocation() + "ab");
  }

  @Test
  public void testAbsolutePathSerWithEL() throws Exception {
    String location = HiveMetastoreUtil.SEP + ABSOLUTE_PATH
        + HiveMetastoreUtil.SEP +  "${str:concat('a', 'b')}"
        + HiveMetastoreUtil.SEP+  "${str:concat('c', 'd')}"
        + HiveMetastoreUtil.SEP + SCHEMA_FOLDER;
    run(location, getRecords());
    verifySerializationLocation(HiveMetastoreUtil.SEP + ABSOLUTE_PATH + "/ab/cd/" + SCHEMA_FOLDER);
  }
}
