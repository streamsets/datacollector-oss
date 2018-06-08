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
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage.ConfigIssue;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.OriginAvroSchemaSource;
import com.streamsets.pipeline.impl.Pair;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterHDFSSourceIT {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterHdfsSource.class);
  private static MiniDFSCluster miniDFS;
  private static Path dir;
  private static File dummyEtc;
  private static String resourcesDir;
  private static String hadoopConfDir;
  private static File minidfsDir;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    minidfsDir = new File("target/minidfs-" + UUID.randomUUID()).getAbsoluteFile();
    assertTrue(minidfsDir.mkdirs());
    System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, minidfsDir.getPath());
    Configuration conf = new HdfsConfiguration();
    conf.set("dfs.namenode.fs-limits.min-block-size", String.valueOf(32));
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    miniDFS = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    dir = new Path(miniDFS.getURI()+"/dir");
    FileSystem fs = miniDFS.getFileSystem();
    fs.mkdirs(dir);
    writeFile(fs, new Path(dir + "/forAllTests/" + "path"), 1000);
    dummyEtc = new File(minidfsDir, "dummy-etc");
    assertTrue(dummyEtc.mkdirs());
    Configuration dummyConf = new Configuration(false);
    for (String file : new String[]{"core", "hdfs", "mapred", "yarn"}) {
      File siteXml = new File(dummyEtc, file + "-site.xml");
      FileOutputStream out = new FileOutputStream(siteXml);
      dummyConf.writeXml(out);
      out.close();
    }
    resourcesDir = minidfsDir.getAbsolutePath();
    hadoopConfDir = dummyEtc.getName();
    System.setProperty("sdc.resources.dir", resourcesDir);;
  }

  @AfterClass
  public static void cleanUpClass() throws IOException {
    System.clearProperty("sdc.resources.dir");
    if (miniDFS != null) {
      miniDFS.shutdown();
      miniDFS = null;
    }
  }

  static ClusterHdfsSource createSource(ClusterHdfsConfigBean conf) {
    return new ClusterHdfsSource(conf);
  }

  @Test
  public void testConfigsAbsent() throws Exception {
    File dummyEtcConfigsAbsent = new File(minidfsDir, "dummyEtcConfigsAbsent");
    dummyEtcConfigsAbsent.mkdirs();
    try {
      // Write only config file
      writeConfig(dummyEtcConfigsAbsent, "core");
      ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
      conf.hdfsUri = miniDFS.getURI().toString();
      conf.hdfsDirLocations = Arrays.asList(dir.toUri().getPath());
      conf.hdfsConfigs = new HashMap<>();
      conf.hdfsConfDir = dummyEtcConfigsAbsent.getName();
      conf.dataFormat = DataFormat.TEXT;
      conf.dataFormatConfig.textMaxLineLen = 1024;
      ClusterHdfsSource clusterHdfsSource = createSource(conf);
      if (!clusterHdfsSource.shouldHadoopConfigsExist()) {
        return;
      }
      SourceRunner sourceRunner =
        new SourceRunner.Builder(ClusterHdfsDSource.class, clusterHdfsSource)
        .addOutputLane("lane")
        .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
        .setResourcesDir(resourcesDir)
        .build();


      verifyForTestConfigsAbsent(sourceRunner, 3);

      // Write second config file
      writeConfig(dummyEtcConfigsAbsent, "mapred");
      verifyForTestConfigsAbsent(sourceRunner, 2);

      // Write third config file
      writeConfig(dummyEtcConfigsAbsent, "hdfs");
      verifyForTestConfigsAbsent(sourceRunner, 1);

      // Write the 4th; now all config files are present so init shouldn't throw exception
      writeConfig(dummyEtcConfigsAbsent, "yarn");
      sourceRunner.runInit();
      sourceRunner.runDestroy();

    } finally {
      FileUtils.deleteQuietly(dummyEtcConfigsAbsent);
    }
  }

  private void verifyForTestConfigsAbsent(SourceRunner sourceRunner, int issueCount) throws StageException {
    List<ConfigIssue> issues = sourceRunner.runValidateConfigs();
    assertEquals(String.valueOf(issues), issueCount, issues.size());
    assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_30"));
  }


  private void writeConfig(File configDir, String configFileNamePrefix) throws IOException {
    Configuration dummyConf = new Configuration(false);
    File siteXml = new File(configDir, configFileNamePrefix + "-site.xml");
    FileOutputStream out = new FileOutputStream(siteXml);
    dummyConf.writeXml(out);
    out.close();
  }

  @Test
  public void testConfigsNotInResourceDirectory() throws Exception {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Arrays.asList(dir.toUri().getPath());
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsConfDir = "../" + hadoopConfDir;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    SourceRunner sourceRunner =
      new SourceRunner.Builder(ClusterHdfsDSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
      .setResourcesDir(resourcesDir + "/subdirectory/")
      .build();

    List<ConfigIssue> issues = sourceRunner.runValidateConfigs();
    assertEquals(String.valueOf(issues), 1, issues.size());
    assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_29"));
  }

  @Test
  public void testWrongHDFSDirLocation() throws Exception {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Arrays.asList(dir.toUri().getPath());
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsConfDir = hadoopConfDir;
    conf.hdfsConfigs.put("x", "X");
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    conf.hdfsUri = "/pathwithnoschemeorauthority";

    ClusterHdfsSource clusterHdfsSource = createSource(conf);
    try {
      List<ConfigIssue> issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane"), resourcesDir));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_02"));

      if (clusterHdfsSource.isURIAuthorityRequired()) {
        conf.hdfsUri = "hdfs:///noauthority";
        clusterHdfsSource = createSource(conf);
        issues = clusterHdfsSource.init(null,
            ContextInfoCreator.createSourceContext("myInstance",
                false,
                OnRecordError.TO_ERROR,
                ImmutableList.of("lane"),
                resourcesDir
            )
        );
        assertEquals(String.valueOf(issues), 1, issues.size());
        assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_13"));
      }

      conf.hdfsUri = "hdfs://localhost:50000";
      clusterHdfsSource = createSource(conf);
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane"), resourcesDir));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_11"));

      conf.hdfsUri = miniDFS.getURI().toString();
      conf.hdfsDirLocations = Arrays.asList("/pathdoesnotexist");
      clusterHdfsSource = createSource(conf);
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane"), resourcesDir));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_10"));

      conf.hdfsUri = miniDFS.getURI().toString();
      conf.hdfsDirLocations = Arrays.asList(dir.toUri().getPath());
      FileSystem fs = miniDFS.getFileSystem();
      Path someFile = new Path(new Path(dir.toUri()), "/someFile");
      fs.create(someFile).close();
      clusterHdfsSource = createSource(conf);
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane"), resourcesDir));
      assertEquals(String.valueOf(issues), 0, issues.size());

      conf.hdfsUri = null;
      conf.hdfsConfigs.put(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, miniDFS.getURI().toString());
      someFile = new Path(new Path(dir.toUri()), "/someFile2");
      fs.create(someFile).close();
      clusterHdfsSource = createSource(conf);
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane"), resourcesDir));
      assertEquals(String.valueOf(issues), 0, issues.size());

      Path dummyFile = new Path(new Path(dir.toUri()), "/dummyFile");
      fs.create(dummyFile).close();
      conf.hdfsUri = miniDFS.getURI().toString();
      conf.hdfsDirLocations = Arrays.asList(dummyFile.toUri().getPath());
      clusterHdfsSource = createSource(conf);
      issues = clusterHdfsSource.init(null, ContextInfoCreator
        .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
          ImmutableList.of("lane"), resourcesDir));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_15"));

      Path emptyDir = new Path(dir.toUri().getPath(), "emptyDir");
      fs.mkdirs(emptyDir);
      conf.hdfsUri = miniDFS.getURI().toString();
      conf.hdfsDirLocations = Arrays.asList(emptyDir.toUri().getPath());
      clusterHdfsSource = createSource(conf);
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane"), resourcesDir));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_17"));

      Path path1 = new Path(emptyDir, "path1");
      fs.create(path1).close();
      conf.hdfsUri = miniDFS.getURI().toString();
      conf.hdfsDirLocations = Arrays.asList(emptyDir.toUri().getPath());
      clusterHdfsSource = createSource(conf);
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane"), resourcesDir));
      assertEquals(String.valueOf(issues), 0, issues.size());
    } finally {
      clusterHdfsSource.destroy();
    }
  }

  @Test
  public void testGetHdfsConfiguration() throws Exception {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Arrays.asList(dir.toString());
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsConfigs.put("x", "X");
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    ClusterHdfsSource clusterHdfsSource = createSource(conf);
    try {
      clusterHdfsSource.init(null, ContextInfoCreator.createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                                                                          ImmutableList.of("lane")));
      Assert.assertNotNull(clusterHdfsSource.getConfiguration());
      assertEquals("X", clusterHdfsSource.getConfiguration().get("x"));
    } finally {
      clusterHdfsSource.destroy();
    }
  }


  @Test(timeout = 30000)
  public void testProduce() throws Exception {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Arrays.asList(dir.toUri().getPath());
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsKerberos = false;
    conf.hdfsConfDir = hadoopConfDir;
    conf.recursive = false;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterHdfsDSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
      .setResourcesDir(resourcesDir)
      .build();

    sourceRunner.runInit();

    List<Map.Entry> list = new ArrayList<>();
    list.add(new Pair(new LongWritable(1), new Text("aaa")));
    list.add(new Pair(new LongWritable(2), new Text("bbb")));
    list.add(new Pair(new LongWritable(3), new Text("ccc")));

    Thread th = createThreadForAddingBatch(sourceRunner, list);
    try {
    StageRunner.Output output = sourceRunner.runProduce(null, 5);

    String newOffset = output.getNewOffset();
    Assert.assertEquals("3", newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(3, records.size());

    for (int i = 0; i < records.size(); i++) {
      Assert.assertNotNull(records.get(i).get("/text"));
      LOG.info("Header " + records.get(i).getHeader().getSourceId());
      Assert.assertTrue(!records.get(i).get("/text").getValueAsString().isEmpty());
      Assert.assertEquals(list.get(i).getValue().toString(), records.get(i).get("/text").getValueAsString());
    }

    if (sourceRunner != null) {
      sourceRunner.runDestroy();
    }
    } finally {
      th.interrupt();
    }
  }

  private FileStatus writeTextToFileAndGetFileStatus(Path filePath, String text) throws Exception {
    FSDataOutputStream os = null;
    try {
      os = miniDFS.getFileSystem().create(filePath, true);
      os.write(text.getBytes());
    } finally {
      if (os != null) {
        os.close();
      }
    }

    RemoteIterator<LocatedFileStatus> iterator = miniDFS.getFileSystem().listFiles(filePath.getParent(), false);
    assertTrue(iterator.hasNext());
    return iterator.next();
  }

  @Test(timeout = 30000)
  public void testDontReadAllFilesInPreview() throws Exception {
    String dirLocation = dir.toUri().getPath() + "/dummy";
    Path filePath = new Path(dirLocation + "/sample.txt");
    int i = 0;
    String text = "";
    while (i < 100) {
      text += i + "\n";
      i++;
    }
    // 2 files with 100 lines
    writeTextToFileAndGetFileStatus(filePath, text);
    writeTextToFileAndGetFileStatus(new Path(dirLocation + "/sample2.txt"), text);

    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Collections.singletonList(dirLocation);
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsKerberos = false;
    conf.hdfsConfDir = hadoopConfDir;
    conf.recursive = false;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    ClusterHdfsSource source = Mockito.spy(createSource(conf));
    SourceRunner sourceRunner = new SourceRunner.Builder(
        ClusterHdfsDSource.class,
        source
    ).setPreview(true).addOutputLane("lane").setExecutionMode(ExecutionMode.CLUSTER_BATCH).setResourcesDir
        (resourcesDir).build();

    sourceRunner.runInit();
    // readInPreview should be called only once even if there are 2 files with 100 lines each
    Mockito.verify(source, Mockito.times(1)).readInPreview(Mockito.any(FileStatus.class), Mockito.any(List.class));
  }

  @Test(timeout = 30000)
  public void testReadFileInSubdirectoryInPreview() throws Exception {
    /* Directory structure
       dummy/subdirectory/sample.txt
    */
    String dirLocation = dir.toUri().getPath() + "/dummy/dummy2";
    Path filePath = new Path(dirLocation + "/sample.txt");
    writeTextToFileAndGetFileStatus(filePath, "this is a sample text file");

    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Collections.singletonList(dirLocation);
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsKerberos = false;
    conf.hdfsConfDir = hadoopConfDir;
    conf.recursive = false;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    ClusterHdfsSource source = Mockito.spy(createSource(conf));
    SourceRunner sourceRunner = new SourceRunner.Builder(
        ClusterHdfsDSource.class,
        source
    ).setPreview(true).addOutputLane("lane").setExecutionMode(ExecutionMode.CLUSTER_BATCH).setResourcesDir
        (resourcesDir).build();

    sourceRunner.runInit();
    // readInPreview should be called once
    Mockito.verify(source, Mockito.times(1)).readInPreview(Mockito.any(FileStatus.class), Mockito.any(List.class));
  }

  @Test(timeout = 30000)
  public void testReadFileRecursiveInPreview() throws Exception {
    /* Directory structure. Since text files are small, both files should be read
       /dummy/subdirectory/sample.txt
       /dummy/subdirectory/subdirectory2/sample2.txt
    */
    String dirLocation = dir.toUri().getPath() + "/dummy/subdirectory";
    Path filePath = new Path(dirLocation + "/sample.txt");

    String text = "this is a sample text file";
    writeTextToFileAndGetFileStatus(filePath, text);
    writeTextToFileAndGetFileStatus(new Path(dirLocation + "/subdirectory2/sample2.txt"), text);

    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Collections.singletonList(dirLocation);
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsKerberos = false;
    conf.hdfsConfDir = hadoopConfDir;
    conf.recursive = false;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    ClusterHdfsSource source = Mockito.spy(createSource(conf));
    SourceRunner sourceRunner = new SourceRunner.Builder(
        ClusterHdfsDSource.class,
        source
    ).setPreview(true).addOutputLane("lane").setExecutionMode(ExecutionMode.CLUSTER_BATCH).setResourcesDir
        (resourcesDir).build();

    sourceRunner.runInit();
    // readInPreview should be called twice for both text files
    Mockito.verify(source, Mockito.times(2)).readInPreview(Mockito.any(FileStatus.class), Mockito.any(List.class));
  }

  @Test(timeout = 30000)
  public void testProduceCustomDelimiterByPreview() throws Exception {
    String dirLocation = dir.toUri().getPath() + "/dummy";
    Path filePath = new Path(dirLocation + "/sample.txt");

    FileStatus fileStatus = writeTextToFileAndGetFileStatus(filePath, "A@B@C@D");

    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Collections.singletonList(dirLocation);
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsKerberos = false;
    conf.hdfsConfDir = hadoopConfDir;
    conf.recursive = false;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textMaxLineLen = 1024;
    conf.dataFormatConfig.useCustomDelimiter = true;
    conf.dataFormatConfig.customDelimiter = "@";

    ClusterHdfsSource source = Mockito.spy(createSource(conf));
    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterHdfsDSource.class, source)
        .addOutputLane("lane")
        .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
        .setResourcesDir(resourcesDir)
        .build();

    sourceRunner.runInit();

    Configuration hadoopConf = (Configuration) Whitebox.getInternalState(source, "hadoopConf");
    assertEquals("@", hadoopConf.get(ClusterHdfsSource.TEXTINPUTFORMAT_RECORD_DELIMITER));

    try {
      List<Map.Entry> batch = source.previewTextBatch(fileStatus, 10);
      assertEquals(4, batch.size());
      String[] values = new String[4];
      for (int i =0 ; i < batch.size(); i++) {
        values[i] = (String)batch.get(i).getValue();
      }
      assertArrayEquals(new String[] {"A", "B", "C", "D"}, values);
    } finally {
      sourceRunner.runDestroy();
    }
  }

  @Test(timeout = 30000)
  public void testProduceDelimitedNoHeader() throws Exception {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Arrays.asList(dir.toUri().getPath());
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsKerberos = false;
    conf.hdfsConfDir = hadoopConfDir;
    conf.recursive = false;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.DELIMITED;
    conf.dataFormatConfig.csvFileFormat = CsvMode.CSV;
    conf.dataFormatConfig.csvHeader = CsvHeader.NO_HEADER;
    conf.dataFormatConfig.csvMaxObjectLen = 4096;
    conf.dataFormatConfig.csvRecordType = CsvRecordType.LIST;
    conf.dataFormatConfig.csvSkipStartLines = 0;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterHdfsDSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
      .setResourcesDir(resourcesDir)
      .build();

    sourceRunner.runInit();

    List<Map.Entry> list = new ArrayList<>();
    list.add(new Pair("1", new String("A,B\na,b")));
    list.add(new Pair("2", new String("C,D\nc,d")));

    Thread th = createThreadForAddingBatch(sourceRunner, list);
    try {
    StageRunner.Output output = sourceRunner.runProduce(null, 5);

    String newOffset = output.getNewOffset();
    Assert.assertEquals("2", newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(4, records.size());
    Record record = records.get(0);
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    record = records.get(1);
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    record = records.get(2);
    Assert.assertEquals("C", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("D", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    record = records.get(3);
    Assert.assertEquals("c", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("d", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));

    if (sourceRunner != null) {
      sourceRunner.runDestroy();
    }
    } finally {
      th.interrupt();
    }
  }

  @Test(timeout = 30000)
  public void testProduceDelimitedIgnoreHeader() throws Exception {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Arrays.asList(dir.toUri().getPath());
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsKerberos = false;
    conf.hdfsConfDir = hadoopConfDir;
    conf.recursive = false;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.DELIMITED;
    conf.dataFormatConfig.csvFileFormat = CsvMode.CSV;
    conf.dataFormatConfig.csvHeader = CsvHeader.IGNORE_HEADER;
    conf.dataFormatConfig.csvMaxObjectLen = 4096;
    conf.dataFormatConfig.csvRecordType = CsvRecordType.LIST;
    conf.dataFormatConfig.csvSkipStartLines = 0;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterHdfsDSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
      .setResourcesDir(resourcesDir)
      .build();

    sourceRunner.runInit();

    List<Map.Entry> list = new ArrayList<>();
    list.add(new Pair("path::0::0", new String("A,B\na,b")));
    list.add(new Pair("path::1::1", new String("C,D\nc,d")));

    Thread th = createThreadForAddingBatch(sourceRunner, list);
    try {
      StageRunner.Output output = sourceRunner.runProduce(null, 5);

      String newOffset = output.getNewOffset();
      Assert.assertEquals("path::1::1", newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(2, records.size());
      Record record = records.get(0);
      Assert.assertEquals("C", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
      Assert.assertFalse(record.has("[0]/header"));
      Assert.assertEquals("D", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
      Assert.assertFalse(record.has("[1]/header"));
      record = records.get(1);
      Assert.assertEquals("c", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
      Assert.assertFalse(record.has("[0]/header"));
      Assert.assertEquals("d", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
      Assert.assertFalse(record.has("[1]/header"));

      if (sourceRunner != null) {
        sourceRunner.runDestroy();
      }
    } finally {
      th.interrupt();
    }
  }

  @Test
  public void testDelimitedWithRecoverableException() throws Exception {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Collections.singletonList(dir.toUri().getPath());
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsKerberos = false;
    conf.hdfsConfDir = hadoopConfDir;
    conf.recursive = false;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.DELIMITED;
    conf.dataFormatConfig.csvFileFormat = CsvMode.CSV;
    conf.dataFormatConfig.csvHeader = CsvHeader.WITH_HEADER;
    conf.dataFormatConfig.csvMaxObjectLen = 4096;
    conf.dataFormatConfig.csvRecordType = CsvRecordType.LIST_MAP;
    conf.dataFormatConfig.csvSkipStartLines = 0;

    SourceRunner runner = new SourceRunner.Builder(ClusterHdfsDSource.class, createSource(conf))
        .addOutputLane("lane")
        .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
        .setResourcesDir(resourcesDir)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Map.Entry> list = ImmutableList.of(
        new Pair("a,b,c", null),
        new Pair("path::1::1", "1,2,3"),
        new Pair("path::1::2", "4,5,6,7"),
        new Pair("path::1::2", "8,9,10")
    );

    runner.runInit();

    Thread th = createThreadForAddingBatch(runner, list);

    try {
      StageRunner.Output output = runner.runProduce(null, 10);

      List<Record> outputRecords = output.getRecords().get("lane");
      List<Record> errorRecords = runner.getErrorRecords();
      Assert.assertEquals(1, errorRecords.size());
      Assert.assertEquals(2, outputRecords.size());

      //Checking output records
      Record record1 = outputRecords.get(0);
      Assert.assertEquals(1, record1.get("/a").getValueAsInteger());
      Assert.assertEquals(2, record1.get("/b").getValueAsInteger());
      Assert.assertEquals(3, record1.get("/c").getValueAsInteger());

      Record record2 = outputRecords.get(1);
      Assert.assertEquals(8, record2.get("/a").getValueAsInteger());
      Assert.assertEquals(9, record2.get("/b").getValueAsInteger());
      Assert.assertEquals(10, record2.get("/c").getValueAsInteger());

      //Check error record
      Record record = errorRecords.get(0);
      List<Field> columns = record.get("/columns").getValueAsList();
      List<Field> headers = record.get("/headers").getValueAsList();
      Assert.assertEquals(4, columns.size());
      Assert.assertEquals(3, headers.size());
      Assert.assertArrayEquals(
          ImmutableList.of("a", "b", "c").toArray(),
          headers.stream().map(Field::getValueAsString).collect(Collectors.toList()).toArray()
      );

      Assert.assertArrayEquals(
          ImmutableList.of(4,5,6,7).toArray(),
          columns.stream().map(Field::getValueAsInteger).collect(Collectors.toList()).toArray()
      );
    } finally {
      runner.runDestroy();
      th.interrupt();
    }
  }

  @Test(timeout = 30000)
  public void testProduceDelimitedWithHeader() throws Exception {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Arrays.asList(dir.toUri().getPath());
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsKerberos = false;
    conf.hdfsConfDir = hadoopConfDir;
    conf.recursive = false;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.DELIMITED;
    conf.dataFormatConfig.csvFileFormat = CsvMode.CSV;
    conf.dataFormatConfig.csvHeader = CsvHeader.WITH_HEADER;
    conf.dataFormatConfig.csvMaxObjectLen = 4096;
    conf.dataFormatConfig.csvRecordType = CsvRecordType.LIST;
    conf.dataFormatConfig.csvSkipStartLines = 0;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterHdfsDSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
      .setResourcesDir(resourcesDir)
      .build();

    sourceRunner.runInit();

    List<Map.Entry> list = new ArrayList<>();
    list.add(new Pair("HEADER_COL_1,HEADER_COL_2", null));
    list.add(new Pair("path::" + "1", new String("a,b\nC,D\nc,d")));

    Thread th = createThreadForAddingBatch(sourceRunner, list);
    try {
      StageRunner.Output output = sourceRunner.runProduce(null, 5);

      String newOffset = output.getNewOffset();
      Assert.assertEquals("path::" + "1", newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(3, records.size());
      Record record = records.get(0);
      Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
      Assert.assertEquals("HEADER_COL_1", record.get().getValueAsList().get(0).getValueAsMap().get("header").getValueAsString());
      Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
      Assert.assertEquals("HEADER_COL_2", record.get().getValueAsList().get(1).getValueAsMap().get("header").getValueAsString());
      record = records.get(1);
      Assert.assertEquals("C", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
      Assert.assertEquals("HEADER_COL_1", record.get().getValueAsList().get(0).getValueAsMap().get("header").getValueAsString());
      Assert.assertEquals("D", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
      Assert.assertEquals("HEADER_COL_2", record.get().getValueAsList().get(1).getValueAsMap().get("header").getValueAsString());
      record = records.get(2);
      Assert.assertEquals("c", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
      Assert.assertEquals("HEADER_COL_1", record.get().getValueAsList().get(0).getValueAsMap().get("header").getValueAsString());
      Assert.assertEquals("d", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
      Assert.assertEquals("HEADER_COL_2", record.get().getValueAsList().get(1).getValueAsMap().get("header").getValueAsString());
      if (sourceRunner != null) {
        sourceRunner.runDestroy();
      }
    } finally {
      th.interrupt();
    }
  }

  @Test(timeout = 30000)
  public void testProduceAvroData() throws Exception {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Arrays.asList(dir.toUri().getPath());
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsKerberos = false;
    conf.hdfsConfDir = hadoopConfDir;
    conf.recursive = false;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.AVRO;
    conf.dataFormatConfig.avroSchemaSource = OriginAvroSchemaSource.SOURCE;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterHdfsDSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
      .setResourcesDir(resourcesDir)
      .build();

    sourceRunner.runInit();

    List<Map.Entry> list = new ArrayList<>();
    list.add(new Pair("path::" + "1" + "::1", createAvroData("a", 30, ImmutableList.of("a@company.com", "a2@company.com"))));
    list.add(new Pair("path::" + "1" + "::2", createAvroData("b", 40, ImmutableList.of("b@company.com", "b2@company.com"))));

    Thread th = createThreadForAddingBatch(sourceRunner, list);
    try {
      StageRunner.Output output = sourceRunner.runProduce(null, 5);
      String newOffset = output.getNewOffset();
      Assert.assertEquals("path::" + "1::2", newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(2, records.size());

      Record record = records.get(0);
      Assert.assertTrue(record.has("/name"));
      Assert.assertEquals("a", record.get("/name").getValueAsString());
      Assert.assertTrue(record.has("/age"));
      Assert.assertEquals(30, record.get("/age").getValueAsInteger());
      Assert.assertTrue(record.has("/emails"));
      Assert.assertTrue(record.get("/emails").getValueAsList() instanceof List);
      List<Field> emails = record.get("/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("a@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("a2@company.com", emails.get(1).getValueAsString());

      record = records.get(1);
      Assert.assertTrue(record.has("/name"));
      Assert.assertEquals("b", record.get("/name").getValueAsString());
      Assert.assertTrue(record.has("/age"));
      Assert.assertEquals(40, record.get("/age").getValueAsInteger());
      Assert.assertTrue(record.has("/emails"));
      Assert.assertTrue(record.get("/emails").getValueAsList() instanceof List);
      emails = record.get("/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("b@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("b2@company.com", emails.get(1).getValueAsString());

    } finally {
      th.interrupt();
    }
  }

  @Test(timeout = 30000)
  public void testLineageEvent() throws Exception {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = miniDFS.getURI().toString();
    conf.hdfsDirLocations = Arrays.asList(dir.toUri().getPath());
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsKerberos = false;
    conf.hdfsConfDir = hadoopConfDir;
    conf.recursive = false;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.DELIMITED;
    conf.dataFormatConfig.csvFileFormat = CsvMode.CSV;
    conf.dataFormatConfig.csvHeader = CsvHeader.WITH_HEADER;
    conf.dataFormatConfig.csvMaxObjectLen = 4096;
    conf.dataFormatConfig.csvRecordType = CsvRecordType.LIST;
    conf.dataFormatConfig.csvSkipStartLines = 0;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterHdfsDSource.class, createSource(conf))
        .addOutputLane("lane")
        .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
        .setResourcesDir(resourcesDir)
        .build();

    sourceRunner.runInit();

    List<Map.Entry> list = new ArrayList<>();
    list.add(new Pair("HEADER_COL_1,HEADER_COL_2", null));
    list.add(new Pair("path::" + "1", new String("a,b\nC,D\nc,d")));

    Thread th = createThreadForAddingBatch(sourceRunner, list);
    try {
      sourceRunner.runProduce(null, 5);
      List<LineageEvent> events = sourceRunner.getLineageEvents();
      Assert.assertEquals(1, events.size());
      Assert.assertEquals(LineageEventType.ENTITY_READ, events.get(0).getEventType());
      Assert.assertEquals("path", events.get(0).getSpecificAttribute(LineageSpecificAttribute.DESCRIPTION));
      Assert.assertEquals(EndPointType.HDFS.name(), events.get(0).getSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE));
    } finally {
      th.interrupt();
    }
  }


  private byte[] createAvroData(String name, int age, List<String> emails)  throws IOException {
    String AVRO_SCHEMA = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"name\", \"type\": \"string\"},\n"
      +" {\"name\": \"age\", \"type\": \"int\"},\n"
      +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
      +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
      +"]}";
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GenericRecord e1 = new GenericData.Record(schema);
    e1.put("name", name);
    e1.put("age", age);
    e1.put("emails", emails);
    e1.put("boss", null);

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord>dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, out);
    dataFileWriter.append(e1);
    dataFileWriter.close();
    return out.toByteArray();
  }


  private Thread createThreadForAddingBatch(final SourceRunner sourceRunner, final List<Map.Entry> list) {
    Thread sourceThread = new Thread() {
      @Override
      public void run() {
        try {
          ClusterHdfsSource source = (ClusterHdfsSource) sourceRunner.getStage();
          source.put(list);
        } catch (Exception ex) {
          LOG.error("Error in waiter thread: " + ex, ex);
        }
      }
    };
    sourceThread.setName(getClass().getName() + "-sourceThread");
    sourceThread.setDaemon(true);
    sourceThread.start();
    return sourceThread;
  }

  private static void writeFile(FileSystem fs, Path ph, int size) throws IOException {
    FSDataOutputStream stm = fs.create(ph, true, 4096, (short)3, 512);
    for (int i = 0; i < 1; i++) {
      stm.write(new byte[size]);
    }
    stm.hsync();
    stm.hsync();
    stm.close();
  }

}
