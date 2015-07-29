/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.impl.Pair;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;

import com.streamsets.pipeline.api.Stage.ConfigIssue;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.configurablestage.DSource;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;

public class TestClusterHDFSSource {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterHdfsSource.class);
  private static MiniDFSCluster miniDFS;
  private static Path dir;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    File minidfsDir = new File("target/minidfs").getAbsoluteFile();
    if (!minidfsDir.exists()) {
      Assert.assertTrue(minidfsDir.mkdirs());
    }
    System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, minidfsDir.getPath());
    Configuration conf = new HdfsConfiguration();
    conf.set("dfs.namenode.fs-limits.min-block-size", String.valueOf(32));
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    miniDFS = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    dir = new Path(miniDFS.getURI()+"/dir");
    FileSystem fs = miniDFS.getFileSystem();
    fs.mkdirs(dir);
    writeFile(fs, new Path(dir+"/forAllTests/"+"path"), 1000);
  }

  @AfterClass
  public static void cleanUpClass() throws IOException {
    if (miniDFS != null) {
      miniDFS.shutdown();
      miniDFS = null;
    }
  }

  private void configure(ClusterHdfsDSource hdfsClusterSource, String dirLocation) {
    hdfsClusterSource.hdfsUri = miniDFS.getURI().toString();
    hdfsClusterSource.hdfsDirLocations = Arrays.asList(dirLocation);
    hdfsClusterSource.hdfsConfigs = new HashMap<String, String>();
    hdfsClusterSource.hdfsConfigs.put("x", "X");
    hdfsClusterSource.dataFormat = DataFormat.TEXT;
    hdfsClusterSource.textMaxLineLen = 1024;
  }

  @Test
  public void testWrongHDFSDirLocation() throws Exception {
    ClusterHdfsDSource dSource = new ForTestClusterHdfsDSource();
    configure(dSource, dir.toUri().getPath());
    dSource.hdfsUri = "/pathwithnoschemeorauthority";
    ClusterHdfsSource clusterHdfsSource = (ClusterHdfsSource) dSource.createSource();
    try {
      List<ConfigIssue> issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane")));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_02"));

      dSource.hdfsUri = "file://localhost:8020/";
      clusterHdfsSource = (ClusterHdfsSource) dSource.createSource();
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane")));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_12"));

      dSource.hdfsUri = "hdfs:///noauthority";
      clusterHdfsSource = (ClusterHdfsSource) dSource.createSource();
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane")));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_13"));

      dSource.hdfsUri = "hdfs://localhost/invalidauthorityformat";
      clusterHdfsSource = (ClusterHdfsSource) dSource.createSource();
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane")));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_14"));

      dSource.hdfsUri = "hdfs://localhost:8020";
      clusterHdfsSource = (ClusterHdfsSource) dSource.createSource();
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane")));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_11"));

      dSource.hdfsUri = miniDFS.getURI().toString();
      dSource.hdfsDirLocations = Arrays.asList("/pathdoesnotexist");
      clusterHdfsSource = (ClusterHdfsSource) dSource.createSource();
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane")));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_10"));

      dSource.hdfsUri = miniDFS.getURI().toString();
      dSource.hdfsDirLocations = Arrays.asList(dir.toUri().getPath());
      FileSystem fs = miniDFS.getFileSystem();
      Path someFile = new Path(new Path(dir.toUri()), "/someFile");
      fs.create(someFile).close();
      clusterHdfsSource = (ClusterHdfsSource) dSource.createSource();
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane")));
      assertEquals(String.valueOf(issues), 0, issues.size());

      Path dummyFile = new Path(new Path(dir.toUri()), "/dummyFile");
      fs.create(dummyFile).close();
      dSource.hdfsUri = miniDFS.getURI().toString();
      dSource.hdfsDirLocations = Arrays.asList(dummyFile.toUri().getPath());
      clusterHdfsSource = (ClusterHdfsSource) dSource.createSource();
      issues = clusterHdfsSource.init(null, ContextInfoCreator
        .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
          ImmutableList.of("lane")));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_15"));

      Path emptyDir = new Path(dir.toUri().getPath(), "emptyDir");
      fs.mkdirs(emptyDir);
      dSource.hdfsUri = miniDFS.getURI().toString();
      dSource.hdfsDirLocations = Arrays.asList(emptyDir.toUri().getPath());
      clusterHdfsSource = (ClusterHdfsSource) dSource.createSource();
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane")));
      assertEquals(String.valueOf(issues), 1, issues.size());
      assertTrue(String.valueOf(issues), issues.get(0).toString().contains("HADOOPFS_16"));

      Path path1 = new Path(emptyDir, "path1");
      fs.create(path1).close();
      dSource.hdfsUri = miniDFS.getURI().toString();
      dSource.hdfsDirLocations = Arrays.asList(emptyDir.toUri().getPath());
      clusterHdfsSource = (ClusterHdfsSource) dSource.createSource();
      issues = clusterHdfsSource.init(null, ContextInfoCreator
          .createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
                               ImmutableList.of("lane")));
      assertEquals(String.valueOf(issues), 0, issues.size());
    } finally {
      clusterHdfsSource.destroy();
    }
  }

  @Test
  public void testGetHdfsConfiguration() throws Exception {
    ClusterHdfsDSource dSource = new ForTestClusterHdfsDSource();
    configure(dSource, dir.toString());
    ClusterHdfsSource clusterHdfsSource = (ClusterHdfsSource) dSource.createSource();
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
    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterHdfsDSource.class)
      .addOutputLane("lane")
      .setClusterMode(true)
      .addConfiguration("hdfsUri", miniDFS.getURI().toString())
      .addConfiguration("hdfsDirLocations", Arrays.asList(dir.toUri().getPath()))
      .addConfiguration("recursive", false)
      .addConfiguration("hdfsConfigs", new HashMap<String, String>())
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("textMaxLineLen", 1024)
      .addConfiguration("produceSingleRecordPerMessage", false)
      .addConfiguration("regex", null)
      .addConfiguration("grokPatternDefinition", null)
      .addConfiguration("enableLog4jCustomLogFormat", false)
      .addConfiguration("customLogFormat", null)
      .addConfiguration("fieldPathsToGroupName", null)
      .addConfiguration("log4jCustomLogFormat", null)
      .addConfiguration("grokPattern", null)
      .addConfiguration("hdfsKerberos", false)
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


  private Thread createThreadForAddingBatch(final SourceRunner sourceRunner, final List<Map.Entry> list) {
    Thread sourceThread = new Thread() {
      @Override
      public void run() {
        try {
          ClusterHdfsSource source =
            ((ClusterHdfsSource) ((DSource) sourceRunner.getStage()).getSource());
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

  static class ForTestClusterHdfsDSource extends ClusterHdfsDSource {
    @Override
    protected ClusterHdfsSource createSource() {
      return new ClusterHdfsSource(hdfsUri, hdfsDirLocations, recursive, hdfsConfigs, dataFormat, textMaxLineLen,
        jsonMaxObjectLen, logMode, retainOriginalLine, customLogFormat, regex, fieldPathsToGroupName,
        grokPatternDefinition, grokPattern, enableLog4jCustomLogFormat, log4jCustomLogFormat, logMaxObjectLen,
        produceSingleRecordPerMessage, hdfsKerberos, null, null);
    }
  }

}
