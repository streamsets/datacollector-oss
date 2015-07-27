/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.hdfs.standalone;

import com.google.common.io.Resources;
import com.streamsets.datacollector.base.TestPipelineRunStandalone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestHdfsDestinationPipelineRun extends TestPipelineRunStandalone {

  private static MiniDFSCluster miniDFS;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // setting some dummy kerberos settings to be able to test a mis-setting
    System.setProperty("java.security.krb5.realm", "foo");
    System.setProperty("java.security.krb5.kdc", "localhost:0");

    File minidfsDir = new File("target/minidfs").getAbsoluteFile();
    if (!minidfsDir.exists()) {
      Assert.assertTrue(minidfsDir.mkdirs());
    }
    System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, minidfsDir.getPath());
    Configuration conf = new HdfsConfiguration();
    conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
    conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");
    UserGroupInformation.createUserForTesting("foo", new String[]{"all", "supergroup"});
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    miniDFS = new MiniDFSCluster.Builder(conf).build();
  }

  @After
  @Override
  public void tearDown() {
    if (miniDFS != null) {
      miniDFS.shutdown();
      miniDFS = null;
    }
  }

  @Override
  protected String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("hdfs_destination_pipeline_run.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    pipelineJson = pipelineJson.replaceAll("/uri", miniDFS.getURI().toString());
    return pipelineJson;
  }

  @Override
  protected int getRecordsInOrigin() {
    return 500;
  }

  @Override
  protected int getRecordsInTarget() throws IOException {
    int recordsRead = 0;
    DistributedFileSystem fileSystem = miniDFS.getFileSystem();
    //resolve ${YYYY()} instead of hardcoding 2015
    FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/tmp/out/2015"));
    for(FileStatus f : fileStatuses) {
      BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(f.getPath())));
      String line = br.readLine();
      while (line != null) {
        recordsRead++;
        line=br.readLine();
      }
    }
    return recordsRead;
  }

  @Override
  protected String getPipelineName() {
    return "hdfs_destination_pipeline";
  }

  @Override
  protected String getPipelineRev() {
    return "0";
  }
}
