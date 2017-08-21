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
package com.streamsets.datacollector.hdfs.standalone;

import com.google.common.io.Resources;
import com.streamsets.datacollector.base.PipelineOperationsStandaloneIT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class HdfsDestinationPipelineOperationsIT extends PipelineOperationsStandaloneIT {

  private static MiniDFSCluster miniDFS;

  @BeforeClass
  public static void beforeClass() throws Exception {
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
    UserGroupInformation.createUserForTesting("foo", new String[]{ "all", "supergroup"});
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    miniDFS = new MiniDFSCluster.Builder(conf).build();
    PipelineOperationsStandaloneIT.beforeClass(getPipelineJson());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (miniDFS != null) {
      miniDFS.shutdown();
      miniDFS = null;
    }
    PipelineOperationsStandaloneIT.afterClass();
  }

  private static String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("hdfs_destination_pipeline_operations.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    pipelineJson = pipelineJson.replaceAll("/uri", miniDFS.getURI().toString());
    return pipelineJson;
  }

  @Override
  protected String getPipelineName() {
    return "hdfs_destination_pipeline";
  }

  @Override
  protected String getPipelineRev() {
    return "0";
  }

  @Override
  protected void postPipelineStart() {

  }

}
