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
package com.streamsets.datacollector.client.cli;

import com.streamsets.datacollector.client.util.TestUtil;
import com.streamsets.datacollector.task.Task;
import com.streamsets.testing.NetworkUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestCLI {

  private String baseURL;
  private String[] authenticationTypes = {"none", "basic", "form", "digest"};

  @Test
  public void testWithOutAnyParameter() {
    String cliOutput = TestUtil.runCliCommand();
    Assert.assertTrue(cliOutput.contains("usage: cli"));
    System.out.println(cliOutput);
  }

  @Test
  public void testWithOutSDCURL() {
    String cliOutput = TestUtil.runCliCommand("ping");
    Assert.assertTrue(cliOutput.contains("Required option '-U' is missing"));
    System.out.println(cliOutput);
  }

  @Test
  public void testWithWrongURL() {
    String cliOutput = TestUtil.runCliCommand("-U", "http://localhost:1233", "ping");
    Assert.assertTrue(cliOutput.contains("java.net.ConnectException: Connection refused"));
    System.out.println(cliOutput);
  }

  @Test
  public void testWithInvalidAuthType() {
    String cliOutput = TestUtil.runCliCommand("-U", "http://localhost:1233", "-a", "invalidAuthType", "ping");
    Assert.assertTrue(cliOutput.contains("Invalid Authentication Type"));
    System.out.println(cliOutput);
  }

  @Ignore
  @Test
  public void testForDifferentAuthenticationTypes() {
    Task server = null;
    try {
      for(String authType: authenticationTypes) {
        int port = NetworkUtils.getRandomPort();
        server = TestUtil.startServer(port, authType);
        baseURL = "http://127.0.0.1:" + port;

        if(!authType.equals("none")) {
          testWithInvalidCredentials(authType);
        }

        testPingCommand(authType);

        testDefinitionsListCommand(authType);

        testSystemConfigurationCommand(authType);
        testSystemDirectoriesCommand(authType);
        testSystemInfoCommand(authType);
        testSystemCurrentUserCommand(authType);
        testSystemServerTimeCommand(authType);
        // TODO testSystemStatsCommand if we ever re-enable these tests
        testSystemThreadsCommand(authType);

        testStoreListCommand(authType);
        testStoreCreateCommand(authType);
        testStoreGetConfigCommand(authType);
        testStoreUpdateConfigCommand(authType);
        testStoreGetRulesCommand(authType);
        testStoreUpdateRulesCommand(authType);
        testStoreExportImportCommand(authType);

        testManagerCommand(authType);

        testPreviewCommand(authType);

        TestUtil.stopServer(server);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if(server != null) {
        TestUtil.stopServer(server);
      }
    }
  }

  private void testWithInvalidCredentials(String authType) {
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "invalidpassword",
      "ping");
    Assert.assertTrue(cliOutput.contains("HTTP Error 401 - Unauthorized: Access is denied due to invalid credentials."));
  }

  private void testPingCommand(String authType) {
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "ping");
    Assert.assertTrue(cliOutput.contains("version"));
  }

  private void testDefinitionsListCommand(String authType) {
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin",
      "definitions");
    Assert.assertTrue(cliOutput.contains("pipeline"));
    Assert.assertTrue(cliOutput.contains("configGroupDefinition"));
  }

  private  void testSystemConfigurationCommand(String authType) {
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "system",
      "configuration");
    Assert.assertTrue(cliOutput.contains("http.authentication"));
  }

  private  void testSystemDirectoriesCommand(String authType) {
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "system",
      "directories");
    Assert.assertTrue(cliOutput.contains("runtimeDir"));
    Assert.assertTrue(cliOutput.contains("configDir"));
    Assert.assertTrue(cliOutput.contains("dataDir"));
    Assert.assertTrue(cliOutput.contains("logDir"));
    Assert.assertTrue(cliOutput.contains("resourcesDir"));
  }

  private  void testSystemInfoCommand(String authType) {
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "system",
      "info");
    Assert.assertTrue(cliOutput.contains("builtDate"));
    Assert.assertTrue(cliOutput.contains("builtBy"));
    Assert.assertTrue(cliOutput.contains("builtRepoSha"));
    Assert.assertTrue(cliOutput.contains("sourceMd5Checksum"));
    Assert.assertTrue(cliOutput.contains("version"));
  }

  private  void testSystemCurrentUserCommand(String authType) {
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "system",
      "current-user");
    Assert.assertTrue(cliOutput.contains("roles"));
    Assert.assertTrue(cliOutput.contains("user"));
    Assert.assertTrue(cliOutput.contains("admin"));
  }


  private  void testSystemServerTimeCommand(String authType) {
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "system",
      "server-time");
    Assert.assertTrue(cliOutput.contains("serverTime"));
  }

  private  void testSystemThreadsCommand(String authType) {
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "system",
      "threads");
    Assert.assertTrue(cliOutput.contains("threadInfo"));
  }


  private  void testStoreListCommand(String authType) {
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "list");
    Assert.assertTrue(cliOutput.contains("[ ]"));
  }

  private  void testStoreCreateCommand(String authType) {
    //without pipeline name
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "create");
    Assert.assertTrue(cliOutput.contains("Required option '-n' is missing"));

    //with pipeline name
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "create", "-n", "Sample Pipeline", "-d", "description for pipeline");
    Assert.assertTrue(cliOutput.contains("Sample Pipeline"));
    Assert.assertTrue(cliOutput.contains("description for pipeline"));
    Assert.assertTrue(cliOutput.contains("schemaVersion"));
  }

  private  void testStoreGetConfigCommand(String authType) {
    //without pipeline name
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "get-config");
    Assert.assertTrue(cliOutput.contains("Required option '-n' is missing"));

    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "get-config", "-n", "Sample Pipeline");
    Assert.assertTrue(cliOutput.contains("Sample Pipeline"));
    Assert.assertTrue(cliOutput.contains("description for pipeline"));
    Assert.assertTrue(cliOutput.contains("schemaVersion"));

    //Try to get invalid pipeline
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "get-config", "-n", "Not a valid Pipeline");
    Assert.assertTrue(cliOutput.contains("RemoteException"));
    Assert.assertTrue(cliOutput.contains("CONTAINER_0200 - Pipeline"));

  }

  private  void testStoreUpdateConfigCommand(String authType) throws IOException {
    String pipelineConfig = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "get-config", "-n", "Sample Pipeline");
    FileUtils.writeStringToFile(new File("target/testStoreUpdateConfigCommand.json"), pipelineConfig);

    //without pipeline name
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "update-config", "-f", "testStoreUpdateConfigCommand.json");
    Assert.assertTrue(cliOutput.contains("Required option '-n' is missing"));

    //without file name
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "update-config", "-n", "Sample Pipeline");
    Assert.assertTrue(cliOutput.contains("Required option '-f' is missing"));

    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "update-config", "-n", "Sample Pipeline", "-f", "target/testStoreUpdateConfigCommand.json");
    Assert.assertTrue(cliOutput.contains("Sample Pipeline"));
    Assert.assertTrue(cliOutput.contains("description for pipeline"));
    Assert.assertTrue(cliOutput.contains("schemaVersion"));
  }

  private  void testStoreGetRulesCommand(String authType) {
    //without pipeline name
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "get-rules");
    Assert.assertTrue(cliOutput.contains("Required option '-n' is missing"));

    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "get-rules", "-n", "Sample Pipeline");
    Assert.assertTrue(cliOutput.contains("metricsRuleDefinitions"));
    Assert.assertTrue(cliOutput.contains("dataRuleDefinitions"));
  }

  private  void testStoreUpdateRulesCommand(String authType) throws IOException {
    String pipelineConfig = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "get-rules", "-n", "Sample Pipeline");
    FileUtils.writeStringToFile(new File("target/testStoreUpdateRulesCommand.json"), pipelineConfig);

    //without pipeline name
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "update-rules", "-f", "target/testStoreUpdateRulesCommand.json");
    Assert.assertTrue(cliOutput.contains("Required option '-n' is missing"));

    //without file name
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "update-rules", "-n", "Sample Pipeline");
    Assert.assertTrue(cliOutput.contains("Required option '-f' is missing"));

    //with all parameters
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "update-rules", "-n", "Sample Pipeline", "-f", "target/testStoreUpdateRulesCommand.json");
    Assert.assertTrue(cliOutput.contains("metricsRuleDefinitions"));
    Assert.assertTrue(cliOutput.contains("dataRuleDefinitions"));
  }

  private  void testStoreExportImportCommand(String authType) throws IOException {
    String filename = "target/testStoreExportImportCommand.json";

    //export - without pipeline name
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "export", "-f", filename);
    Assert.assertTrue(cliOutput.contains("Required option '-n' is missing"));

    //export - without file name
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "export", "-n", "Sample Pipeline");
    Assert.assertTrue(cliOutput.contains("Required option '-f' is missing"));

    //export - with all parameters
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "export", "-n", "Sample Pipeline", "-f", filename);
    Assert.assertTrue(cliOutput.contains("Successfully exported pipeline"));
    Assert.assertTrue((new File(filename)).exists());


    //import - without pipeline name
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "import", "-f", filename);
    Assert.assertTrue(cliOutput.contains("Required option '-n' is missing"));

    //import - without file name
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "import", "-n", "Sample Pipeline");
    Assert.assertTrue(cliOutput.contains("Required option '-f' is missing"));

    //import - with all parameters
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "import", "-n", "Imported Pipeline", "-f", filename);
    Assert.assertTrue(cliOutput.contains("Successfully imported from file"));

    //import - to existing pipeline, should throw error
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
        "import", "-n", "Sample Pipeline", "-f", filename);
    Assert.assertTrue(cliOutput.contains("CONTAINER_0201 - Pipeline 'Sample Pipeline' already exists"));

    //import - to existing pipeline with overwrite option
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
        "import", "-n", "Sample Pipeline", "-f", filename, "-o");
    Assert.assertTrue(cliOutput.contains("Successfully imported from file"));
  }


  private  void testManagerCommand(String authType) {
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "create", "-n", "Manager Pipeline", "-d", "description for pipeline");
    Assert.assertTrue(cliOutput.contains("Manager Pipeline"));
    Assert.assertTrue(cliOutput.contains("description for pipeline"));
    Assert.assertTrue(cliOutput.contains("schemaVersion"));

    //Get Status - without pipeline name
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "manager",
      "status");
    Assert.assertTrue(cliOutput.contains("Required option '-n' is missing"));

    //Get status
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "manager",
      "status", "-n", "Manager Pipeline");
    Assert.assertTrue(cliOutput.contains("\"name\" : \"Manager Pipeline\""));
    Assert.assertTrue(cliOutput.contains("\"status\" : \"EDITED\""));

    //Start Pipeline
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "manager",
      "start", "-n", "Manager Pipeline");
    Assert.assertTrue(cliOutput.contains("\"name\" : \"Manager Pipeline\""));
    Assert.assertTrue(cliOutput.contains("\"status\" : \"STARTING\""));

    //Get Status after starting pipeline
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "manager",
      "status", "-n", "Manager Pipeline");
    Assert.assertTrue(cliOutput.contains("\"name\" : \"Manager Pipeline\""));
    Assert.assertTrue(cliOutput.contains("\"status\" : \"START_ERROR\""));
    Assert.assertTrue(cliOutput.contains("VALIDATION_0001 - The pipeline is empty..."));

  }


  private  void testPreviewCommand(String authType) {
    //create sample pipeline for preview
    String cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "store",
      "create", "-n", "Preview Pipeline", "-d", "description for pipeline");
    Assert.assertTrue(cliOutput.contains("Preview Pipeline"));
    Assert.assertTrue(cliOutput.contains("description for pipeline"));
    Assert.assertTrue(cliOutput.contains("schemaVersion"));

    //validate pipeline - without pipeline name
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "preview",
      "validate-pipeline");
    Assert.assertTrue(cliOutput.contains("Required option '-n' is missing"));

    //validate pipeline
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "preview",
      "validate-pipeline", "-n", "Preview Pipeline");
    Assert.assertTrue(cliOutput.contains("previewerId"));
    //Assert.assertTrue(cliOutput.contains("\"status\" : \"VALIDATING\""));

    //Run Preview - without pipeline name
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "preview",
      "run");
    Assert.assertTrue(cliOutput.contains("Required option '-n' is missing"));

    //Run Preview
    cliOutput = TestUtil.runCliCommand("-U", baseURL, "-a", authType, "-u", "admin", "-p", "admin", "preview",
      "run", "-n", "Preview Pipeline");
    Assert.assertTrue(cliOutput.contains("previewerId"));
    //Assert.assertTrue(cliOutput.contains("\"status\" : \"RUNNING\""));
  }
}
