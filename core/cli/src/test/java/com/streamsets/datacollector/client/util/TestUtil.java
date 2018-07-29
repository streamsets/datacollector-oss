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
package com.streamsets.datacollector.client.util;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.client.cli.DataCollector;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.main.MainStandalonePipelineManagerModule;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.Configuration;
import dagger.ObjectGraph;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.UUID;

public class TestUtil {
  public static Task startServer(int port, String authenticationType) throws  Exception {
    String baseDir = createTestDir();
    Assert.assertTrue(new File(baseDir, "etc").mkdir());
    Assert.assertTrue(new File(baseDir, "data").mkdir());
    Assert.assertTrue(new File(baseDir, "log").mkdir());
    Assert.assertTrue(new File(baseDir, "web").mkdir());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, baseDir + "/etc");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, baseDir + "/data");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR, baseDir + "/log");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR, baseDir + "/web");

    Configuration conf = new Configuration();
    conf.set(WebServerTask.HTTP_PORT_KEY, port);
    conf.set(WebServerTask.AUTHENTICATION_KEY, authenticationType);
    Writer writer = writer = new FileWriter(new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX +
      RuntimeInfo.CONFIG_DIR), "sdc.properties"));
    conf.save(writer);
    writer.close();

    if(!authenticationType.equals("none")) {
      File realmFile = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX +
        RuntimeInfo.CONFIG_DIR), authenticationType + "-realm.properties");
      writer = new FileWriter(realmFile);
      writer.write("admin: admin,user,admin\n");
      writer.close();
      Files.setPosixFilePermissions(realmFile.toPath(), ImmutableSet.of(PosixFilePermission.OWNER_EXECUTE,
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE));
    }

    ObjectGraph dagger = ObjectGraph.create(MainStandalonePipelineManagerModule.class);

    Task server = dagger.get(TaskWrapper.class);
    server.init();
    server.run();

    return server;
  }

  public static void stopServer(Task server) {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR);
    if (server != null) {
      server.stop();
    }
  }

  public static String createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    return dir.getAbsolutePath();
  }

  public static String runCliCommand(String... args) {
    System.out.println("$ streamsets cli " + Joiner.on(' ').join(args));
    PrintStream stdout = System.out;
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    DataCollector.main(args);
    String output = outContent.toString();
    System.setOut(stdout);
    System.out.println(output);
    return output;
  }
}
