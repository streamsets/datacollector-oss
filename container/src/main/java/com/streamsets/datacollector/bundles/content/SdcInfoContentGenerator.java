/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.bundles.content;

import com.streamsets.datacollector.bundles.BundleContentGenerator;
import com.streamsets.datacollector.bundles.BundleContentGeneratorDef;
import com.streamsets.datacollector.bundles.BundleContext;
import com.streamsets.datacollector.bundles.BundleWriter;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Properties;

@BundleContentGeneratorDef(
  name = "SDC Info",
  description = "Information about SDC itself (precise build information, configuration and thread dump, ...).",
  version = 1,
  enabledByDefault = true
)
public class SdcInfoContentGenerator implements BundleContentGenerator {
  private static final String FILE = "F";
  private static final String DIR = "D";

  @Override
  public void generateContent(BundleContext context, BundleWriter writer) throws IOException {
    // Various properties
    writer.write("properties/build.properties", context.getBuildInfo().getInfo());
    writer.write("properties/system.properties", System.getProperties());

    // Interesting directory listings
    listDirectory(context.getRuntimeInfo().getConfigDir(), "conf.txt", writer);
    listDirectory(context.getRuntimeInfo().getResourcesDir(), "resource.txt", writer);
    listDirectory(context.getRuntimeInfo().getDataDir(), "data.txt", writer);
    listDirectory(context.getRuntimeInfo().getLogDir(), "log.txt", writer);
    listDirectory(context.getRuntimeInfo().getLibsExtraDir(), "lib_extra.txt", writer);
    listDirectory(context.getRuntimeInfo().getRuntimeDir() + "/streamsets-libs/", "stagelibs.txt", writer);

    // Interesting files
    String confDir = context.getRuntimeInfo().getConfigDir();
    writer.write("conf", Paths.get(confDir, "sdc.properties"));
    writer.write("conf", Paths.get(confDir, "sdc-log4j.properties"));
    writer.write("conf", Paths.get(confDir, "dpm.properties"));
    writer.write("conf", Paths.get(confDir, "ldap-login.conf"));
    writer.write("conf", Paths.get(confDir, "sdc-security.policy"));
    String libExecDir = context.getRuntimeInfo().getLibexecDir();
    writer.write("libexec", Paths.get(libExecDir, "sdc-env.sh"));
    writer.write("libexec", Paths.get(libExecDir, "sdcd-env.sh"));

    // Thread dump
    threadDump(writer);
  }

  public void threadDump(BundleWriter writer) throws IOException {
    writer.markStartOfFile("runtime/threads.txt");

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threads = threadMXBean.dumpAllThreads(true, true);

    for(ThreadInfo info: threads) {
      writer.write(info.toString());
    }

    writer.markEndOfFile();
  }

  private void listDirectory(String configDir, String name, BundleWriter writer) throws IOException {
    writer.markStartOfFile("dir_listing/" + name);
    Path prefix = Paths.get(configDir);

    Files.walkFileTree(Paths.get(configDir), new FileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        printFile(dir, prefix, DIR, writer);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        printFile(file, prefix, FILE, writer);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
      }
    });
    writer.markEndOfFile();
  }

  private void printFile(Path path, Path prefix, String type, BundleWriter writer) throws IOException {
    writer.write(type);
    writer.write(";");
    writer.write(prefix.relativize(path).toString());
    writer.write(";");
    writer.write(Files.getOwner(path).getName());
    writer.write(";");
    if("F".equals(type)) {
      writer.write(String.valueOf(Files.size(path)));
    }
    writer.write(";");
    writer.write(StringUtils.join(Files.getPosixFilePermissions(path), ","));
    writer.write("\n");
  }

}
