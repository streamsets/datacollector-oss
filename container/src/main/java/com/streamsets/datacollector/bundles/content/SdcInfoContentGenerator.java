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
package com.streamsets.datacollector.bundles.content;

import com.fasterxml.jackson.core.JsonGenerator;
import com.streamsets.datacollector.bundles.BundleContentGenerator;
import com.streamsets.datacollector.bundles.BundleContentGeneratorDef;
import com.streamsets.datacollector.bundles.BundleContext;
import com.streamsets.datacollector.bundles.BundleWriter;
import com.streamsets.datacollector.http.GaugeValue;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;
import java.io.IOException;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Array;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;

@BundleContentGeneratorDef(
  name = "SDC Info",
  description = "Information about Data Collector itself (precise build information, configuration and thread dump, ...).",
  version = 1,
  enabledByDefault = true,
  // Run Info always first to get all metrics and such before rest of the generators might mess with them (memory, ...).
  order = Integer.MIN_VALUE
)
public class SdcInfoContentGenerator implements BundleContentGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(BundleContentGenerator.class);

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

    // JMX
    writeJmx(writer);

    // Thread dump
    threadDump(writer);
  }

  public void threadDump(BundleWriter writer) throws IOException {
    writer.markStartOfFile("runtime/threads.txt");

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threads = threadMXBean.dumpAllThreads(true, true);

    // Sadly we can't easily do info.toString() as the implementation is hardcoded to cut the stack trace only to 8
    // items which does not serve our purpose well. Hence we have custom implementation that prints entire stack trace
    // for all threads.
    for(ThreadInfo info: threads) {
      StringBuilder sb = new StringBuilder("\"" + info.getThreadName() + "\"" + " Id=" + info.getThreadId() + " " + info.getThreadState());
      if (info.getLockName() != null) {
        sb.append(" on " + info.getLockName());
      }
      if (info.getLockOwnerName() != null) {
        sb.append(" owned by \"" + info.getLockOwnerName() + "\" Id=" + info.getLockOwnerId());
      }
      if (info.isSuspended()) {
        sb.append(" (suspended)");
      }
      if (info.isInNative()) {
        sb.append(" (in native)");
      }
      sb.append('\n');
      int i = 0;
      for(StackTraceElement ste : info.getStackTrace()) {
        if (i == 0 && info.getLockInfo() != null) {
          Thread.State ts = info.getThreadState();
          switch (ts) {
            case BLOCKED:
              sb.append("\t-  blocked on " + info.getLockInfo());
              sb.append('\n');
              break;
            case WAITING:
              sb.append("\t-  waiting on " + info.getLockInfo());
              sb.append('\n');
              break;
            case TIMED_WAITING:
              sb.append("\t-  waiting on " + info.getLockInfo());
              sb.append('\n');
              break;
            default:
          }
          sb.append("\tat " + ste.toString());
          sb.append('\n');

          i++;
        }

        for (MonitorInfo mi : info.getLockedMonitors()) {
          if (mi.getLockedStackDepth() == i) {
            sb.append("\t-  locked " + mi);
            sb.append('\n');
          }
        }
      }

      LockInfo[] locks = info.getLockedSynchronizers();
      if (locks.length > 0) {
        sb.append("\n\tNumber of locked synchronizers = " + locks.length);
        sb.append('\n');
        for (LockInfo li : locks) {
          sb.append("\t- " + li);
          sb.append('\n');
        }
      }
      sb.append('\n');

      writer.write(sb.toString());
    }

    writer.markEndOfFile();
  }

  private void listDirectory(String configDir, String name, BundleWriter writer) throws IOException {
    writer.markStartOfFile("dir_listing/" + name);
    Path prefix = Paths.get(configDir);

    try {
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
    } catch (Exception e) {
      LOG.error("Can't generate listing of {} directory: {}", configDir, e.toString(), e);
    }
    writer.markEndOfFile();
  }

  private void printFile(Path path, Path prefix, String type, BundleWriter writer) throws IOException {
    writer.write(type);
    writer.write(";");
    writer.write(getOrWriteError(() ->prefix.relativize(path).toString()));
    writer.write(";");
    writer.write(getOrWriteError(() -> Files.getOwner(path).getName()));
    writer.write(";");
    if("F".equals(type)) {
      writer.write(getOrWriteError(() -> String.valueOf(Files.size(path))));
    }
    writer.write(";");
    writer.write(getOrWriteError(() -> StringUtils.join(Files.getPosixFilePermissions(path), ",")));
    writer.write("\n");
  }

  private interface GetOrWriteError {
    String call() throws IOException;
  }

  private String getOrWriteError(GetOrWriteError getMethod) {
    try {
      return getMethod.call();
    } catch (IOException e) {
      LOG.error("Error while getting metadata: ", e);
      return "ERROR";
    }
  }

  private void writeJmx(BundleWriter writer) throws IOException {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    try (JsonGenerator generator = writer.createGenerator("runtime/jmx.json")) {
      generator.useDefaultPrettyPrinter();
      generator.writeStartObject();
      generator.writeArrayFieldStart("beans");

      for (ObjectName objectName : mBeanServer.queryNames(null, null)) {
        MBeanInfo info;
        try {
          info = mBeanServer.getMBeanInfo(objectName);
        } catch (InstanceNotFoundException | IntrospectionException | ReflectionException ex) {
          LOG.warn("Exception accessing MBeanInfo ", ex);
          continue;
        }

        generator.writeStartObject();
        generator.writeStringField("name", objectName.toString());
        generator.writeObjectFieldStart("attributes");

        for (MBeanAttributeInfo attr : info.getAttributes()) {
          try {
            writeAttribute(
              generator,
              attr.getName(),
              mBeanServer.getAttribute(objectName, attr.getName())
            );
          } catch (MBeanException | AttributeNotFoundException | InstanceNotFoundException | ReflectionException |
              RuntimeMBeanException ex) {
            generator.writeStringField(attr.getName(), "Exception: " + ex.toString());
          }
        }

        generator.writeEndObject();
        generator.writeEndObject();
        writer.writeLn("");
      }

      generator.writeEndArray();
      generator.writeEndObject();
    } finally {
      writer.markEndOfFile();
    }
  }

  private void writeAttribute(JsonGenerator jg, String attName, Object value) throws IOException {
    jg.writeFieldName(attName);
    writeObject(jg, value);
  }

  private void writeObject(JsonGenerator jg, Object value) throws IOException {
    if(value == null) {
      jg.writeNull();
    } else {
      Class<?> c = value.getClass();
      if (c.isArray()) {
        jg.writeStartArray();
        int len = Array.getLength(value);
        for (int j = 0; j < len; j++) {
          Object item = Array.get(value, j);
          writeObject(jg, item);
        }
        jg.writeEndArray();
      } else if(value instanceof Number) {
        Number n = (Number)value;
        if (value instanceof Double && (((Double) value).isInfinite() || ((Double) value).isNaN())) {
          jg.writeString(n.toString());
        } else {
          jg.writeNumber(n.toString());
        }
      } else if(value instanceof Boolean) {
        Boolean b = (Boolean)value;
        jg.writeBoolean(b);
      } else if(value instanceof CompositeData) {
        CompositeData cds = (CompositeData)value;
        CompositeType comp = cds.getCompositeType();
        Set<String> keys = comp.keySet();
        jg.writeStartObject();
        for(String key: keys) {
          writeAttribute(jg, key, cds.get(key));
        }
        jg.writeEndObject();
      } else if(value instanceof TabularData) {
        TabularData tds = (TabularData)value;
        jg.writeStartArray();
        for(Object entry : tds.values()) {
          writeObject(jg, entry);
        }
        jg.writeEndArray();
      } else if (value instanceof GaugeValue) {
        ((GaugeValue)value).serialize(jg);
      } else {
        jg.writeString(value.toString());
      }
    }
  }

}
