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
package com.streamsets.datacollector.cluster;

import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.io.IOUtils;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarHeader;
import org.kamranzafar.jtar.TarOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class TarFileCreator {

  private TarFileCreator() {}

  public static void createLibsTarGz(
      List<URL> apiCl,
      List<URL> containerCL,
      List<URL> asterClientCL,
      Map<String, List<URL>> streamsetsLibsCl,
      Map<String, List<URL>> userLibsCL,
      File staticWebDir,
      File outputFile
  ) throws IOException {
    long now = System.currentTimeMillis() / 1000L;
    FileOutputStream dest = new FileOutputStream(outputFile);
    TarOutputStream out = new TarOutputStream(new BufferedOutputStream(new GZIPOutputStream(dest), 65536));
    // api-lib
    String prefix = ClusterModeConstants.API_LIB;
    out.putNextEntry(new TarEntry(TarHeader.createHeader(prefix, 0L, now, true)));
    addClasspath(prefix, out, apiCl);

    prefix = ClusterModeConstants.CONTAINER_LIB;
    out.putNextEntry(new TarEntry(TarHeader.createHeader(prefix, 0L, now, true)));
    addClasspath(prefix, out, containerCL);

    prefix = ClusterModeConstants.ASTER_CLIENT_LIB;
    out.putNextEntry(new TarEntry(TarHeader.createHeader(prefix, 0L, now, true)));
    addClasspath(prefix, out, asterClientCL);

    addLibrary(ClusterModeConstants.STREAMSETS_LIBS, now, out, streamsetsLibsCl);
    addLibrary(ClusterModeConstants.USER_LIBS, now, out, userLibsCL);
    tarFolder(null, staticWebDir.getAbsolutePath(), out);
    out.putNextEntry(new TarEntry(TarHeader.createHeader("libs-common-lib", 0L, now, true)));
    out.flush();
    out.close();
  }

  public static void createTarGz(File dir,
                                 File outputFile) throws IOException {
    Utils.checkState(dir.isDirectory(), Utils.formatL("Path {} is not a directory", dir));
    Utils.checkState(dir.canRead(), Utils.formatL("Directory {} cannot be read", dir));
    FileOutputStream dest = new FileOutputStream(outputFile);
    TarOutputStream out = new TarOutputStream(new BufferedOutputStream(new GZIPOutputStream(dest), 65536));
    File[] files = dir.listFiles();
    Utils.checkState(files != null, Utils.formatL("Directory {} could not be read", dir));
    if(files.length > 0) {
      tarFolder(null, dir.getAbsolutePath(), out);
    }
    out.close();
  }

  private static void addLibrary(final String originalPrefix, long now, TarOutputStream out,
                                 Map<String, List<URL>> lib) throws IOException {
    out.putNextEntry(new TarEntry(TarHeader.createHeader(originalPrefix, 0L, now, true)));
    for (Map.Entry<String, List<URL>> entry : lib.entrySet()) {
      String prefix = originalPrefix;
      prefix += "/" + entry.getKey();
      out.putNextEntry(new TarEntry(TarHeader.createHeader(prefix, 0L, now, true)));
      prefix += "/lib";
      out.putNextEntry(new TarEntry(TarHeader.createHeader(prefix, 0L, now, true)));
      addClasspath(prefix, out, entry.getValue());
    }
  }

  private static void addClasspath(String prefix, TarOutputStream out, List<URL> urls)
    throws IOException {
    if (urls != null) {
      for (URL url : urls) {
        File file = new File(url.getPath());
        String name = file.getName();
        if (name.endsWith(".jar")) {
          out.putNextEntry(new TarEntry(file, prefix + "/" + file.getName()));
          BufferedInputStream src = new BufferedInputStream(new FileInputStream(file), 65536);
          IOUtils.copy(src, out);
          src.close();
          out.flush();
        }
      }
    }
  }

  /**
   * Copied from https://raw.githubusercontent.com/kamranzafar/jtar/master/src/test/java/org/kamranzafar/jtar/JTarTest.java
   */
  private static void tarFolder(String parent, String path, TarOutputStream out) throws IOException {
    BufferedInputStream src = null;
    File f = new File(path);
    String files[] = f.list();
    // is file
    if (files == null) {
      files = new String[1];
      files[0] = f.getName();
    }
    parent = ((parent == null) ? (f.isFile()) ? "" : f.getName() + "/" : parent + f.getName() + "/");
    for (int i = 0; i < files.length; i++) {
      File fe = f;
      if (f.isDirectory()) {
        fe = new File(f, files[i]);
      }
      if (fe.isDirectory()) {
        String[] fl = fe.list();
        if (fl != null && fl.length != 0) {
          tarFolder(parent, fe.getPath(), out);
        } else {
          TarEntry entry = new TarEntry(fe, parent + files[i] + "/");
          out.putNextEntry(entry);
        }
        continue;
      }
      FileInputStream fi = new FileInputStream(fe);
      src = new BufferedInputStream(fi);
      TarEntry entry = new TarEntry(fe, parent + files[i]);
      out.putNextEntry(entry);
      IOUtils.copy(src, out);
      src.close();
      out.flush();
    }
  }
}
