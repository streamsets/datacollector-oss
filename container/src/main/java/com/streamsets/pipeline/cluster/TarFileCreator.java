/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

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
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

public class TarFileCreator {

  public static void createLibsTarGz(URLClassLoader apiCl, URLClassLoader containerCL,
                                         Map<String, URLClassLoader> streamsetsLibsCl,
                                         Map<String, URLClassLoader> userLibsCL,
                                          Set<String> excludedJarPrefixes,
                                         File staticWebDir,
                                         File outputFile) throws IOException {
    long now = System.currentTimeMillis() / 1000L;
    FileOutputStream dest = new FileOutputStream(outputFile);
    TarOutputStream out = new TarOutputStream(new BufferedOutputStream(new GZIPOutputStream(dest), 65536));
    // api-lib
    String prefix = ClusterModeConstants.API_LIB;
    out.putNextEntry(new TarEntry(TarHeader.createHeader(prefix, 0L, now, true)));
    addClasspath(prefix, out, apiCl.getURLs(), excludedJarPrefixes);
    prefix = ClusterModeConstants.CONTAINER_LIB;
    out.putNextEntry(new TarEntry(TarHeader.createHeader(prefix, 0L, now, true)));
    addClasspath(prefix, out, containerCL.getURLs(), excludedJarPrefixes);
    addLibrary(ClusterModeConstants.STREAMSETS_LIBS, now, out, streamsetsLibsCl, excludedJarPrefixes);
    addLibrary(ClusterModeConstants.USER_LIBS, now, out, userLibsCL, excludedJarPrefixes);
    tarFolder(null, staticWebDir.getAbsolutePath(), out);
    out.flush();
    out.close();
  }

  public static void createTarGz(File dir,
                                 File outputFile) throws IOException {
    Utils.checkState(dir.isDirectory(), Utils.formatL("Path {} is not a directory", dir));
    Utils.checkState(dir.canRead(), Utils.formatL("Directory {} cannot be read", dir));
    long now = System.currentTimeMillis() / 1000L;
    FileOutputStream dest = new FileOutputStream(outputFile);
    TarOutputStream out = new TarOutputStream(new BufferedOutputStream(new GZIPOutputStream(dest), 65536));
    File[] files = dir.listFiles();
    Utils.checkState(files != null, Utils.formatL("Directory {} could not be read", dir));
    Utils.checkState(files.length > 0, Utils.formatL("Directory {} is empty", dir));
    tarFolder(null, dir.getAbsolutePath(), out);
    out.close();
  }

  private static void addLibrary(final String originalPrefix, long now, TarOutputStream out,
                                 Map<String, URLClassLoader> lib, Set<String> excludedJarPrefixes) throws IOException {
    out.putNextEntry(new TarEntry(TarHeader.createHeader(originalPrefix, 0L, now, true)));
    for (Map.Entry<String, URLClassLoader> entry : lib.entrySet()) {
      String prefix = originalPrefix;
      prefix += "/" + entry.getKey();
      out.putNextEntry(new TarEntry(TarHeader.createHeader(prefix, 0L, now, true)));
      prefix += "/lib";
      out.putNextEntry(new TarEntry(TarHeader.createHeader(prefix, 0L, now, true)));
      addClasspath(prefix, out, entry.getValue().getURLs(), excludedJarPrefixes);
    }
  }

  private static void addClasspath(String prefix, TarOutputStream out, URL[] urls, Set<String> excludedJarPrefixes)
    throws IOException {
    if (urls != null) {
      String dirname = null;
      for (URL url : urls) {
        File file = new File(url.getPath());
        String name = file.getName();
        if (name.endsWith(".jar")) {
          boolean include = true;
          for (String excludedPrefix : excludedJarPrefixes) {
            if (name.startsWith(excludedPrefix)) {
              include = false;
              break;
            }
          }
          if (include) {
            if (dirname == null) {
              dirname = file.getParent();
            } else if (!dirname.equals(file.getParent())) {
              String msg = Utils.format("Expected {} to be a sub-directory of {}", file.getPath(), dirname);
              throw new IllegalStateException(msg);
            }
            out.putNextEntry(new TarEntry(file, prefix + "/" + file.getName()));
            BufferedInputStream src = new BufferedInputStream(new FileInputStream(file), 65536);
            IOUtils.copy(src, out);
            src.close();
            out.flush();
          }
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
