/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.play;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AppClassLoader extends URLClassLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(AppClassLoader.class);

  public static URL[] getURLsFromClasspath(String classpath)
      throws MalformedURLException {
    List<URL> urls = new ArrayList<URL>();
    for (String element : classpath.split(File.pathSeparator)) {
      if (element.endsWith("/*")) {
        String dir = element.substring(0, element.length() - 1);
        File[] files = new File(dir).listFiles(
            new FilenameFilter() {
              @Override
              public boolean accept(File dir, String name) {
                return dir.isDirectory() || name.endsWith(".jar") || name.endsWith(".JAR");
              }
            }
        );
        if (files != null) {
          for (File file : files) {
            urls.add(file.toURI().toURL());
          }
        }
      } else {
        File file = new File(element);
        if (file.exists()) {
          urls.add(new File(element).toURI().toURL());
        }
      }
    }
    return urls.toArray(new URL[urls.size()]);
  }

  //Java7 & Java8
  private static final List<String> JDK_PACKAGES =
      Arrays.asList("java.", "javax.", "org.ietf.jgss.", "org.omg.", "org.w3c.dom", "org.xml.sax.");

  private final ClassLoader parent;
  private final List<String> sysPackages;

  public AppClassLoader(URL[] urls, ClassLoader parent, List<String> nonShadowablePackages) {
    super(urls, parent);
    sysPackages = new ArrayList<String>(JDK_PACKAGES);
    if (nonShadowablePackages != null) {
      sysPackages.addAll(nonShadowablePackages);
    }
    this.parent = parent;
    if (parent == null) {
      throw new IllegalArgumentException("No parent classloader!");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("URLs {}, Non shadowable packages {}", urls, sysPackages);
    }
  }

  @Override
  public URL getResource(String name) {
    URL url = null;

    if (!isSystemClass(name, sysPackages)) {
      url = findResource(name);
      if (url == null && name.startsWith("/")) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Remove leading / off " + name);
        }
        url = findResource(name.substring(1));
      }
    }

    if (url == null) {
      url = parent.getResource(name);
    }

    if (url != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("getResource(" + name + ")=" + url);
      }
    }

    return url;
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return this.loadClass(name, false);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Loading class: " + name);
    }

    Class<?> c = findLoadedClass(name);
    ClassNotFoundException ex = null;

    if (c == null && !isSystemClass(name, sysPackages)) {
      // Try to load class from this classloader's URLs. Note that this is like
      // the servlet spec, not the usual Java 2 behaviour where we ask the
      // parent to attempt to load first.
      try {
        c = findClass(name);
        if (LOG.isDebugEnabled() && c != null) {
          LOG.debug("Loaded class: " + name + " ");
        }
      } catch (ClassNotFoundException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Class not found", e);
        }
        ex = e;
      }
    }

    if (c == null) { // try parent
      c = parent.loadClass(name);
      if (LOG.isDebugEnabled() && c != null) {
        LOG.debug("Loaded class from parent: " + name + " ");
      }
    }

    if (c == null) {
      throw ex != null ? ex : new ClassNotFoundException(name);
    }

    if (resolve) {
      resolveClass(c);
    }

    return c;
  }

  boolean isSystemClass(String name, List<String> systemClasses) {
    if (systemClasses != null) {
      String canonicalName = name.replace('/', '.');
      while (canonicalName.startsWith(".")) {
        canonicalName = canonicalName.substring(1);
      }
      for (String c : systemClasses) {
        if (canonicalName.startsWith(c)) {
          return true;
        }
      }
    }
    return false;
  }

}