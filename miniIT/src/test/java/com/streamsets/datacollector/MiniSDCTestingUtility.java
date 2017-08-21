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
package com.streamsets.datacollector;

import com.google.common.io.Resources;
import com.streamsets.datacollector.MiniSDC;
import com.streamsets.datacollector.MiniSDC.ExecutionMode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class MiniSDCTestingUtility {

  private MiniSDC miniSDC;
  private MiniYARNCluster miniYarnCluster;
  private static final Logger LOG = LoggerFactory.getLogger(MiniSDCTestingUtility.class);

  /**
   * Property to determine the dist root where libs can be picked up
   */
  private static final String SDC_DIST_DIR = "sdc.dist.dir";

  /**
   * System property key to get base test directory value
   */
  private static final String BASE_TEST_DIRECTORY_KEY = "test.build.data.basedirectory";

  /**
   * System property to find the test data dir, will be used by mini SDC
   */
  private static final String TEST_DATA_DIR = "test.data.dir";

  public static final String PRESERVE_TEST_DIR = "sdc.testing.preserve.testdir";

  /**
   * Default base directory for test output.
   */
  private static final String DEFAULT_BASE_TEST_DIRECTORY = "target/test-data";

  /**
   * Directory where we put the data for this instance of MiniSDCTestingUtility
   */
  private File dataTestDir = null;

  public MiniSDCTestingUtility() {
  }

  /**
   * Gets the current working directory used by test
   * @return
   * @throws IOException
   */
  public File getDataTestDir() throws IOException {
    if (this.dataTestDir == null) {
      setupDataTestDir();
    }
    return new File(this.dataTestDir.getAbsolutePath());
  }

  /**
   * Sets up a directory for a test to use.
   * @return New directory path, if created.
   * @throws IOException
   */
  protected void setupDataTestDir() throws IOException {
    if (this.dataTestDir != null) {
      LOG.warn("Data test dir already setup in " + dataTestDir.getAbsolutePath());
      return;
    }

    String randomStr = UUID.randomUUID().toString();
    File testPath = new File(getBaseTestDir(), randomStr);

    this.dataTestDir = new File(testPath.toString()).getAbsoluteFile();
    // Will be used by MiniSDC
    System.setProperty(TEST_DATA_DIR, this.dataTestDir.toString());
    if (deleteOnExit()) {
      this.dataTestDir.deleteOnExit();
    }
    dataTestDir.mkdirs();
    LOG.debug("Test data dir setup at " + dataTestDir);
  }

  /**
   * @return True if we should delete testing dirs on exit.
   */
  private static boolean deleteOnExit() {
    String v = System.getProperty(PRESERVE_TEST_DIR);
    // Let default be true, to delete on exit.
    return v == null ? true : !Boolean.parseBoolean(v);
  }

  /**
   * @return Where to write test data; usually {@link #DEFAULT_BASE_TEST_DIRECTORY}
   * @see #setupDataTestDir()
   */
  private File getBaseTestDir() {
    String pathName = System.getProperty(BASE_TEST_DIRECTORY_KEY, DEFAULT_BASE_TEST_DIRECTORY);

    return new File(pathName);
  }

  /**
   * @return True if we removed the test dirs
   * @throws IOException
   */
  public boolean cleanupTestDir() throws IOException {
    if (deleteDir(this.dataTestDir)) {
      this.dataTestDir = null;
      return true;
    }
    return false;
  }

  /**
   * @param dir Directory to delete
   * @return True if we deleted it.
   * @throws IOException
   */
  public static boolean deleteDir(final File dir) throws IOException {
    if (dir == null || !dir.exists()) {
      return true;
    }
    int ntries = 0;
    do {
      ntries += 1;
      try {
        if (deleteOnExit()) {
          FileUtils.deleteDirectory(dir);
        }
        return true;
      } catch (IOException ex) {
        LOG.warn("Failed to delete " + dir.getAbsolutePath());
      } catch (IllegalArgumentException ex) {
        LOG.warn("Failed to delete " + dir.getAbsolutePath(), ex);
      }
    } while (ntries < 5);
    return ntries < 5;
  }

  /**
   * Start mini SDC
   * @param executionMode the Execution mode - could be standalone or cluster
   * @return
   * @throws Exception
   */
  public MiniSDC createMiniSDC(ExecutionMode executionMode)
    throws Exception {
    Properties miniITProps = new Properties();
    InputStream sdcInStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("miniIT.properties");
    miniITProps.load(sdcInStream);
    String sdcDistRoot = (String) miniITProps.get(SDC_DIST_DIR);
    File sdcDistFile = new File(sdcDistRoot);
    if (!sdcDistFile.exists()) {
      throw new RuntimeException("SDC dist root dir " + sdcDistFile.getAbsolutePath()
        + "doesn't exist");
    }
    LOG.info("SDC dist root at " + sdcDistFile.getAbsolutePath());
    sdcInStream.close();

    File target = getDataTestDir();
    String targetRoot = target.getAbsolutePath();
    File etcTarget = new File(target, "etc");
    File resourcesTarget = new File(target, "resources");
    FileUtils.copyDirectory(new File(sdcDistRoot + "/etc"), etcTarget);
    FileUtils.copyDirectory(new File(sdcDistRoot + "/resources"), resourcesTarget);
    FileUtils.copyDirectory(new File(sdcDistRoot + "/libexec"), new File(target, "libexec"));
    // Set execute permissions back on script
    Set<PosixFilePermission> set = new HashSet<PosixFilePermission>();
    set.add(PosixFilePermission.OWNER_EXECUTE);
    set.add(PosixFilePermission.OWNER_READ);
    set.add(PosixFilePermission.OWNER_WRITE);
    set.add(PosixFilePermission.OTHERS_READ);
    Files.setPosixFilePermissions(new File(target, "libexec" + "/_cluster-manager").toPath(), set);
    File staticWebDir = new File(target, "static-web");
    staticWebDir.mkdir();

    setExecutePermission(new File(target, "libexec" + "/_cluster-manager").toPath());
    File log4jProperties = new File(etcTarget, "sdc-log4j.properties");
    if (log4jProperties.exists()) {
      log4jProperties.delete();
    }
    Files.copy(Paths.get(Resources.getResource("log4j.properties").toURI()), log4jProperties.toPath());

    File sdcProperties = new File(etcTarget, "sdc.properties");
    System.setProperty("sdc.conf.dir", etcTarget.getAbsolutePath());
    System.setProperty("sdc.resources.dir", resourcesTarget.getAbsolutePath());
    System.setProperty("sdc.libexec.dir", targetRoot + "/libexec");
    System.setProperty("sdc.static-web.dir", targetRoot + "/static-web");
    rewriteProperties(sdcProperties, executionMode);
    this.miniSDC = new MiniSDC(sdcDistRoot);
    return this.miniSDC;
  }

  public static void setExecutePermission(Path path) throws IOException {
    Set<PosixFilePermission> set = new HashSet<PosixFilePermission>();
    set.add(PosixFilePermission.OWNER_EXECUTE);
    set.add(PosixFilePermission.OWNER_READ);
    set.add(PosixFilePermission.OWNER_WRITE);
    set.add(PosixFilePermission.OTHERS_READ);
    Files.setPosixFilePermissions(path, set);
  }

  public void stopMiniSDC() {
    if (miniSDC != null) {
      miniSDC.stop();
      miniSDC = null;
    }
  }

  public MiniYARNCluster startMiniYarnCluster(String testName, int numNodeManager, int numLocalDir, int numLogDir, YarnConfiguration yarnConfiguration) {
    miniYarnCluster = new MiniYARNCluster(testName, numNodeManager, numLocalDir, numLogDir);
    miniYarnCluster.init(yarnConfiguration);
    miniYarnCluster.start();
    return miniYarnCluster;
  }

  public void stopMiniYarnCluster() {
    if (miniYarnCluster != null) {
      miniYarnCluster.stop();
      miniYarnCluster = null;
    }
  }

  private Map<String, String> getCommonProperties() {
    Map<String, String> commonProps = new HashMap<String, String>();
    // Start on random port
    commonProps.put("http.port", "0");
    // TODO - MiniSDC creates problems with other form of auth
    commonProps.put("http.authentication", "none");
    // Reduce interval of callback
    commonProps.put("callback.server.ping.interval.ms", "3000");
    return commonProps;
  }

  private void rewriteProperties(File sdcPropertiesFile, ExecutionMode executionMode)
    throws IOException {
    InputStream sdcInStream = null;
    OutputStream sdcOutStream = null;
    Properties sdcProperties = new Properties();
    try {
      sdcInStream = new FileInputStream(sdcPropertiesFile);
      sdcProperties.load(sdcInStream);

      for (Map.Entry<String, String> mapEntry : getCommonProperties().entrySet()) {
        sdcProperties.setProperty(mapEntry.getKey(), mapEntry.getValue());
      }

      sdcOutStream = new FileOutputStream(sdcPropertiesFile);
      sdcProperties.store(sdcOutStream, null);
      sdcOutStream.flush();
      sdcOutStream.close();
    } finally {
      if (sdcInStream != null) {
        IOUtils.closeQuietly(sdcInStream);
      }
      if (sdcOutStream != null) {
        IOUtils.closeQuietly(sdcOutStream);
      }
    }
  }

}
