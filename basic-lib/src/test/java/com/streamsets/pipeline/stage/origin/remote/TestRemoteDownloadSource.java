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
package com.streamsets.pipeline.stage.origin.remote;

import com.github.fommil.ssh.SshRsaCrypto;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.io.fileref.FileRefTestUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.common.session.Session;
import org.apache.sshd.common.session.SessionListener;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.auth.password.PasswordChangeRequiredException;
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static java.lang.Thread.currentThread;
import static org.awaitility.Awaitility.await;

public class TestRemoteDownloadSource {
  private final Random RANDOM = new Random();

  private SshServer sshd;
  private int port;
  private String path;
  private String oldWorkingDir;
  private AtomicInteger opened = new AtomicInteger(0);
  private AtomicInteger closed = new AtomicInteger(0);
  private AtomicBoolean closedAll = new AtomicBoolean(true);

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  // SSHD uses the current working directory as the directory from which to serve files. So cd to the correct dir.
  private void cd(String dir, boolean absolutePath) {
    oldWorkingDir = System.getProperty("user.dir");
    String path = null;
    if (absolutePath) {
      path = dir;
    } else {
      URL url = currentThread().getContextClassLoader().getResource(dir);
      path = url.getPath();
    }

    System.setProperty("user.dir", path);
  }

  @BeforeClass
  public static void setUpClass() {
    DataCollectorServicesUtils.loadDefaultServices();
  }

  @Before
  public void before() {
    opened.set(0);
    closed.set(0);
    closedAll.set(true);
  }

  @After
  public void resetWorkingDir() throws Exception {
    if (oldWorkingDir != null) {
      System.setProperty("user.dir", oldWorkingDir);
    }
    if (sshd != null && sshd.isOpen()) {
      sshd.close();
    }
  }

  // Need to make sure each test uses a different dir.
  public void setupSSHD(String dataDir, boolean absolutePath) throws Exception {
    cd(dataDir, absolutePath);
    ServerSocket s = new ServerSocket(0);
    port = s.getLocalPort();
    s.close();
    sshd = SshServer.setUpDefaultServer();
    sshd.setPort(port);
    sshd.setSubsystemFactories(Arrays.<NamedFactory<Command>>asList(new SftpSubsystemFactory()));
    sshd.setPasswordAuthenticator(new PasswdAuth());
    sshd.setPublickeyAuthenticator(new TestPublicKeyAuth());
    sshd.setKeyPairProvider(new HostKeyProvider());

    sshd.addSessionListener(new SessionListener() {
      @Override
      public void sessionCreated(Session session) {
        opened.incrementAndGet();
        closedAll.set(false);
      }

      @Override
      public void sessionEvent(Session session, Event event) {

      }

      @Override
      public void sessionException(Session session, Throwable t) {

      }

      @Override
      public void sessionClosed(Session session) {
        closed.incrementAndGet();
        if (opened.get() == closed.get()) {
          closedAll.set(true);
        }
      }
    });
    sshd.start();
  }

  @Test
  public void testNoError() throws Exception {
    path = "remote-download-source/parseNoError";
    setupSSHD(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
    destroyAndValidate(runner);
  }

  @Test
  public void testRespectsConfiguredBatchSize() throws Exception {
    path = "remote-download-source/parseNoError";
    setupSSHD(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*",
            1
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    for (int j = 0; j < 2; j++) {
      StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
      List<Record> expected = getExpectedRecords();
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
      Assert.assertEquals(expected.get(j).get(), actual.get(0).get());
    }
    destroyAndValidate(runner);
  }


  private void destroyAndValidate(SourceRunner runner) throws Exception {
    runner.runDestroy();
    await()
        .atMost(10, TimeUnit.SECONDS)
        .untilTrue(closedAll);
  }

  @Test(expected = StageException.class)
  public void testWrongPass() throws Exception {
    path = "remote-download-source/parseNoError";
    setupSSHD(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "wrongpass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
    destroyAndValidate(runner);
  }

  @Test
  public void testInvalidURL() throws Exception {
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "wrongpass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    try {
      runner.runInit();
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertTrue(ex.getMessage().contains("REMOTE_15"));
    }
  }

  @Test
  public void testNoErrorPrivateKey() throws Exception {
    path = "remote-download-source/parseNoError";
    setupSSHD(path, false);
    File privateKeyFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/id_rsa_test").getPath());
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            null,
            privateKeyFile.toString(),
            "streamsets",
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
    destroyAndValidate(runner);
  }

  @Test(expected = StageException.class)
  public void testPrivateKeyWrongPassphrase() throws Exception {
    path = "remote-download-source/parseNoError";
    File privateKeyFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/id_rsa_test").getPath());
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            null,
            privateKeyFile.toString(),
            "randomrandom",
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    destroyAndValidate(runner);
  }

  @Test(expected = StageException.class)
  public void testInvalidKey() throws Exception {
    path = "remote-download-source/parseNoError";
    File privateKeyFile = testFolder.newFile("randomkey_rsa");
    privateKeyFile.deleteOnExit();
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            null,
            privateKeyFile.toString(),
            "randomrandom",
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    destroyAndValidate(runner);
  }

  @Test(expected = StageException.class)
  public void testPrivateKeyWithFTP() throws Exception {
    path = "remote-download-source/parseNoError";
    File privateKeyFile = testFolder.newFile("randomkey_rsa");
    privateKeyFile.deleteOnExit();
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "ftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            null,
            privateKeyFile.toString(),
            "streamsets",
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    runner.runDestroy();
  }

  @Test
  public void testNoErrorOrdering() throws Exception {
    path = "remote-download-source/parseSameTimestamp";
    File dir =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseSameTimestamp").getPath());
    File[] files = dir.listFiles();
    Assert.assertEquals(3, files.length);
    for (File f : files) {
      if (f.getName().equals("panda.txt")) {
        Assert.assertTrue(f.setLastModified(18000000L));
      } else if (f.getName().equals("polarbear.txt")) {
        f.setLastModified(18000000L);
      } else if (f.getName().equals("sloth.txt")) {
        f.setLastModified(17000000L);
      }
    }
    setupSSHD(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords();
    Record record = RecordCreator.create();
    record.set(Field.create(new HashMap<String, Field>()));
    record.set("/name", Field.create("polarbear"));
    record.set("/age", Field.create("6"));
    record.set("/characterisitics", Field.create(Arrays.asList(
        Field.create("cool"),
        Field.create("cute"),
        Field.create("huge"),
        Field.create("round"),
        Field.create("playful")
    )));
    expected.add(record);
    String offset = RemoteDownloadSource.NOTHING_READ;
    for (int i = 0; i < 3; i++) {
      StageRunner.Output op = runner.runProduce(offset, 1000);
      offset = op.getNewOffset();
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
      Assert.assertEquals(expected.get(i).get(), actual.get(0).get());
    }
    destroyAndValidate(runner);
  }

  @Test
  public void testInitialFile() throws Exception {
    path = "remote-download-source/parseSameTimestamp";
    File dir =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseSameTimestamp").getPath());
    File[] files = dir.listFiles();
    Assert.assertEquals(3, files.length);
    for (File f : files) {
      if (f.getName().equals("panda.txt")) {
        Assert.assertTrue(f.setLastModified(18000000L));
      } else if (f.getName().equals("polarbear.txt")) {
        f.setLastModified(16000000L);
      } else if (f.getName().equals("sloth.txt")) {
        f.setLastModified(17000000L);
      }
    }
    setupSSHD(path, false);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*",
              1000,
            "sloth.txt"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords();
    String offset = RemoteDownloadSource.NOTHING_READ;
    for (int i = 0; i < 2; i++) {
      StageRunner.Output op = runner.runProduce(offset, 1000);
      offset = op.getNewOffset();
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
      Assert.assertEquals(expected.get(i).get(), actual.get(0).get());
    }
    destroyAndValidate(runner);
  }

  @Test(expected = StageException.class)
  public void testInitialFileDoesntExists() throws Exception {
    path = "remote-download-source/parseSameTimestamp";
    setupSSHD(path, false);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*",
              1000,
            "is-arvind-son-of-god.txt"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      runner.runProduce(null, 1000);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRestartFromMiddleOfFile() throws Exception {
    path = "remote-download-source/parseNoError";
    setupSSHD(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords();
    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op = runner.runProduce(offset, 1);
    offset = op.getNewOffset();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    destroyAndValidate(runner);

    // Create a new source.
    origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();
    op = runner.runProduce(offset, 1);
    actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(1).get(), actual.get(0).get());
    destroyAndValidate(runner);
  }

  @Test
  public void testRestartCompletedFile() throws Exception {
    path = "remote-download-source/parseSameTimestamp";
    File dir =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseSameTimestamp").getPath());
    File[] files = dir.listFiles();
    Assert.assertEquals(3, files.length);
    for (File f : files) {
      if (f.getName().equals("panda.txt")) {
        Assert.assertTrue(f.setLastModified(18000000L));
      } else if (f.getName().equals("polarbear.txt")) {
        Assert.assertTrue(f.setLastModified(18000000L));
      } else if (f.getName().equals("sloth.txt")) {
        Assert.assertTrue(f.setLastModified(17000000L));
      }
    }
    setupSSHD(path, false);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords();
    Record record = RecordCreator.create();
    record.set(Field.create(new HashMap<String, Field>()));
    record.set("/name", Field.create("polarbear"));
    record.set("/age", Field.create("6"));
    record.set("/characterisitics", Field.create(Arrays.asList(
        Field.create("cool"),
        Field.create("cute"),
        Field.create("huge"),
        Field.create("round"),
        Field.create("playful")
    )));
    expected.add(record);
    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op = runner.runProduce(offset, 1);
    offset = op.getNewOffset();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    destroyAndValidate(runner);

    // Create a new source.
    origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();
    // Since we don't proactively close steams, we must hit at least one null event in a batch to close the current
    // stream and open the next one, else the next batch will be empty and the data comes in the batch following that.
    op =runner.runProduce(offset, 1); // Forces /sloth.txt file parse to return -1
    op = runner.runProduce(op.getNewOffset(), 1);
    offset = op.getNewOffset();
    actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(1).get(), actual.get(0).get());

    op =runner.runProduce(offset, 1); //Forces /panda.txt file parse to return -1
    offset = op.getNewOffset();
    op = runner.runProduce(offset, 2);
    actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(2).get(), actual.get(0).get());
    destroyAndValidate(runner);
  }

  @Test
  public void testOverrunErrorArchiveFile() throws Exception {
    path = "remote-download-source/parseOverrun";
    setupSSHD(path, false);
    File archiveDir = testFolder.newFolder();
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            archiveDir.toString(),
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords();
    Record record = RecordCreator.create();
    record.set(Field.create(new HashMap<String, Field>()));
    record.set("/name", Field.create("polarbear"));
    record.set("/age", Field.create("6"));
    record.set("/characterisitics", Field.create(Arrays.asList(
        Field.create("cool"),
        Field.create("cute"),
        Field.create("huge"),
        Field.create("round"),
        Field.create("playful")
    )));
    expected.add(record);
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    Assert.assertEquals(1, archiveDir.listFiles().length);
    File expectedFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseOverrun").getPath()).listFiles()[0];
    File actualFile = archiveDir.listFiles()[0];
    Assert.assertEquals(expectedFile.getName(), actualFile.getName());
    Assert.assertTrue(FileUtils.contentEquals(expectedFile, actualFile));
    destroyAndValidate(runner);
  }

  @Test
  public void testOverrunErrorArchiveFileRecovery() throws Exception {
    path = "remote-download-source/parseRecoveryFromFailure";
    File dir =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseRecoveryFromFailure").getPath());
    File[] files = dir.listFiles();
    for (File f : files) {
      if (f.getName().equals("polarbear.txt")) {
        f.setLastModified(18000000L);
      } else if (f.getName().equals("longobject.txt")) {
        f.setLastModified(17500000L);
      } else if (f.getName().equals("sloth.txt")) {
        f.setLastModified(17000000L);
      }
    }
    setupSSHD(path, false);
    File archiveDir = testFolder.newFolder();
    FilenameFilter filter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.equals("longobject.txt");
      }
    };
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            archiveDir.toString(),
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords();
    Record record = RecordCreator.create();
    record.set(Field.create(new HashMap<String, Field>()));
    record.set("/name", Field.create("polarbear"));
    record.set("/age", Field.create("6"));
    record.set("/characterisitics", Field.create(Arrays.asList(
        Field.create("cool"),
        Field.create("cute"),
        Field.create("huge"),
        Field.create("round"),
        Field.create("playful")
    )));
    expected.add(record);
    Assert.assertEquals(0, archiveDir.listFiles().length);
    String offset = RemoteDownloadSource.NOTHING_READ;
    for (int i = 0; i < 3; i++) {
      StageRunner.Output op = runner.runProduce(offset, 1000);
      offset = op.getNewOffset();
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
      if (i >= 1) { //longobject
        Assert.assertEquals(1, archiveDir.listFiles().length);
        continue;
      } else {
        Assert.assertEquals(0, archiveDir.listFiles().length);
      }
      Assert.assertEquals(expected.get(i).get(), actual.get(0).get());
    }
    Assert.assertEquals(1, archiveDir.listFiles().length);
    File expectedFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseRecoveryFromFailure").getPath()).listFiles(filter)[0];

    File actualFile = archiveDir.listFiles(filter)[0];
    Assert.assertEquals(expectedFile.getName(), actualFile.getName());
    Assert.assertTrue(FileUtils.contentEquals(expectedFile, actualFile));
    destroyAndValidate(runner);
  }

  @Test
  public void testOverrunError() throws Exception {
    path = "remote-download-source/parseOverrun";
    setupSSHD(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.DISCARD)
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    destroyAndValidate(runner);
  }

  @Test
  public void testParseError() throws Exception {
    path = "remote-download-source/parseError";
    File dir =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseError").getPath());
    File[] files = dir.listFiles();
    Assert.assertEquals(2, files.length);
    for (File f : files) {
      if (f.getName().equals("polarbear.txt")) {
        f.setLastModified(15000000L);
      } else if (f.getName().equals("sloth.txt")) {
        f.setLastModified(17000000L);
      }
    }
    setupSSHD(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.DISCARD)
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    List<Record> expected = new ArrayList<>();
    Record record = RecordCreator.create();
    record.set(Field.create(new HashMap<String, Field>()));
    record.set("/name", Field.create("polarbear"));
    record.set("/age", Field.create("6"));
    record.set("/characterisitics", Field.create(Arrays.asList(
        Field.create("cool"),
        Field.create("cute"),
        Field.create("huge"),
        Field.create("round"),
        Field.create("playful")
    )));
    expected.add(record);
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    String offset = op.getNewOffset();
    op = runner.runProduce(offset, 1000);
    Assert.assertEquals("/sloth.txt::17000000::-1", op.getNewOffset());
    actual = op.getRecords().get("lane");
    Assert.assertEquals(0, actual.size());

    op = runner.runProduce(offset, 1000);
    Assert.assertEquals("/sloth.txt::17000000::-1", op.getNewOffset());
    actual = op.getRecords().get("lane");
    Assert.assertEquals(0, actual.size());

    destroyAndValidate(runner);
  }

  private List<Record> getExpectedRecords() {
    List<Record> records = new ArrayList<>(2);
    Record record = RecordCreator.create();
    record.set(Field.create(new HashMap<String, Field>()));
    record.set("/name", Field.create("sloth"));
    record.set("/age", Field.create("5"));
    record.set("/characterisitics", Field.create(Arrays.asList(
        Field.create("cute"),
        Field.create("slooooow"),
        Field.create("sloooooow"),
        Field.create("sloooooooow")
    )));
    records.add(record);

    record = RecordCreator.create();
    record.set(Field.create(new HashMap<String, Field>()));
    record.set("/name", Field.create("panda"));
    record.set("/age", Field.create("3"));
    record.set("/characterisitics", Field.create(Arrays.asList(
        Field.create("cool"),
        Field.create("cute"),
        Field.create("round"),
        Field.create("playful"),
        Field.create("hungry")
    )));
    records.add(record);
    return records;
  }

  @Test
  public void testPicksUpNewFiles() throws Exception {
    String originPath =
        currentThread().getContextClassLoader().getResource("remote-download-source/parseNoError").getPath();
    File originDirFile = new File(originPath).listFiles()[0];
    File tempDir = testFolder.newFolder();
    File copied = new File(tempDir, originDirFile.getName());
    Files.copy(originDirFile, copied);
    long lastModified = copied.lastModified();
    setupSSHD(tempDir.toString(), true);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op = runner.runProduce(offset, 1000);
    offset = op.getNewOffset();
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
    File eventualFile = new File(tempDir, "z" + originDirFile.getName() + RemoteDownloadSource.NOTHING_READ);
    Files.copy(originDirFile, eventualFile);
    eventualFile.setLastModified(lastModified);
    op = runner.runProduce(offset, 1000);
    expected = getExpectedRecords();
    actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
    destroyAndValidate(runner);
  }

  @Test
  public void testFtp() throws Exception {
    FakeFtpServer fakeFtpServer = new FakeFtpServer();
    fakeFtpServer.setServerControlPort(0);
    FileSystem fileSystem = new UnixFakeFileSystem();
    File file =
        new File(currentThread().getContextClassLoader().getResource("remote-download-source/parseNoError").getFile())
            .listFiles()[0];
    String value = FileUtils.readFileToString(file);

    fileSystem.add(new FileEntry("/testfile", value));
    fakeFtpServer.setFileSystem(fileSystem);

    UserAccount userAccount = new UserAccount("testuser", "pass", "/");
    fakeFtpServer.addUserAccount(userAccount);
    fakeFtpServer.start();
    int port = fakeFtpServer.getServerControlPort();
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "ftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords();
    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op = runner.runProduce(offset, 1000);
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
    runner.runDestroy();
  }


  @Test
  public void testWholeFile() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testWholeFile";

    Path filePath = Paths.get(path + "/testWholeFile.txt");

    Assert.assertTrue(new File(path).mkdirs());

    java.nio.file.Files.write(
        filePath,
        "This is sample text".getBytes(),
        StandardOpenOption.CREATE_NEW
    );

    setupSSHD(path, true);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.WHOLE_FILE,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    try {
      runner.runInit();

      StageRunner.Output op = runner.runProduce("null", 1000);

      List<Record> actual = op.getRecords().get("lane");

      Assert.assertEquals(1, actual.size());
      Record record = actual.get(0);

      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH));
      Assert.assertTrue(record.has(FileRefUtil.FILE_REF_FIELD_PATH));

      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + RemoteDownloadSource.SIZE));
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + RemoteDownloadSource.LAST_MODIFIED_TIME));
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + RemoteDownloadSource.CONTENT_TYPE));
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + RemoteDownloadSource.CONTENT_ENCODING));

      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + RemoteDownloadSource.REMOTE_URI));
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + HeaderAttributeConstants.FILE_NAME));
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + HeaderAttributeConstants.FILE));

      Assert.assertEquals("testWholeFile.txt", record.getHeader().getAttribute(HeaderAttributeConstants.FILE_NAME));
      Assert.assertEquals("/testWholeFile.txt", record.getHeader().getAttribute(HeaderAttributeConstants.FILE));

      Assert.assertEquals("testWholeFile.txt", record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + HeaderAttributeConstants.FILE_NAME).getValueAsString());
      Assert.assertEquals("/testWholeFile.txt", record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + HeaderAttributeConstants.FILE).getValueAsString());
      Assert.assertEquals("sftp://localhost:" + String.valueOf(port) + "/", record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + RemoteDownloadSource.REMOTE_URI).getValueAsString());

      InputStream is1 = new FileInputStream(filePath.toFile());

      InputStream is2 = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef().createInputStream(runner.getContext(), InputStream.class);

      FileRefTestUtil.checkFileContent(is1, is2);

      List<Record> records = runner.runProduce(op.getNewOffset(), 1000).getRecords().get("lane");
      Assert.assertEquals(0, records.size());
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testWholeFileMultipleInputStreams() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testWholeFileMultipleInputStreams";

    Assert.assertTrue(new File(path).mkdirs());

    Path filePath = Paths.get(path + "/testWholeFileMultipleInputStreams.txt");

    java.nio.file.Files.write(
        Paths.get(path + "/testWholeFileMultipleInputStreams.txt"),
        "This is sample text".getBytes(),
        StandardOpenOption.CREATE_NEW
    );

    setupSSHD(path, true);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.WHOLE_FILE,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    try {
      runner.runInit();

      StageRunner.Output op = runner.runProduce("null", 1000);

      List<Record> actual = op.getRecords().get("lane");

      Assert.assertEquals(1, actual.size());
      Record record = actual.get(0);

      InputStream is1 = new FileInputStream(filePath.toFile());

      InputStream is2 = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef().createInputStream(runner.getContext(), InputStream.class);

      FileRefTestUtil.checkFileContent(is1, is2);

      //create the input streams again

      is1 = new FileInputStream(filePath.toFile());

      is2 = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef().createInputStream(runner.getContext(), InputStream.class);

      FileRefTestUtil.checkFileContent(is1, is2);

      List<Record> records = runner.runProduce(op.getNewOffset(), 1000).getRecords().get("lane");
      Assert.assertEquals(0, records.size());
    } finally {
      runner.runDestroy();
    }
  }


  private Pair<String, List<Record>> runSourceAndReturnOffsetAndRecords(
      String lastOffset,
      int batchSize
  ) throws Exception {
    RemoteDownloadConfigBean configBean = getBean(
        "sftp://localhost:" + String.valueOf(port) + "/",
        true,
        "testuser",
        "pass",
        null,
        null,
        null,
        true,
        DataFormat.TEXT,
        null,
        //Process subdirectories
        false,
        "*.zip"
    );
    configBean.dataFormatConfig.compression = Compression.ARCHIVE;
    configBean.dataFormatConfig.filePatternInArchive = "testReadArchive/*.txt";
    RemoteDownloadSource origin =
        new RemoteDownloadSource(configBean);
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      StageRunner.Output op = runner.runProduce(lastOffset, batchSize);
      List<Record> records = op.getRecords().get("lane");
      return Pair.of(op.getNewOffset(), records);
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testReadArchive() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testReadArchive";

    Assert.assertTrue(new File(path).mkdirs());

    String pathPrefixInsideTheArchive = "testReadArchive";

    int numberOfFilesInArchive = 5;
    int numberOfRecordsInAFile = 10;

    List<String> internalArchiveFiles = new ArrayList<>();
    //Zip archive file
    //Writing 5 different files each with 10 records.
    Path archiveFilePath = Paths.get(path + "/testReadArchive.zip");
    try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(archiveFilePath.toFile()))) {
      for (int i = 0; i < numberOfFilesInArchive; i++) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < numberOfRecordsInAFile; j++) {
          if (j != 0) {
            sb.append("\n");
          }
          sb.append(j);
        }
        String entry = pathPrefixInsideTheArchive + "/testReadArchive" + i + ".txt";
        internalArchiveFiles.add(entry);
        zos.putNextEntry(new ZipEntry(entry));
        zos.write(sb.toString().getBytes());
        zos.closeEntry();
      }
      zos.finish();
      zos.flush();
    }

    //Write 10 records in each file
    setupSSHD(path, true);

    int expectedTotalNoOfRecords = numberOfFilesInArchive * numberOfRecordsInAFile;
    int recordCount = 0;
    String lastOffset = "";

    //Run till all records are read.
    while (recordCount < expectedTotalNoOfRecords) {
      int randomBatchSize = RANDOM.nextInt(numberOfRecordsInAFile);
      //This is like pipeline stop and start, because we create a new source and runner
      //but use the old offset
      Pair<String, List<Record>> output =
          runSourceAndReturnOffsetAndRecords(lastOffset, randomBatchSize);

      List<Record> currentBatchRecords = output.getRight();

      for (Record record : currentBatchRecords) {
        //Read record and check headers to see whether the files are read properly
        int fileIdxInsideTheArchive = recordCount / numberOfRecordsInAFile;
        String currentInnerArchiveFileEntryName =
            internalArchiveFiles.get(fileIdxInsideTheArchive)
                .replace(pathPrefixInsideTheArchive + "/", "");

        Assert.assertNotNull(record.getHeader().getAttribute("fileNameInsideArchive"));
        Assert.assertEquals(
            currentInnerArchiveFileEntryName,
            record.getHeader().getAttribute("fileNameInsideArchive")
        );
        Assert.assertNotNull(record.getHeader().getAttribute("filePathInsideArchive"));
        Assert.assertEquals(
            pathPrefixInsideTheArchive,
            record.getHeader().getAttribute("filePathInsideArchive")
        );
        //Each record in a file two bytes (so multiply the result of the record idx in a file by 2)
        long currentOffset = (recordCount - (fileIdxInsideTheArchive * numberOfRecordsInAFile)) * 2;

        Assert.assertNotNull(record.getHeader().getAttribute("fileOffsetInsideArchive"));
        Assert.assertEquals(
            String.valueOf(currentOffset),
            record.getHeader().getAttribute("fileOffsetInsideArchive")
        );
        recordCount++;
      }
      lastOffset = output.getLeft();
    }
    Assert.assertEquals(expectedTotalNoOfRecords, recordCount);

    Pair<String, List<Record>> output =
        runSourceAndReturnOffsetAndRecords(lastOffset, 5);
    Assert.assertEquals(0, output.getRight().size());
  }


  @Test
  public void testHeaderAttributes() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testHeaderAttributes";
    List<String> foldersToCreate = ImmutableList.of("/folder1/folder11", "/folder2/folder22", "/folder3/folder33", "/folder4/folder44");
    List<String> filePaths = new ArrayList<>();

    long currrentMillis = System.currentTimeMillis() - (foldersToCreate.size() * 1000);
    for (String folder : foldersToCreate) {

      String fullFolderPath = path + folder ;
      Assert.assertTrue(new File(fullFolderPath).mkdirs());

      String fileFullPath = fullFolderPath + "/" + folder.substring(1).replaceAll("/", "_") + "testHeaderAttributes.txt";

      //creating file name as _ instead of / in folder
      //for ex: folder1/folder11 will have the file name folder1_folder11_testHeaderAttributes.txt
      java.nio.file.Files.write(
          Paths.get(fileFullPath),
          "This is sample text".getBytes(),
          StandardOpenOption.CREATE_NEW
      );

      Assert.assertTrue(Paths.get(fileFullPath).toFile().setLastModified(currrentMillis));
      currrentMillis = currrentMillis + 1000;
      //Strip local file path
      filePaths.add(fileFullPath.substring(path.length()));
    }

    setupSSHD(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.TEXT,
            null,
            //Process subdirectories
            true,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    try {
      runner.runInit();
      String offset = "";
      for (int i = 0; i < filePaths.size(); i++) {
        StageRunner.Output op = runner.runProduce(offset, 1000);
        List<Record> records = op.getRecords().get("lane");
        Assert.assertEquals(1, records.size());
        Record.Header header = records.get(0).getHeader();
        String fileFullPath = filePaths.get(i);
        Assert.assertEquals(fileFullPath, header.getAttribute(HeaderAttributeConstants.FILE));
        String fileName = fileFullPath.substring(fileFullPath.lastIndexOf("/") + 1);
        Assert.assertEquals(fileName, header.getAttribute(HeaderAttributeConstants.FILE_NAME));
        Assert.assertNotNull(header.getAttribute(RemoteDownloadSource.REMOTE_URI));
        offset = op.getNewOffset();
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReadFileInMultipleRuns() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testReadFileInMultipleRuns";

    Path filePath =  Paths.get(path + "/testReadFileInMultipleRuns.txt");

    Assert.assertTrue(new File(path).mkdirs());

    java.nio.file.Files.write(
        filePath,
        ("1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n" + "7\n" + "8\n" + "9\n" + "10\n").getBytes(),
        StandardOpenOption.CREATE_NEW
    );

    setupSSHD(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.TEXT,
            null,
            false,
            "*"
        ));

    int totalRecordsRead = 0, runTimes = 0, expectedRecordCount = 10, totalRunTimes = expectedRecordCount * 2;
    String lastOffset = RemoteDownloadSource.NOTHING_READ;
    while (runTimes < totalRunTimes) {
      SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
          .addOutputLane("lane")
          .build();
      runner.runInit();
      try {
        //no more files to process, this will fail.
        StageRunner.Output op = runner.runProduce(lastOffset, 5);
        lastOffset = op.getNewOffset();
        List<Record> actual = op.getRecords().get("lane");
        totalRecordsRead += actual.size();
      } finally {
        runner.runDestroy();
      }
      runTimes++;
    }
    Assert.assertEquals(expectedRecordCount, totalRecordsRead);
  }


  @Test
  public void testEvents() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testEvents";

    Assert.assertTrue(new File(path).mkdirs());

    Path filePath1 = Paths.get(path + "/testEvents1.txt");
    Path filePath2 = Paths.get(path + "/testEvents2.txt");

    byte[] sampleText = "This is sample text".getBytes();

    java.nio.file.Files.write(filePath1, sampleText, StandardOpenOption.CREATE_NEW);
    java.nio.file.Files.write(filePath2, sampleText, StandardOpenOption.CREATE_NEW);

    setupSSHD(path, true);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.TEXT,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProduce("", 10);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(2, runner.getEventRecords().size());
      List<Record> eventRecords = runner.getEventRecords();
      Record newFileEvent = eventRecords.get(0);
      Record finishedFileEvent = eventRecords.get(1);

      Assert.assertEquals("new-file", newFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals("finished-file", finishedFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals("/" + filePath1.getFileName().toString(), newFileEvent.get("/filepath").getValueAsString());
      Assert.assertEquals("/" + filePath1.getFileName().toString(), finishedFileEvent.get("/filepath").getValueAsString());
      Assert.assertEquals(1, finishedFileEvent.get("/record-count").getValueAsLong());

      runner.getEventRecords().clear();
      output = runner.runProduce(output.getNewOffset(), 10);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(2, runner.getEventRecords().size());
      eventRecords = runner.getEventRecords();
      newFileEvent = eventRecords.get(0);
      finishedFileEvent = eventRecords.get(1);

      Assert.assertEquals("new-file", newFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals("finished-file", finishedFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals("/" + filePath2.getFileName().toString(), newFileEvent.get("/filepath").getValueAsString());
      Assert.assertEquals("/" + filePath2.getFileName().toString(), finishedFileEvent.get("/filepath").getValueAsString());
      Assert.assertEquals(1, finishedFileEvent.get("/record-count").getValueAsLong());

      runner.getEventRecords().clear();
      output = runner.runProduce(output.getNewOffset(), 10);
      records = output.getRecords().get("lane");
      Assert.assertEquals(0, records.size());
      Assert.assertEquals(1, runner.getEventRecords().size());
      Record noMoreDataEventRecord = runner.getEventRecords().get(0);
      Assert.assertEquals("no-more-data", noMoreDataEventRecord.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals(2, noMoreDataEventRecord.get("/record-count").getValueAsLong());
      Assert.assertEquals(2, noMoreDataEventRecord.get("/file-count").getValueAsLong());

      runner.getEventRecords().clear();
      output = runner.runProduce(output.getNewOffset(), 10);
      records = output.getRecords().get("lane");
      Assert.assertEquals(0, records.size());
      Assert.assertEquals(0, runner.getEventRecords().size());

      Path filePath3 = Paths.get(path + "/testEvents3.txt");
      java.nio.file.Files.write(filePath3, sampleText, StandardOpenOption.CREATE_NEW);

      runner.getEventRecords().clear();
      output = runner.runProduce(output.getNewOffset(), 10);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(2, runner.getEventRecords().size());
      eventRecords = runner.getEventRecords();
      newFileEvent = eventRecords.get(0);
      finishedFileEvent = eventRecords.get(1);
      Assert.assertEquals("new-file", newFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals("finished-file", finishedFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals("/" + filePath3.getFileName().toString(), newFileEvent.get("/filepath").getValueAsString());
      Assert.assertEquals("/" + filePath3.getFileName().toString(), finishedFileEvent.get("/filepath").getValueAsString());
      Assert.assertEquals(1, finishedFileEvent.get("/record-count").getValueAsLong());

      runner.getEventRecords().clear();
      output = runner.runProduce(output.getNewOffset(), 10);
      records = output.getRecords().get("lane");
      Assert.assertEquals(0, records.size());
      Assert.assertEquals(1, runner.getEventRecords().size());
      //Counters are reset, so we should have seen one file after the last no more data event
      noMoreDataEventRecord = runner.getEventRecords().get(0);
      Assert.assertEquals("no-more-data", noMoreDataEventRecord.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals(1, noMoreDataEventRecord.get("/record-count").getValueAsLong());
      Assert.assertEquals(1, noMoreDataEventRecord.get("/file-count").getValueAsLong());
    } finally {
      runner.runDestroy();
    }
  }
  @Test
  public void testMockReset() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testMockReset";

    Path filePath =  Paths.get(path + "/testMockReset.txt");

    Assert.assertTrue(new File(path).mkdirs());

    java.nio.file.Files.write(
        filePath,
        ("This is sample text").getBytes(),
        StandardOpenOption.CREATE_NEW
    );

    setupSSHD(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.TEXT,
            null,
            false,
            "*"
        ));

    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      //no more files to process, this will fail.
      StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 5);
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
    } finally {
      runner.runDestroy();
    }

    runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      //no more files to process, this will fail.
      StageRunner.Output op = runner.runProduce(null, 5);
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
    } finally {
      runner.runDestroy();
    }

  }

  @Test
  public void testMultiBatchSameFile() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testMultiBatchSameFile";

    Path filePath =  Paths.get(path + "/testMultiBatchSameFile.txt");

    Assert.assertTrue(new File(path).mkdirs());

    StringBuilder sb = new StringBuilder();

    for (int i = 0 ; i < 100; i++) {
      sb.append("This is sample text" + i);
      sb.append("\n");
    }

    java.nio.file.Files.write(
        filePath,
        sb.toString().getBytes(),
        StandardOpenOption.CREATE_NEW
    );

    setupSSHD(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.TEXT,
            null,
            false,
            "*"
        ));

    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> actual;
    String lastOffset = RemoteDownloadSource.NOTHING_READ;
    try {
      do {
        //Batch size 25, records 100. (4 batches)
        StageRunner.Output op = runner.runProduce(lastOffset, 25);
        lastOffset = op.getNewOffset();
        actual = op.getRecords().get("lane");
      } while (actual.size() > 0);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFileSelection() throws Exception {

    String [] fileNames = {
        "aaa",
        "aab",
        "bbb",
        "bba",
        "aaaa"
    };

    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/fileNameSelection";
    Assert.assertTrue(new File(path).mkdirs());

    for(String f : fileNames) {
      File dest = new File(path, f);
      Files.write(someSampleData(), dest);
    }
    setupSSHD(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            false,
            "??a"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op = runner.runProduce(offset, 1000);
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }

    op = runner.runProduce(offset, 1000);
    actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      System.out.println(actual.get(i));
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }

    op = runner.runProduce(offset, 1000);
    actual = op.getRecords().get("lane");
    Assert.assertNotEquals(expected.size(), actual.size());
  }

  @Test
  public void testFileSelectionRecursive() throws Exception {

    String [] directories = {
        "fileNameSelection",
        "fileNameSelection/dir1",
        "fileNameSelection/dir2",
        "fileNameSelection/dir3"
    };

    String [] fileNames = {
        "aaa",
        "aab",
        "bbb",
        "bba",
        "aaaa"
    };

    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source";
    for(String dir : directories) {
      String myPath = path + '/' + dir;
      Assert.assertTrue(new File(myPath).mkdirs());

      for(String f : fileNames) {
        File dest = new File(myPath, f);
        Files.write(someSampleData(), dest);
      }
    }

    setupSSHD(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            "sftp://localhost:" + String.valueOf(port) + "/",
            true,
            "testuser",
            "pass",
            null,
            null,
            null,
            true,
            DataFormat.JSON,
            null,
            true,
            "??a"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op;
    List<Record> expected = getExpectedRecords();
    List<Record> actual = null;

    // 2 files per directory.  root directory and 3 child directories.
    final int MATCHING_FILES = 8;

    for(int j = 0 ; j < MATCHING_FILES; ++j) {
      op = runner.runProduce(offset, 1000);
      actual = op.getRecords().get("lane");
      Assert.assertEquals(expected.size(), actual.size());
      for (int i = 0; i < 2; i++) {
        Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
      }
    }

    // try again, but there are no more matching files.
    op = runner.runProduce(offset, 1000);
    actual = op.getRecords().get("lane");
    Assert.assertNotEquals(expected.size(), actual.size());
  }

  private byte [] someSampleData() {
    String data =
        "{\"name\": \"sloth\",\"age\": \"5\",\"characterisitics\": [\"cute\", \"slooooow\", \"sloooooow\", \"sloooooooow\"]}\n"
            + "{\"name\": \"panda\",\"age\": \"3\",\"characterisitics\": [\"cool\", \"cute\", \"round\", \"playful\", \"hungry\"]}";
    return data.getBytes();
  }

  private RemoteDownloadConfigBean getBean(
      String remoteHost,
      boolean userDirIsRoot,
      String username,
      String password,
      String privateKey,
      String passphrase,
      String knownHostsFile,
      boolean noHostChecking,
      DataFormat dataFormat,
      String errorArchive,
      boolean processSubDirectories,
      String filePattern
  ) {
    return getBean(
        remoteHost,
        userDirIsRoot,
        username,
        password,
        privateKey,
        passphrase,
        knownHostsFile,
        noHostChecking,
        dataFormat,
        errorArchive,
        processSubDirectories,
        filePattern,
        1000,
        ""
    );
  }

  private RemoteDownloadConfigBean getBean(
    String remoteHost,
    boolean userDirIsRoot,
    String username,
    String password,
    String privateKey,
    String passphrase,
    String knownHostsFile,
    boolean noHostChecking,
    DataFormat dataFormat,
    String errorArchive,
    boolean processSubDirectories,
    String filePattern,
    int batchSize
  ) {
    return getBean(
      remoteHost,
      userDirIsRoot,
      username,
      password,
      privateKey,
      passphrase,
      knownHostsFile,
      noHostChecking,
      dataFormat,
      errorArchive,
      processSubDirectories,
      filePattern,
      batchSize,
      ""
    );
  }

  private RemoteDownloadConfigBean getBean(
      String remoteHost,
      boolean userDirIsRoot,
      String username,
      String password,
      String privateKey,
      String passphrase,
      String knownHostsFile,
      boolean noHostChecking,
      DataFormat dataFormat,
      String errorArchive,
      boolean processSubDirectories,
      String filePattern,
      int batchSize,
      String initialFile
  ) {
    RemoteDownloadConfigBean configBean = new RemoteDownloadConfigBean();
    configBean.remoteAddress = remoteHost;
    configBean.userDirIsRoot = userDirIsRoot;
    configBean.username = () -> username;
    configBean.password = () -> password;
    configBean.privateKey = privateKey;
    configBean.privateKeyPassphrase = () -> passphrase;
    configBean.knownHosts = knownHostsFile;
    configBean.strictHostChecking = !noHostChecking;
    configBean.dataFormat = dataFormat;
    configBean.errorArchiveDir = errorArchive;
    configBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    configBean.processSubDirectories = processSubDirectories;
    configBean.filePattern = filePattern;
    configBean.basic.maxBatchSize = batchSize;
    configBean.initialFileToProcess = initialFile;
    if (password != null) {
      configBean.auth = Authentication.PASSWORD;
    } else {
      configBean.auth = Authentication.PRIVATE_KEY;
    }
    return configBean;
  }
  private static class PasswdAuth implements PasswordAuthenticator {

    @Override
    public boolean authenticate(String username, String password, ServerSession session)
        throws PasswordChangeRequiredException {
      return username.equals("testuser") && password.equals("pass");
    }
  }

  private static class TestPublicKeyAuth implements PublickeyAuthenticator {

    private final PublicKey key;

    TestPublicKeyAuth() throws Exception {
      File publicKeyFile =
          new File(currentThread().getContextClassLoader().
              getResource("remote-download-source/id_rsa_test.pub").getPath());
      String publicKeyBody = null;
      try (FileInputStream fs = new FileInputStream(publicKeyFile)) {
        publicKeyBody = IOUtils.toString(fs);
      }

      SshRsaCrypto rsa = new SshRsaCrypto();
      key = rsa.readPublicKey(rsa.slurpPublicKey(publicKeyBody));
    }

    @Override
    public boolean authenticate(String username, PublicKey key, ServerSession session) {
      return key.equals(this.key);
    }
  }

  private class HostKeyProvider implements KeyPairProvider {
    KeyPair keyPair = null;

    HostKeyProvider() throws Exception {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
      keyPair = keyGen.generateKeyPair();
    }

    @Override
    public KeyPair loadKey(String type) {
      Preconditions.checkArgument(type.equals("ssh-rsa"));
      return keyPair;
    }

    @Override
    public Iterable<String> getKeyTypes() {
      return Arrays.asList("ssh-rsa");
    }

    @Override
    public Iterable<KeyPair> loadKeys() {
      return Arrays.asList(keyPair);
    }
  }
}
