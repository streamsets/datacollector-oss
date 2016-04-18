/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.remote;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.io.FileUtils;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.auth.password.PasswordChangeRequiredException;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.net.ServerSocket;
import java.net.URL;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.lang.Thread.currentThread;

public class TestRemoteDownloadSource {
  private SshServer sshd;
  private int port;
  private String path;
  private String oldWorkingDir;

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

  @After
  public void resetWorkingDir() throws Exception {
    System.setProperty("user.dir", oldWorkingDir);
    sshd.close();
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
    sshd.setKeyPairProvider(new HostKeyProvider());
    sshd.start();
  }

  @Test
  public void testNoError() throws Exception {
    path = "remote-download-source/parseNoError";
    RemoteDownloadConfigBean configBean = new RemoteDownloadConfigBean();
    configBean.pollInterval = 60;
    configBean.dataFormat = DataFormat.JSON;
    configBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    setupSSHD(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(
            "sftp://localhost:" + String.valueOf(port) + "/",
            "testuser",
            "pass",
            null,
            true,
            configBean.dataFormatConfig,
            configBean.dataFormat,
            configBean.pollInterval,
            configBean.errorArchiveDir
        );
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce("-1", 1000);
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
  }

  @Test
  public void testNoErrorOrdering() throws Exception {
    path = "remote-download-source/parseSameTimestamp";
    RemoteDownloadConfigBean configBean = new RemoteDownloadConfigBean();
    configBean.pollInterval = 60;
    configBean.dataFormat = DataFormat.JSON;
    configBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
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
      } else if (f.getName().equals("sloth.txt")){
        f.setLastModified(17000000L);
      }
    }
    setupSSHD(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(
            "sftp://localhost:" + String.valueOf(port) + "/",
            "testuser",
            "pass",
            null,
            true,
            configBean.dataFormatConfig,
            configBean.dataFormat,
            configBean.pollInterval,
            configBean.errorArchiveDir
        );
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadSource.class, origin)
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
    String offset = "-1";
    for (int i = 0; i < 3; i++) {
      StageRunner.Output op = runner.runProduce(offset, 1000);
      offset = op.getNewOffset();
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
      System.out.println(actual.get(0).get());
      Assert.assertEquals(expected.get(i).get(), actual.get(0).get());
    }
  }

  @Test
  public void testRestartFromMiddleOfFile() throws Exception {
    path = "remote-download-source/parseNoError";
    RemoteDownloadConfigBean configBean = new RemoteDownloadConfigBean();
    configBean.pollInterval = 60;
    configBean.dataFormat = DataFormat.JSON;
    configBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    setupSSHD(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(
            "sftp://localhost:" + String.valueOf(port) + "/",
            "testuser",
            "pass",
            null,
            true,
            configBean.dataFormatConfig,
            configBean.dataFormat,
            configBean.pollInterval,
            ""
        );
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords();
    String offset = "-1";
    StageRunner.Output op = runner.runProduce(offset, 1);
    offset = op.getNewOffset();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    runner.runDestroy();

    // Create a new source.
    origin =
        new RemoteDownloadSource(
            "sftp://localhost:" + String.valueOf(port) + "/",
            "testuser",
            "pass",
            null,
            true,
            configBean.dataFormatConfig,
            configBean.dataFormat,
            configBean.pollInterval,
            ""
        );
    runner = new SourceRunner.Builder(RemoteDownloadSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();
    op = runner.runProduce(offset, 1);
    actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(1).get(), actual.get(0).get());
    runner.runDestroy();
  }

  @Test
  public void testRestartCompletedFile() throws Exception {
    path = "remote-download-source/parseSameTimestamp";
    RemoteDownloadConfigBean configBean = new RemoteDownloadConfigBean();
    configBean.pollInterval = 60;
    configBean.dataFormat = DataFormat.JSON;
    configBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
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
      } else if (f.getName().equals("sloth.txt")){
        Assert.assertTrue(f.setLastModified(17000000L));
      }
    }
    setupSSHD(path, false);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(
            "sftp://localhost:" + String.valueOf(port) + "/",
            "testuser",
            "pass",
            null,
            true,
            configBean.dataFormatConfig,
            configBean.dataFormat,
            configBean.pollInterval,
            ""
        );
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadSource.class, origin)
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
    String offset = "-1";
    StageRunner.Output op = runner.runProduce(offset, 1);
    offset = op.getNewOffset();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    System.out.println(actual.get(0));
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    runner.runDestroy();

    // Create a new source.
    origin =
        new RemoteDownloadSource(
            "sftp://localhost:" + String.valueOf(port) + "/",
            "testuser",
            "pass",
            null,
            true,
            configBean.dataFormatConfig,
            configBean.dataFormat,
            configBean.pollInterval,
            ""
        );
    runner = new SourceRunner.Builder(RemoteDownloadSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();
    // Since we don't proactively close steams, we must hit at least one null event in a batch to close the current
    // stream and open the next one, else the next batch will be empty and the data comes in the batch following that.
    op = runner.runProduce(offset, 1);
    runner.runProduce(offset, 1); // Forces a new stream to be opened.
    offset = op.getNewOffset();
    actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    System.out.println(actual.get(0));
    Assert.assertEquals(expected.get(1).get(), actual.get(0).get());
    op = runner.runProduce(offset, 2);
    actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(2).get(), actual.get(0).get());
    runner.runDestroy();
  }

  @Test
  public void testOverrunErrorArchiveFile() throws Exception {
    path = "remote-download-source/parseOverrun";
    RemoteDownloadConfigBean configBean = new RemoteDownloadConfigBean();
    configBean.pollInterval = 60;
    configBean.dataFormat = DataFormat.JSON;
    configBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    setupSSHD(path, false);
    File archiveDir = Files.createTempDir();
    RemoteDownloadSource origin =
        new RemoteDownloadSource(
            "sftp://localhost:" + String.valueOf(port) + "/",
            "testuser",
            "pass",
            null,
            true,
            configBean.dataFormatConfig,
            configBean.dataFormat,
            configBean.pollInterval,
            archiveDir.toString()
        );
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadSource.class, origin)
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
    StageRunner.Output op = runner.runProduce("-1", 1000);
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    System.out.println(actual.get(0).get());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    Assert.assertEquals(1, archiveDir.listFiles().length);
    File expectedFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseOverrun").getPath()).listFiles()[0];
    File actualFile = archiveDir.listFiles()[0];
    Assert.assertEquals(expectedFile.getName(), actualFile.getName());
    Assert.assertTrue(FileUtils.contentEquals(expectedFile, actualFile));
  }

  @Test
  public void testOverrunErrorArchiveFileRecovery() throws Exception {
    path = "remote-download-source/parseRecoveryFromFailure";
    RemoteDownloadConfigBean configBean = new RemoteDownloadConfigBean();
    configBean.pollInterval = 60;
    configBean.dataFormat = DataFormat.JSON;
    configBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    File dir =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseRecoveryFromFailure").getPath());
    File[] files = dir.listFiles();
    for (File f : files) {
      System.out.println(f.getName());
      if (f.getName().equals("polarbear.txt")) {
        f.setLastModified(18000000L);
      } else if (f.getName().equals("longobject.txt")) {
        f.setLastModified(17500000L);
      } else if (f.getName().equals("sloth.txt")) {
        f.setLastModified(17000000L);
      }
    }
    setupSSHD(path, false);
    File archiveDir = Files.createTempDir();
    FilenameFilter filter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.equals("longobject.txt");
      }
    };
    RemoteDownloadSource origin =
        new RemoteDownloadSource(
            "sftp://localhost:" + String.valueOf(port) + "/",
            "testuser",
            "pass",
            null,
            true,
            configBean.dataFormatConfig,
            configBean.dataFormat,
            configBean.pollInterval,
            archiveDir.toString()
        );
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadSource.class, origin)
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
    String offset = "-1";
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
  }

  @Test
  public void testOverrunError() throws Exception {
    path = "remote-download-source/parseOverrun";
    RemoteDownloadConfigBean configBean = new RemoteDownloadConfigBean();
    configBean.pollInterval = 60;
    configBean.dataFormat = DataFormat.JSON;
    configBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    setupSSHD(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(
            "sftp://localhost:" + String.valueOf(port) + "/",
            "testuser",
            "pass",
            null,
            true,
            configBean.dataFormatConfig,
            configBean.dataFormat,
            configBean.pollInterval,
            configBean.errorArchiveDir
        );
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadSource.class, origin)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.DISCARD)
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce("-1", 1000);
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
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
    File tempDir = Files.createTempDir();
    File copied = new File(tempDir, originDirFile.getName());
    Files.copy(originDirFile, copied);
    long lastModified = copied.lastModified();
    RemoteDownloadConfigBean configBean = new RemoteDownloadConfigBean();
    configBean.pollInterval = 30;
    configBean.dataFormat = DataFormat.JSON;
    configBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    setupSSHD(tempDir.toString(), true);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(
            "sftp://localhost:" + String.valueOf(port) + "/",
            "testuser",
            "pass",
            null,
            true,
            configBean.dataFormatConfig,
            configBean.dataFormat,
            configBean.pollInterval,
            configBean.errorArchiveDir
        );
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    String offset = "-1";
    StageRunner.Output op = runner.runProduce(offset, 1000);
    offset = op.getNewOffset();
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
    File eventualFile = new File(tempDir, "z" + originDirFile.getName() + "-1");
    Files.copy(originDirFile, eventualFile);
    eventualFile.setLastModified(lastModified);
    Thread.sleep(35000);
    op = runner.runProduce(offset, 1000);
    expected = getExpectedRecords();
    actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
  }

  private static class PasswdAuth implements PasswordAuthenticator {

    @Override
    public boolean authenticate(String username, String password, ServerSession session)
        throws PasswordChangeRequiredException {
      return username.equals("testuser") && password.equals("pass");
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