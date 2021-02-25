/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.remote;

import com.google.common.io.Files;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;
import com.streamsets.pipeline.lib.io.fileref.FileRefTestUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.stage.connection.remote.Protocol;
import com.streamsets.pipeline.lib.tls.KeyStoreType;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.connection.remote.Authentication;
import com.streamsets.pipeline.lib.remote.FTPAndSSHDUnitTest;
import com.streamsets.pipeline.stage.connection.remote.RemoteConnection;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.sshd.common.FactoryManager;
import org.apache.sshd.common.PropertyResolverUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@RunWith(Parameterized.class)
public class TestRemoteUploadTarget extends FTPAndSSHDUnitTest {

  private enum Scheme {
    sftp, ftp, ftps
  }

  @Parameterized.Parameters(name = "{0}")
  public static Object[] data() {
    return Scheme.values();
  }

  private Scheme scheme;
  private Target.Context context;

  public TestRemoteUploadTarget(Scheme scheme) {
    this.scheme = scheme;
  }

  @BeforeClass
  public static void setUpClass() {
    DataCollectorServicesUtils.loadDefaultServices();
  }

  @Before
  public void setup() throws Exception {
    context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
  }

  @Test
  public void testNoError() throws Exception {
    FileRefTestUtil.writePredefinedTextToFile(testFolder.getRoot());
    Record record = createRecord();

    File targetFile = new File(testFolder.getRoot(), "target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    RemoteUploadTarget target = new RemoteUploadTarget(getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.WHOLE_FILE,
        targetFile.getName(),
        WholeFileExistsAction.TO_ERROR
    ));
    TargetRunner runner = new TargetRunner.Builder(RemoteUploadDTarget.class, target).build();
    runner.runInit();
    runner.runWrite(Collections.singletonList(record));

    Assert.assertEquals(0, runner.getErrorRecords().size());
    verifyTargetFile(targetFile);
    destroyAndValidate(runner);
  }

  @Test
  public void testNotWholeFile() throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    RemoteUploadTarget target = new RemoteUploadTarget(getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.JSON,
        "target.txt",
        WholeFileExistsAction.TO_ERROR
    ));
    TargetRunner runner = new TargetRunner.Builder(RemoteUploadDTarget.class, target).build();
    runInitWithConfigException(runner, Errors.REMOTE_UPLOAD_04);
    destroyAndValidate(runner);
  }

  @Test
  public void testConnectionRetry() throws Exception {
    FileRefTestUtil.writePredefinedTextToFile(testFolder.getRoot());
    Record record = createRecord();

    File targetFile = new File(testFolder.getRoot(), "target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    RemoteUploadTarget target = new RemoteUploadTarget(getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.WHOLE_FILE,
        targetFile.getName(),
        WholeFileExistsAction.TO_ERROR
    ));
    TargetRunner runner = new TargetRunner.Builder(RemoteUploadDTarget.class, target).build();
    // No connections at first
    if (scheme == Scheme.sftp) {
      Assert.assertEquals(0, opened.get());
      Assert.assertEquals(0, closed.get());
    } else if (scheme == Scheme.ftp) {
      Assert.assertEquals(0, ftpServer.getServerContext().getFtpStatistics().getCurrentConnectionNumber());
    }
    runner.runInit();
    // Now we've made one connection
    if (scheme == Scheme.sftp) {
      Assert.assertEquals(1, opened.get());
    } else if (scheme == Scheme.ftp) {
      Assert.assertEquals(1, ftpServer.getServerContext().getFtpStatistics().getCurrentConnectionNumber());
    }
    // Set timeout after being idle to be really quick (1ms)
    if (scheme == Scheme.sftp) {
      PropertyResolverUtils.updateProperty(sshd, FactoryManager.IDLE_TIMEOUT, 1);
    } else if (scheme == Scheme.ftp) {
      ftpServer.getServerContext().getListeners().get("default").getActiveSessions().iterator().next().setMaxIdleTime(1);
    }
    // Wait until that one connection has been closed
    if (scheme == Scheme.sftp) {
      await().atMost(10, TimeUnit.SECONDS).until(() -> Assert.assertEquals(1, closed.get()));
      Assert.assertEquals(1, closed.get());
    } else if (scheme == Scheme.ftp) {
      await().atMost(10, TimeUnit.SECONDS).until(
          () -> Assert.assertEquals(0, ftpServer.getServerContext().getFtpStatistics().getCurrentConnectionNumber()));
      Assert.assertEquals(0, ftpServer.getServerContext().getFtpStatistics().getCurrentConnectionNumber());
    }
    if (scheme == Scheme.sftp) {
      // Unset the timeout config for SFTP because it's global
      PropertyResolverUtils.updateProperty(sshd, FactoryManager.IDLE_TIMEOUT, FactoryManager.DEFAULT_IDLE_TIMEOUT);
    }
    runner.runWrite(Collections.singletonList(record));
    // Now we've opened a new connection
    if (scheme == Scheme.sftp) {
      Assert.assertEquals(2, opened.get());
    } else if (scheme == Scheme.ftp) {
      Assert.assertEquals(1, ftpServer.getServerContext().getFtpStatistics().getCurrentConnectionNumber());
      Assert.assertEquals(2, ftpServer.getServerContext().getFtpStatistics().getTotalConnectionNumber());
    }
    verifyTargetFile(targetFile);
    destroyAndValidate(runner);
  }

  @Test
  public void testPathInUriUserDirIsRoot() throws Exception {
    testPathInUri(true);
  }

  @Test
  public void testPathInUri() throws Exception {
    testPathInUri(false);
  }

  private void testPathInUri(boolean userDirIsRoot) throws Exception {
    FileRefTestUtil.writePredefinedTextToFile(testFolder.getRoot());
    Record record = createRecord();

    File targetDir = testFolder.newFolder();
    File targetFile = new File(targetDir, "target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    String pathInUri = userDirIsRoot ? targetDir.getName() : targetDir.getAbsolutePath();
    RemoteUploadTarget target = new RemoteUploadTarget(getBean(
        scheme.name() + "://localhost:" + port + "/" + pathInUri,
        userDirIsRoot,
        DataFormat.WHOLE_FILE,
        targetFile.getName(),
        WholeFileExistsAction.TO_ERROR
    ));
    TargetRunner runner = new TargetRunner.Builder(RemoteUploadDTarget.class, target).build();
    runner.runInit();
    runner.runWrite(Collections.singletonList(record));

    verifyTargetFile(targetFile);
    destroyAndValidate(runner);
  }

  @Test
  public void testEmptyRecord() throws Exception {
    FileRefTestUtil.writePredefinedTextToFile(testFolder.getRoot());

    File targetFile = new File(testFolder.getRoot(), "target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    RemoteUploadTarget target = new RemoteUploadTarget(getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.WHOLE_FILE,
        targetFile.getName(),
        WholeFileExistsAction.TO_ERROR
    ));
    TargetRunner runner = new TargetRunner.Builder(RemoteUploadDTarget.class, target).build();
    runner.runInit();
    runner.runWrite(Collections.emptyList());
    Assert.assertEquals(0, runner.getErrorRecords().size());
    Assert.assertEquals(0, runner.getEventRecords().size());

    Assert.assertFalse(targetFile.exists());
    destroyAndValidate(runner);
  }

  @Test
  public void testSourceFileNotExists() throws Exception {
    FileRefTestUtil.writePredefinedTextToFile(testFolder.getRoot());
    Record record = createRecord();

    File sourceFile = new File(FileRefTestUtil.getSourceFilePath(testFolder.getRoot()));
    sourceFile.delete();
    Assert.assertFalse(sourceFile.exists());

    File targetFile = new File(testFolder.getRoot(), "target.txt");
    File tempTargetFile = new File(testFolder.getRoot(), "_tmp_target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    RemoteUploadTarget target = new RemoteUploadTarget(getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.WHOLE_FILE,
        targetFile.getName(),
        WholeFileExistsAction.TO_ERROR
    ));
    TargetRunner runner = new TargetRunner.Builder(RemoteUploadDTarget.class, target).build();
    runner.runInit();
    runner.runWrite(Collections.singletonList(record));

    Assert.assertFalse(targetFile.exists());
    Assert.assertTrue(tempTargetFile.exists());
    Assert.assertEquals(0, tempTargetFile.length());
    destroyAndValidate(runner);
  }

  @Test
  public void testFileExistsOverwrite() throws Exception {
    FileRefTestUtil.writePredefinedTextToFile(testFolder.getRoot());
    Record record = createRecord();

    final String OTHER_TEXT = "some other text";
    File targetFile = testFolder.newFile("target.txt");
    Files.write(OTHER_TEXT.getBytes(Charset.forName("UTF-8")), targetFile);
    Assert.assertEquals(OTHER_TEXT, Files.readFirstLine(targetFile, Charset.forName("UTF-8")));

    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    RemoteUploadTarget target = new RemoteUploadTarget(getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.WHOLE_FILE,
        targetFile.getName(),
        WholeFileExistsAction.OVERWRITE
    ));
    TargetRunner runner = new TargetRunner.Builder(RemoteUploadDTarget.class, target).build();
    runner.runInit();
    runner.runWrite(Collections.singletonList(record));

    Assert.assertEquals(0, runner.getErrorRecords().size());
    verifyTargetFile(targetFile);
    destroyAndValidate(runner);
  }

  @Test
  public void testFileExistsToError() throws Exception {
    FileRefTestUtil.writePredefinedTextToFile(testFolder.getRoot());
    Record record = createRecord();

    final String OTHER_TEXT = "some other text";
    File targetFile = testFolder.newFile("target.txt");
    Files.write(OTHER_TEXT.getBytes(Charset.forName("UTF-8")), targetFile);
    Assert.assertEquals(OTHER_TEXT, Files.readFirstLine(targetFile, Charset.forName("UTF-8")));

    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    RemoteUploadTarget target = new RemoteUploadTarget(getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.WHOLE_FILE,
        targetFile.getName(),
        WholeFileExistsAction.TO_ERROR
    ));
    TargetRunner runner = new TargetRunner.Builder(RemoteUploadDTarget.class, target).build();
    runner.runInit();
    runner.runWrite(Collections.singletonList(record));

    List<Record> errorRecords = runner.getErrorRecords();
    Assert.assertEquals(1, errorRecords.size());
    Assert.assertEquals(Errors.REMOTE_UPLOAD_02.getCode(), errorRecords.get(0).getHeader().getErrorCode());
    Assert.assertEquals(record.get().getValueAsMap(), errorRecords.get(0).get().getValueAsMap());

    Assert.assertEquals(0, runner.getEventRecords().size());

    String line = Files.readFirstLine(targetFile, Charset.forName("UTF-8"));
    Assert.assertEquals(OTHER_TEXT, line);

    destroyAndValidate(runner);
  }

  @Test
  public void testFileNameEL() throws Exception {
    FileRefTestUtil.writePredefinedTextToFile(testFolder.getRoot());
    File sourceFile1 = new File(FileRefTestUtil.getSourceFilePath(testFolder.getRoot()));
    File sourceFile2 = new File(testFolder.getRoot(), "source2.txt");
    Files.copy(sourceFile1, sourceFile2);
    Record record1 = createRecord();
    Record record2 = createRecord(sourceFile2);

    File targetFile1 = new File(testFolder.getRoot(), "source.txt-target.txt");
    File targetFile2 = new File(testFolder.getRoot(), "source2.txt-target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    RemoteUploadTarget target = new RemoteUploadTarget(getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.WHOLE_FILE,
        "${record:value('/fileInfo/filename')}-target.txt",
        WholeFileExistsAction.TO_ERROR
    ));
    TargetRunner runner = new TargetRunner.Builder(RemoteUploadDTarget.class, target).build();
    runner.runInit();
    runner.runWrite(Arrays.asList(record1, record2));

    verifyTargetFile(targetFile1);
    verifyTargetFile(targetFile2);
    destroyAndValidate(runner);
  }

  @Test
  public void testSecondBatch() throws Exception {
    FileRefTestUtil.writePredefinedTextToFile(testFolder.getRoot());
    File sourceFile1 = new File(FileRefTestUtil.getSourceFilePath(testFolder.getRoot()));
    File sourceFile2 = new File(testFolder.getRoot(), "source2.txt");
    Files.copy(sourceFile1, sourceFile2);
    Record record1 = createRecord();
    Record record2 = createRecord(sourceFile2);

    File targetFile1 = new File(testFolder.getRoot(), "source.txt-target.txt");
    File targetFile2 = new File(testFolder.getRoot(), "source2.txt-target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    RemoteUploadTarget target = new RemoteUploadTarget(getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.WHOLE_FILE,
        "${record:value('/fileInfo/filename')}-target.txt",
        WholeFileExistsAction.TO_ERROR
    ));
    TargetRunner runner = new TargetRunner.Builder(RemoteUploadDTarget.class, target).build();
    runner.runInit();
    runner.runWrite(Arrays.asList(record1));

    Assert.assertEquals(1, runner.getEventRecords().size());
    verifyTargetFile(targetFile1);

    Files.copy(sourceFile1, sourceFile2);
    runner.runWrite(Arrays.asList(record2));

    Assert.assertEquals(2, runner.getEventRecords().size());
    verifyTargetFile(targetFile2);
    destroyAndValidate(runner);
  }

  @Test
  public void testEventsUserDirIsRoot() throws Exception {
    testEvents(true);
  }

  @Test
  public void testEvents() throws Exception {
    testEvents(false);
  }

  private void testEvents(boolean userDirIsRoot) throws Exception {
    FileRefTestUtil.writePredefinedTextToFile(testFolder.getRoot());
    Record record = createRecord();

    File targetDir = testFolder.newFolder();
    File targetFile = new File(targetDir, "target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    String pathInUri = userDirIsRoot ? "" : path;
    RemoteUploadTarget target = new RemoteUploadTarget(getBean(
        scheme.name() + "://localhost:" + port + "/" + pathInUri,
        userDirIsRoot,
        DataFormat.WHOLE_FILE,
        targetDir.getName() + "/" + targetFile.getName(),
        WholeFileExistsAction.TO_ERROR
    ));
    TargetRunner runner = new TargetRunner.Builder(RemoteUploadDTarget.class, target).build();
    runner.runInit();
    runner.runWrite(Collections.singletonList(record));
    verifyTargetFile(targetFile);

    List<EventRecord> eventRecords = runner.getEventRecords();
    Assert.assertEquals(1, eventRecords.size());
    Record completedEvent = eventRecords.get(0);

    String type = completedEvent.getHeader().getAttribute("sdc.event.type");
    Assert.assertEquals(WholeFileProcessedEvent.WHOLE_FILE_WRITE_FINISH_EVENT, type);

    Assert.assertTrue(completedEvent.has(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO_PATH));
    Assert.assertTrue(completedEvent.has(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO_PATH));
    Assert.assertEquals(2, completedEvent.get().getValueAsMap().size());

    Assert.assertEquals(
        record.get(FileRefUtil.FILE_INFO_FIELD_PATH).getValueAsMap(),
        completedEvent.get(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO_PATH).getValueAsMap());

    Map<String, Field> attributes = completedEvent.get(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO_PATH).getValueAsMap();
    Assert.assertTrue(attributes.containsKey("path"));
    Assert.assertEquals(1, attributes.size());
    Assert.assertEquals(
        "/" + targetDir.getName() + "/" + targetFile.getName(),
        attributes.get("path").getValueAsString()
    );

    destroyAndValidate(runner);
  }

  private void verifyTargetFile(File targetFile) throws IOException {
    String line = Files.readFirstLine(targetFile, Charset.forName("UTF-8"));
    Assert.assertEquals(FileRefTestUtil.TEXT, line);
  }

  private void destroyAndValidate(TargetRunner runner) throws Exception {
    runner.runDestroy();
    if (scheme == Scheme.sftp) {
      await().atMost(10, TimeUnit.SECONDS).untilTrue(closedAll);
    }
  }

  private Record createRecord() throws Exception {
    return createRecord(new File(FileRefTestUtil.getSourceFilePath(testFolder.getRoot())));
  }

  private Record createRecord(File file) throws Exception {
    Record record = context.createRecord("id");
    Map<String, Object> metadata = FileRefTestUtil.getFileMetadataWithCustomFile(file);
    FileRef fileRef = FileRefTestUtil.getLocalFileRefWithCustomFile(file, true, null, null);
    record.set(FileRefUtil.getWholeFileRecordRootField(fileRef, metadata));
    return record;
  }

  private RemoteUploadConfigBean getBean(
      String remoteHost,
      boolean userDirIsRoot,
      DataFormat dataFormat,
      String fileNameEL,
      WholeFileExistsAction wholeFileExistsAction
  ) {
    RemoteUploadConfigBean configBean = new RemoteUploadConfigBean();
    configBean.remoteConfig.connection = new RemoteConnection();
    configBean.remoteConfig.connection.remoteAddress = remoteHost;
    configBean.remoteConfig.connection.protocol = Protocol.valueOf(scheme.name().toUpperCase(Locale.ROOT));
    configBean.remoteConfig.userDirIsRoot = userDirIsRoot;
    configBean.remoteConfig.connection.credentials.username = () -> TESTUSER;
    configBean.remoteConfig.connection.credentials.auth = Authentication.PASSWORD;
    configBean.remoteConfig.connection.credentials.password = () -> TESTPASS;
    configBean.remoteConfig.connection.credentials.strictHostChecking = false;
    configBean.dataFormat = dataFormat;
    configBean.dataFormatConfig.fileNameEL = fileNameEL;
    configBean.dataFormatConfig.wholeFileExistsAction = wholeFileExistsAction;
    return configBean;
  }

  private StageException runInitWithConfigException(TargetRunner runner, ErrorCode... expected) {
    try {
      runner.runInit();
      Assert.fail("Expected a StageException");
    } catch (StageException e) {
      assertExceptionMessageContainsOnlyRemoteError(e, expected);
      return e;
    }
    return null;
  }

  private void assertExceptionMessageContainsOnlyRemoteError(Exception e, ErrorCode... expected) {
    String msg = e.getMessage();
    for (ErrorCode errror : expected) {
      Assert.assertTrue("Expected exception to contain " + errror.getCode() + " but did not: " + msg,
          msg.contains(errror.getCode()));
    }
    List<String> foundErrors = new ArrayList<>();
    for (Errors error : Errors.values()) {
      if (!ArrayUtils.contains(expected, error) && msg.contains(error.getCode())) {
        foundErrors.add(error.getCode());
      }
    }
    if (!foundErrors.isEmpty()) {
      Assert.fail("Expected exception NOT to contain " + Arrays.toString(foundErrors.toArray()) + " but it did: "
          + msg);
    }
  }

  private void setupServer(String homeDir, boolean absolutePath) throws Exception {
    if (!absolutePath) {
      URL url = Thread.currentThread().getContextClassLoader().getResource(homeDir);
      homeDir = url.getPath();
    }
    switch (scheme) {
      case sftp:
        setupSSHD(homeDir);
        break;
      case ftp:
        setupFTPServer(homeDir);
        break;
      case ftps:
        setupFTPSServer(homeDir, KeyStoreType.JKS, null, false);
        break;
      default:
        Assert.fail("Missing Server setup for scheme " + scheme);
    }
  }
}
