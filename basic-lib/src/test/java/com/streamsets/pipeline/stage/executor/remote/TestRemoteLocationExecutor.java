/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.executor.remote;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.stage.connection.remote.Protocol;
import com.streamsets.pipeline.lib.tls.KeyStoreType;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.connection.remote.Authentication;
import com.streamsets.pipeline.lib.remote.FTPAndSSHDUnitTest;
import com.streamsets.pipeline.stage.connection.remote.RemoteConnection;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@RunWith(Parameterized.class)
public class TestRemoteLocationExecutor extends FTPAndSSHDUnitTest {
   private enum Scheme {
    sftp, ftp, ftps
  }
  private static final boolean[] isDirRootorNot = new boolean[] {true, false};

  @Parameterized.Parameters(
      name = "Server Connection Type: {0}, is User Chroot Jailed : {1}"
  )
  public static Collection<Object[]> data() {
    final List<Object[]> data = new LinkedList<>();

    for (boolean isRootOrNot : isDirRootorNot) {
      for (Scheme scheme : Scheme.values()) {
        data.add(new Object[] {scheme, isRootOrNot});
      }
    }
    return data;
  }

  private final Scheme scheme;
  private final boolean userDirisRoot;
  private Target.Context context;

  private static final String SOURCE_TEXT = "This is source file data";
  private static final String SOURCE_FILENAME = "source.txt";
  private static final String EXISTING_FILE_TEXT = "This is existing file data";
  private static final String NEW_LOCATION_DIR = "/new_location";

  public TestRemoteLocationExecutor(Scheme scheme, boolean userDirisRoot) {
    this.scheme = scheme;
    this.userDirisRoot = userDirisRoot;
  }

  @BeforeClass
  public static void setUpClass() {
    DataCollectorServicesUtils.loadDefaultServices();
  }

  @Before
  public void setup() throws Exception {
    context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
  }

  /* Helper functions to create records, setup servers, verify files etc */

  private void destroyAndValidate(ExecutorRunner executor) throws Exception {
    executor.runDestroy();
    if (scheme == Scheme.sftp) {
      await().atMost(10, TimeUnit.SECONDS).untilTrue(closedAll);
    }
  }

  private Record createRecord(File file) throws Exception {
    Record record = RecordCreator.create();
    record.set(Field.create(file.getName()));
    return record;
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

  private RemoteExecutorConfigBean getBeanforMoveAction(
      String remoteHost,
      String fileNameEL,
      String newLocation,
      WholeFileExistsAction fileExistsAction
  ) {

    RemoteExecutorConfigBean configBean = new RemoteExecutorConfigBean();
    configBean.remoteConfig.connection = new RemoteConnection();

     // Connection related settings
    configBean.remoteConfig.connection.remoteAddress = remoteHost;
    configBean.remoteConfig.connection.protocol = Protocol.valueOf(scheme.name().toUpperCase(Locale.ROOT));
    configBean.remoteConfig.userDirIsRoot = userDirisRoot;
    configBean.remoteConfig.connection.credentials.username = () -> TESTUSER;
    configBean.remoteConfig.connection.credentials.auth = Authentication.PASSWORD;
    configBean.remoteConfig.connection.credentials.password = () -> TESTPASS;
    configBean.remoteConfig.connection.credentials.strictHostChecking = false;

    configBean.action.filePath = fileNameEL;
    configBean.action.actionType = PostProcessingFileAction.MOVE_FILE;
    configBean.action.targetDir = newLocation;
    configBean.action.fileExistsAction = fileExistsAction;

    return configBean;
  }

  private RemoteExecutorConfigBean getBeanforDeleteAction(String remoteHost, String fileNameEL) {
    RemoteExecutorConfigBean configBean = new RemoteExecutorConfigBean();
    configBean.remoteConfig.connection = new RemoteConnection();

    // Connection related settings
    configBean.remoteConfig.connection.remoteAddress = remoteHost;
    configBean.remoteConfig.connection.protocol = Protocol.valueOf(scheme.name().toUpperCase(Locale.ROOT));
    configBean.remoteConfig.userDirIsRoot = userDirisRoot;
    configBean.remoteConfig.connection.credentials.username = () -> TESTUSER;
    configBean.remoteConfig.connection.credentials.auth = Authentication.PASSWORD;
    configBean.remoteConfig.connection.credentials.password = () -> TESTPASS;
    configBean.remoteConfig.connection.credentials.strictHostChecking = false;

    configBean.action.filePath = fileNameEL;
    configBean.action.actionType = PostProcessingFileAction.DELETE_FILE;

    return configBean;
  }

  @Test
  public void testDeleteFileAction() throws Exception {
     path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

     File sourceFile = testFolder.newFile(SOURCE_FILENAME);
     FileUtils.writeStringToFile(sourceFile, SOURCE_TEXT);

    String root = userDirisRoot ? "/" : testFolder.getRoot().getAbsolutePath();
    String remoteHost = scheme.name() + "://localhost:" + port + root;
    RemoteLocationExecutor remoteLocationExecutor = new RemoteLocationExecutor(getBeanforDeleteAction(
        remoteHost,
        sourceFile.getName()
    ));

    ExecutorRunner executor = new ExecutorRunner.Builder(RemoteLocationDExecutor.class, remoteLocationExecutor).build();
    executor.runInit();
    executor.runWrite(Collections.singletonList(createRecord(sourceFile)));

    Assert.assertEquals(0, executor.getErrorRecords().size());
    Assert.assertFalse(sourceFile.exists());

    destroyAndValidate(executor);
  }

  @Test
  public void testMoveFileActionTargetFileDoesntExist() throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    File sourceFile = testFolder.newFile(SOURCE_FILENAME);
    FileUtils.writeStringToFile(sourceFile, SOURCE_TEXT);

    String root = userDirisRoot ? "/" : testFolder.getRoot().getAbsolutePath();
    String remoteHost = scheme.name() + "://localhost:" + port + root;
    RemoteLocationExecutor remoteLocationExecutor = new RemoteLocationExecutor(getBeanforMoveAction(
        remoteHost,
        sourceFile.getName(),
        NEW_LOCATION_DIR,
        WholeFileExistsAction.TO_ERROR
    ));

    ExecutorRunner executor = new ExecutorRunner.Builder(RemoteLocationDExecutor.class, remoteLocationExecutor).build();
    executor.runInit();
    executor.runWrite(Collections.singletonList(createRecord(sourceFile)));

    Assert.assertEquals(0, executor.getErrorRecords().size());
    Assert.assertFalse(sourceFile.exists());

    File targetFile = new File(path + NEW_LOCATION_DIR + "/" + SOURCE_FILENAME);
    Assert.assertTrue(targetFile.exists());
    Assert.assertEquals(FileUtils.readFileToString(targetFile), SOURCE_TEXT);

    destroyAndValidate(executor);
  }

  @Test
  public void testMoveFileActionTargetFileExistsActionOverwrite() throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    File sourceFile = testFolder.newFile(SOURCE_FILENAME);
    FileUtils.writeStringToFile(sourceFile, SOURCE_TEXT);

    File targetFolder = testFolder.newFolder(NEW_LOCATION_DIR.substring(1));
    File targetFile = new File(targetFolder.getAbsolutePath(),  "/" + SOURCE_FILENAME);
    FileUtils.writeStringToFile(targetFile, EXISTING_FILE_TEXT);

    String root = userDirisRoot ? "/" : testFolder.getRoot().getAbsolutePath();
    String remoteHost = scheme.name() + "://localhost:" + port + root;
    RemoteLocationExecutor remoteLocationExecutor = new RemoteLocationExecutor(getBeanforMoveAction(
        remoteHost,
        sourceFile.getName(),
        NEW_LOCATION_DIR,
        WholeFileExistsAction.OVERWRITE
    ));

    ExecutorRunner executor = new ExecutorRunner.Builder(RemoteLocationDExecutor.class, remoteLocationExecutor).build();
    executor.runInit();
    executor.runWrite(Collections.singletonList(createRecord(sourceFile)));

    Assert.assertEquals(0, executor.getErrorRecords().size());
    Assert.assertFalse(sourceFile.exists());

    Assert.assertTrue(targetFile.exists());
    Assert.assertEquals(FileUtils.readFileToString(targetFile), SOURCE_TEXT);

    destroyAndValidate(executor);
  }

  @Test
  public void testMoveFileActionTargetFileExistsActionToErrorDontStopPipeline() throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    File sourceFile = testFolder.newFile(SOURCE_FILENAME);
    FileUtils.writeStringToFile(sourceFile, SOURCE_TEXT);

    File targetFolder = testFolder.newFolder(NEW_LOCATION_DIR.substring(1));
    File targetFile = new File(targetFolder.getAbsolutePath(),  "/" + SOURCE_FILENAME);
    FileUtils.writeStringToFile(targetFile, EXISTING_FILE_TEXT);

    String root = userDirisRoot ? "/" : testFolder.getRoot().getAbsolutePath();
    String remoteHost = scheme.name() + "://localhost:" + port + root;
    RemoteLocationExecutor remoteLocationExecutor = new RemoteLocationExecutor(getBeanforMoveAction(
        remoteHost,
        sourceFile.getName(),
        NEW_LOCATION_DIR,
        WholeFileExistsAction.TO_ERROR
    ));

    ExecutorRunner executor = new ExecutorRunner.Builder(RemoteLocationDExecutor.class, remoteLocationExecutor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    executor.runInit();
    executor.runWrite(Collections.singletonList(createRecord(sourceFile)));

    Assert.assertEquals(1, executor.getErrorRecords().size());
    Assert.assertEquals(
        Errors.REMOTE_LOCATION_EXECUTOR_04.getCode(),
        executor.getErrorRecords().get(0).getHeader().getErrorCode());
    Assert.assertTrue(sourceFile.exists());

    Assert.assertTrue(targetFile.exists());
    Assert.assertEquals(FileUtils.readFileToString(targetFile), EXISTING_FILE_TEXT);

    destroyAndValidate(executor);
  }

  @Test
  public void testMoveFileActionTargetFileExistsActionToErrorStopPipeline() throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    setupServer(path, true);

    File sourceFile = testFolder.newFile(SOURCE_FILENAME);
    FileUtils.writeStringToFile(sourceFile, SOURCE_TEXT);

    File targetFolder = testFolder.newFolder(NEW_LOCATION_DIR.substring(1));
    File targetFile = new File(targetFolder.getAbsolutePath(),  "/" + SOURCE_FILENAME);
    FileUtils.writeStringToFile(targetFile, EXISTING_FILE_TEXT);

    String root = userDirisRoot ? "/" : testFolder.getRoot().getAbsolutePath();
    String remoteHost = scheme.name() + "://localhost:" + port + root;
    RemoteLocationExecutor remoteLocationExecutor = new RemoteLocationExecutor(getBeanforMoveAction(
        remoteHost,
        sourceFile.getName(),
        NEW_LOCATION_DIR,
        WholeFileExistsAction.TO_ERROR
    ));

    ExecutorRunner executor = new ExecutorRunner.Builder(RemoteLocationDExecutor.class, remoteLocationExecutor)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    executor.runInit();
    runWriteWithException(executor,
        Collections.singletonList(createRecord(sourceFile)),
        Errors.REMOTE_LOCATION_EXECUTOR_04);

    Assert.assertTrue(sourceFile.exists());
    Assert.assertTrue(targetFile.exists());
    Assert.assertEquals(FileUtils.readFileToString(targetFile), EXISTING_FILE_TEXT);

    destroyAndValidate(executor);
  }

  private void runWriteWithException(ExecutorRunner executor, List<Record> records, ErrorCode... expected) {
    try {
      executor.runWrite(records);
      Assert.fail("Expected a StageException");
    } catch (StageException e) {
      assertExceptionMessageContainsOnlyRemoteError(e, expected);
    }
  }

  private void assertExceptionMessageContainsOnlyRemoteError(Exception e, ErrorCode... expected) {
    String msg = e.getMessage();
    for (ErrorCode error : expected) {
      Assert.assertTrue("Expected exception to contain " + error.getCode() + " but did not: " + msg,
          msg.contains(error.getCode()));
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
}
