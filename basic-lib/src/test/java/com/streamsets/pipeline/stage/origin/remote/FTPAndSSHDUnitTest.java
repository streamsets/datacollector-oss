/*
 * Copyright 2018 StreamSets Inc.
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPCmd;
import org.junit.After;
import org.junit.Before;
import org.mockftpserver.core.command.Command;
import org.mockftpserver.core.command.InvocationRecord;
import org.mockftpserver.core.command.StaticReplyCommandHandler;
import org.mockftpserver.core.session.Session;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystemEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.mockftpserver.stub.command.AbstractStubCommandHandler;
import org.mockftpserver.stub.command.StatCommandHandler;

import java.io.File;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public abstract class FTPAndSSHDUnitTest extends SSHDUnitTest {

  private final static SimpleDateFormat MDTM_DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
  static {
    MDTM_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  protected SessionTrackingFakeFtpServer fakeFtpServer;

  @Before
  public void before() {
    super.before();
  }

  @After
  public void after() throws Exception {
    if (fakeFtpServer != null && fakeFtpServer.isStarted()) {
      fakeFtpServer.stop();
    }
    super.after();
  }

  // Allows us access to the sessions in the FTP Server that are otherwise hidden
  static class SessionTrackingFakeFtpServer extends FakeFtpServer {
    private List<Session> sessions = new ArrayList<>();

    @Override
    protected Session createSession(Socket clientSocket) {
      Session session = super.createSession(clientSocket);
      sessions.add(session);
      return session;
    }

    public List<Session> getSessions() {
      return sessions;
    }
  }

  protected void setupFTPServer(String homeDir) throws Exception {
    fakeFtpServer = new SessionTrackingFakeFtpServer();
    fakeFtpServer.setServerControlPort(0);
    fakeFtpServer.setSystemName(FTPClientConfig.SYST_UNIX);
    fakeFtpServer.setFileSystem(new UnixFakeFileSystem());
    UserAccount userAccount = new UserAccount(TESTUSER, TESTPASS, homeDir);
    fakeFtpServer.addUserAccount(userAccount);
    fakeFtpServer.start();
    port = fakeFtpServer.getServerControlPort();
    populateFakeFileSystemFromReal(new File(homeDir));

    // Add the missing FEAT and MDTM commands
    fakeFtpServer.setCommandHandler(FTPCmd.FEAT.getCommand(), new StaticReplyCommandHandler(211, "MDTM"));
    fakeFtpServer.setCommandHandler(FTPCmd.MDTM.getCommand(), new AbstractStubCommandHandler() {
      @Override
      protected void handleCommand(Command command, Session session, InvocationRecord invocationRecord) {
        String pathname = command.getOptionalString(0);
        if (!pathname.startsWith("/")) {
          pathname = homeDir + "/" + pathname;
        }
        invocationRecord.set(StatCommandHandler.PATHNAME_KEY, pathname);

        FileSystemEntry file = fakeFtpServer.getFileSystem().getEntry(pathname);
        if (file == null) {
          sendReply(session, 550, null, "No such file or directory.", null);
        } else {
          String time = MDTM_DATE_FORMAT.format(file.getLastModified());
          sendReply(session, 213, null, time, null);
        }
      }
    });
  }

  private void populateFakeFileSystemFromReal(File absFile) throws Exception {
    for (File f : absFile.listFiles()) {
      if (f.isFile()) {
        addFileToFakeFileSystem(f.getAbsolutePath(), FileUtils.readFileToByteArray(f), f.lastModified());
      } else if (f.isDirectory()) {
        DirectoryEntry entry = new DirectoryEntry(f.getAbsolutePath());
        fakeFtpServer.getFileSystem().add(entry);
        populateFakeFileSystemFromReal(f);
      }
    }
  }

  protected void addFileToFakeFileSystem(String path, byte[] contents, long lastModified) {
    FileEntry entry = new FileEntry(path);
    entry.setContents(contents);
    entry.setLastModified(new Date(lastModified));
    fakeFtpServer.getFileSystem().add(entry);
  }
}
