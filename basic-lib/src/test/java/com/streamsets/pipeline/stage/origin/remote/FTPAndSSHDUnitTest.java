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

import org.apache.commons.net.ftp.FTPCmd;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.command.CommandFactoryFactory;
import org.apache.ftpserver.command.impl.FEAT;
import org.apache.ftpserver.filesystem.nativefs.NativeFileSystemFactory;
import org.apache.ftpserver.ftplet.DefaultFtpReply;
import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.impl.DefaultFtpServer;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

public abstract class FTPAndSSHDUnitTest extends SSHDUnitTest {

  protected DefaultFtpServer ftpServer;
  protected boolean supportMDTM = true;

  @Before
  public void before() {
    super.before();
  }

  @After
  public void after() throws Exception {
    if (ftpServer != null && !ftpServer.isStopped()) {
      ftpServer.stop();
    }
    super.after();
  }

  protected void setupFTPServer(String homeDir) throws Exception {
    FtpServerFactory serverFactory = new FtpServerFactory();
    ListenerFactory factory = new ListenerFactory();
    factory.setPort(0);
    serverFactory.addListener("default", factory.createListener());

    PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
    UserManager um = userManagerFactory.createUserManager();
    BaseUser user = new BaseUser();
    user.setName(TESTUSER);
    user.setPassword(TESTPASS);
    user.setHomeDirectory("/");
    user.setAuthorities(Collections.singletonList(new WritePermission()));
    um.save(user);
    serverFactory.setUserManager(um);

    // For whatever reason, they hardcode the root of the filesystem to be the user's home dir.  This prevents us from
    // testing scenarios where userDirIsRoot is false because we can't get out of the user dir.  As a workaround, we set
    // the user's home dir to root (see above) and then modify the FileSystemView to change the working directory to the
    // user's home dir instead - this properly emulates the expected behavior.
    FileSystemFactory fsf = new NativeFileSystemFactory() {
      @Override
      public FileSystemView createFileSystemView(User user) throws FtpException {
        FileSystemView view = super.createFileSystemView(user);
        view.changeWorkingDirectory(homeDir);
        return view;
      }
    };
    serverFactory.setFileSystem(fsf);

    CommandFactoryFactory cff = new CommandFactoryFactory();
    // Allow pretending to not support MDTM by overriding the FEAT command to return an empty String (i.e. supports no
    // extra features) if supportMDTM is false
    cff.addCommand(FTPCmd.FEAT.getCommand(), new FEAT() {
      @Override
      public void execute(
          FtpIoSession session, FtpServerContext context, FtpRequest request
      ) throws IOException, FtpException {
        if (supportMDTM) {
          super.execute(session, context, request);
        } else {
          session.resetState();
          session.write(new DefaultFtpReply(211, ""));
        }
      }
    });
    serverFactory.setCommandFactory(cff.createCommandFactory());

    ftpServer = (DefaultFtpServer) serverFactory.createServer();
    ftpServer.start();
    port = ftpServer.getListener("default").getPort();
  }
}
