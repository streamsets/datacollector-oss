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

import com.google.common.io.Files;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.sftp.SFTPException;
import net.schmizz.sshj.transport.TransportException;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

public class TestChrootSFTPClient extends SSHDUnitTest {

  @Test
  public void testRootNotExist() throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    Assert.assertFalse(new File(path + "/blah").exists());

    try {
      // absolute path
      new ChrootSFTPClient(sshClient.newSFTPClient(), path + "/blah", false);
      Assert.fail("Expected an SFTPException");
    } catch (SFTPException e) {
      Assert.assertEquals(path + "/blah" + ": does not exist", e.getMessage());
    }

    try {
      // relative path
      new ChrootSFTPClient(sshClient.newSFTPClient(), "/blah", true);
      Assert.fail("Expected an SFTPException");
    } catch (SFTPException e) {
      Assert.assertEquals(path + "/blah" + ": does not exist", e.getMessage());
    }

    try {
      // relative path
      new ChrootSFTPClient(sshClient.newSFTPClient(), "blah", true);
      Assert.fail("Expected an SFTPException");
    } catch (SFTPException e) {
      Assert.assertEquals(path + "/blah" + ": does not exist", e.getMessage());
    }
  }

  @Test
  public void testSetSFTPClient() throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();
    ChrootSFTPClient sftpClient = new ChrootSFTPClient(sshClient.newSFTPClient(), path, false);

    sftpClient.ls();
    // Close the SSH client so it's no longer usable and the SFTP client will get an exception
    sshClient.close();
    try {
      sftpClient.ls();
      Assert.fail("Expected a TransportException");
    } catch (TransportException se) {
      Assert.assertEquals("Socket closed", se.getMessage());
    }
    // Set a new SSH client and try again
    sftpClient.setSFTPClient(createSSHClient().newSFTPClient());
    sftpClient.ls();
  }

  @Test
  public void testLs() throws Exception {
    File fileInRoot = testFolder.newFile("fileInRoot.txt");
    File dirInRoot = testFolder.newFolder("dirInRoot");
    File fileInDirInRoot = new File(dirInRoot, "fileInDirInRoot.txt");
    Files.touch(fileInDirInRoot);

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    /**
     * ...
     * ├── path (root)
     * │   ├─── fileInRoot.txt
     * │   ├─── dirInRoot
     * │   │    └── fileInDirInRoot.txt
     */

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      List<ChrootSFTPClient.SimplifiedRemoteResourceInfo> files = sftpClient.ls();
      Assert.assertEquals(2, files.size());
      for (ChrootSFTPClient.SimplifiedRemoteResourceInfo file : files) {
        if (file.getType() == FileMode.Type.REGULAR) {
          Assert.assertEquals("/" + fileInRoot.getName(), file.getPath());
        } else {  // dir
          Assert.assertEquals("/" + dirInRoot.getName(), file.getPath());
        }
      }
      // We can specify a dir as either a relative path "dirInRoot" or an absolute path "/dirInRoot" and they should be
      // equivalent
      for (String p : new String[] {
          dirInRoot.getName(),
          "/" + dirInRoot.getName(),
      })
      files = sftpClient.ls(p);
      Assert.assertEquals(1, files.size());
      Assert.assertEquals("/" + dirInRoot.getName() + "/" + fileInDirInRoot.getName(),
          files.iterator().next().getPath());
    }
  }

  @Test
  public void testLsNotExist() throws Exception {
    File dir = testFolder.newFile("dir");
    dir.delete();
    Assert.assertFalse(dir.exists());

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a dir as either a relative path "dir" or an absolute path "/dir" and they should be
      // equivalent
      for (String p : new String[]{
          dir.getName(),
          "/" + dir.getName(),
      }) {
        try {
          sftpClient.ls(p);
          Assert.fail("Expected an SFTPException");
        } catch (SFTPException e) {
          Assert.assertEquals("No such file or directory", e.getMessage());
        }
      }
    }
  }

  @Test
  public void testStat() throws Exception {
    File file = testFolder.newFile("file.txt");
    final long modTime = 15000000L;
    file.setLastModified(modTime);

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String p : new String[] {
          file.getName(),
          "/" + file.getName(),
      }) {
        Assert.assertEquals(modTime, sftpClient.stat(p).getMtime() * 1000);
      }
    }
  }

  @Test
  public void testStatNotExist() throws Exception {
    File file = testFolder.newFile("file.txt");
    file.delete();
    Assert.assertFalse(file.exists());

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String p : new String[]{
          file.getName(),
          "/" + file.getName(),
      }) {
        try {
          sftpClient.stat(p);
          Assert.fail("Expected an SFTPException");
        } catch (SFTPException e) {
          Assert.assertEquals("No such file or directory", e.getMessage());
        }
      }
    }
  }

  @Test
  public void testOpen() throws Exception {
    File file = testFolder.newFile("file.txt");
    String text = "hello";
    Files.write(text.getBytes(Charset.forName("UTF-8")), file);
    Assert.assertEquals(text, Files.readFirstLine(file, Charset.forName("UTF-8")));

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String p : new String[] {
          file.getName(),
          "/" + file.getName(),
      }) {
        InputStream is = sftpClient.open(p);
        Assert.assertNotNull(is);
        Assert.assertEquals(text, IOUtils.toString(is, Charset.forName("UTF-8")));
      }
    }
  }

  @Test
  public void testOpenNotExist() throws Exception {
    File file = testFolder.newFile("file.txt");
    file.delete();
    Assert.assertFalse(file.exists());

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String p : new String[] {
          file.getName(),
          "/" + file.getName(),
      }) {
        try {
          sftpClient.stat(p);
          Assert.fail("Expected an SFTPException");
        } catch (SFTPException e) {
          Assert.assertEquals("No such file or directory", e.getMessage());
        }
      }
    }
  }

  private ChrootSFTPClient[] getClientsWithEquivalentRoots(SSHClient sshClient) throws Exception {
    // We'll set path as the "root" in three equivalent ways
    return new ChrootSFTPClient[] {
        // Here, we specify it as an absolute path
        new ChrootSFTPClient(sshClient.newSFTPClient(), path, false),
        // Here, we specify it as a relative path to the user dir, which is path (see #setupSSHD)
        new ChrootSFTPClient(sshClient.newSFTPClient(), "/", true),
        // Here, we specify it as a relative path to the user dir, which is path (see #setupSSHD)
        new ChrootSFTPClient(sshClient.newSFTPClient(), "", true),
    };
  }
}
