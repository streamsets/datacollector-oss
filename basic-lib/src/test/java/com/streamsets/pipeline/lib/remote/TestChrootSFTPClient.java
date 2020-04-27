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
package com.streamsets.pipeline.lib.remote;

import com.google.common.io.Files;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPException;
import net.schmizz.sshj.transport.TransportException;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.hamcrest.CoreMatchers.instanceOf;

public class TestChrootSFTPClient extends SSHDUnitTest {

  @Test
  public void testRootNotExist() throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    Assert.assertFalse(new File(path + "/blah").exists());

    try {
      // absolute path
      new ChrootSFTPClient(sshClient.newSFTPClient(), path + "/blah", false, false, false);
      Assert.fail("Expected an SFTPException");
    } catch (SFTPException e) {
      Assert.assertEquals(path + "/blah" + " does not exist", e.getMessage());
    }
    Assert.assertFalse(new File(path + "/blah").exists());

    try {
      // relative path
      new ChrootSFTPClient(sshClient.newSFTPClient(), "/blah", true, false, false);
      Assert.fail("Expected an SFTPException");
    } catch (SFTPException e) {
      Assert.assertEquals(path + "/blah" + " does not exist", e.getMessage());
    }
    Assert.assertFalse(new File(path + "/blah").exists());

    try {
      // relative path
      new ChrootSFTPClient(sshClient.newSFTPClient(), "blah", true, false, false);
      Assert.fail("Expected an SFTPException");
    } catch (SFTPException e) {
      Assert.assertEquals(path + "/blah" + " does not exist", e.getMessage());
    }
    Assert.assertFalse(new File(path + "/blah").exists());
  }

  @Test
  public void testRootNotExistAndMake() throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    Assert.assertFalse(new File(path + "/blah").exists());

    // absolute path
    new ChrootSFTPClient(sshClient.newSFTPClient(), path + "/blah", false, true, false);
    Assert.assertTrue(new File(path + "/blah").exists());

    // relative path
    new ChrootSFTPClient(sshClient.newSFTPClient(), "/blah", true, true, false);
    Assert.assertTrue(new File(path + "/blah").exists());

    // relative path
    new ChrootSFTPClient(sshClient.newSFTPClient(), "blah", true, true, false);
    Assert.assertTrue(new File(path + "/blah").exists());
  }

  @Test
  public void testRootConcatRelativeSeparator() throws Exception {
    // Can't use SSHD because we need to access the root of the disk for these tests, which we might not have permission
    // so we'll need to mock things instead
    String[] values = getConcatRelativeSeparatorValues();
    for (int i = 0; i < getConcatRelativeSeparatorValues().length; i+=3) {
      String userDir = values[i];
      String root = values[i+1];
      String expected = values[i+2];

      SFTPClient sftpClient = Mockito.mock(SFTPClient.class);
      Mockito.when(sftpClient.canonicalize(".")).thenReturn(userDir);
      Mockito.when(sftpClient.statExistence(Mockito.anyString())).thenReturn(FileAttributes.EMPTY);

      new ChrootSFTPClient(sftpClient, root, true, false, false);
      Mockito.verify(sftpClient).statExistence(expected);
    }
  }

  @Test
  public void testSetSFTPClient() throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();
    ChrootSFTPClient sftpClient = new ChrootSFTPClient(sshClient.newSFTPClient(), path, false, false, false);

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
        expectNotExist(() -> sftpClient.ls(p));
      }
    }
  }

  @Test
  public void testStat() throws Exception {
    File file = testFolder.newFile("file.txt");
    final long modTime = 15000000000L;
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
        expectNotExist(() -> sftpClient.stat(p));
      }
    }
  }

  @Test
  public void testExists() throws Exception {
    File file = testFolder.newFile("file.txt");

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
        Assert.assertTrue(sftpClient.exists(p));
      }
    }
  }

  @Test
  public void testExistsNotExist() throws Exception {
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
        Assert.assertFalse(sftpClient.exists(p));
      }
    }
  }

  @Test
  public void testOpenForReading() throws Exception {
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
        InputStream is = sftpClient.openForReading(p);
        Assert.assertThat(is, instanceOf(RemoteFile.ReadAheadRemoteFileInputStream.class));
        Assert.assertTrue(is.getClass().getName().startsWith(SFTPStreamFactory.class.getCanonicalName()));
        Assert.assertNotNull(is);
        Assert.assertEquals(text, IOUtils.toString(is, Charset.forName("UTF-8")));
        is.close();
      }
    }
  }

  @Test
  public void testOpenForReadingReadAheadInputStreamDisabled() throws Exception {
    File file = testFolder.newFile("file.txt");
    String text = "hello";
    Files.write(text.getBytes(Charset.forName("UTF-8")), file);
    Assert.assertEquals(text, Files.readFirstLine(file, Charset.forName("UTF-8")));

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();
    final SFTPClient sftpClient = sshClient.newSFTPClient();
    final ChrootSFTPClient chrootSFTPClient = createChrootSFTPClient(
        sftpClient,
        path,
        false,
        null,
        false,
        true
    );
    final InputStream is = chrootSFTPClient.openForReading(file.getName());
    try {
      Assert.assertThat(is, instanceOf(RemoteFile.RemoteFileInputStream.class));
      Assert.assertTrue(is.getClass().getName().startsWith(SFTPStreamFactory.class.getCanonicalName()));
      Assert.assertNotNull(is);
      Assert.assertEquals(text, IOUtils.toString(is, Charset.forName("UTF-8")));
    } finally {
      is.close();
    }
  }

  @Test
  public void testOpenForReadingNotExist() throws Exception {
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
        expectNotExist(() -> sftpClient.openForReading(p));
      }
    }
  }

  @Test
  public void testOpenForWriting() throws Exception {
    String text = "hello";
    File file = testFolder.newFile("file.txt");

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
        file.delete();
        Assert.assertFalse(file.exists());

        OutputStream os = sftpClient.openForWriting(p);
        Assert.assertTrue(os.getClass().getName().startsWith(SFTPStreamFactory.class.getCanonicalName()));
        Assert.assertNotNull(os);
        IOUtils.write(text, os, Charset.forName("UTF-8"));
        os.close();

        Assert.assertEquals(text, Files.readFirstLine(file, Charset.forName("UTF-8")));
      }
    }
  }

  @Test
  public void testOpenForWritingExists() throws Exception {
    String text = "hello";
    String oldText = "goodbye";
    File file = testFolder.newFile("file.txt");

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
        Files.write(oldText.getBytes(Charset.forName("UTF-8")), file);
        Assert.assertEquals(oldText, Files.readFirstLine(file, Charset.forName("UTF-8")));

        OutputStream os = sftpClient.openForWriting(p);
        Assert.assertNotNull(os);
        IOUtils.write(text, os, Charset.forName("UTF-8"));
        os.close();

        Assert.assertEquals(text, Files.readFirstLine(file, Charset.forName("UTF-8")));
      }
    }
  }

  @Test
  public void testDelete() throws Exception {
    File file = testFolder.newFile("file.txt");

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
        file.createNewFile();
        Assert.assertTrue(file.exists());
        sftpClient.stat(p);

        sftpClient.delete(p);
        expectNotExist(() -> sftpClient.stat(p));
      }
    }
  }

  @Test
  public void testDeleteNotExist() throws Exception {
    File file = testFolder.newFile("file.txt");

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
        file.delete();
        Assert.assertFalse(file.exists());
        expectNotExist(() -> sftpClient.stat(p));

        expectNotExist(() -> {sftpClient.delete(p); return null;});
      }
    }
  }

  @Test
  public void testArchiveNoArchiveDir() throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    String dataPath = testFolder.newFolder("dataDir").getAbsolutePath();

    File fromFile = new File(dataPath, "file.txt");

    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient, null)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String fromPath : new String[] {
          "dataDir/" + fromFile.getName(),
          "/dataDir/" + fromFile.getName(),
      }) {
        String text = "hello";
        Files.write(text.getBytes(Charset.forName("UTF-8")), fromFile);
        Assert.assertEquals(text, Files.readFirstLine(fromFile, Charset.forName("UTF-8")));
        sftpClient.stat(fromPath);

        try {
          sftpClient.archive(fromPath);
          Assert.fail("Expected an SFTPException");
        } catch (IOException e) {
          Assert.assertEquals("No archive directory defined - cannot archive", e.getMessage());
        }

        Assert.assertEquals(text, Files.readFirstLine(fromFile, Charset.forName("UTF-8")));
        Assert.assertEquals(text, IOUtils.toString(sftpClient.openForReading(fromPath)));
        sftpClient.stat(fromPath);
      }
    }
  }

  @Test
  public void testArchive() throws Exception {
    testArchiveHelper(new ArchiveHelperCallable() {
      @Override
      public void call(
          ChrootSFTPClient sftpClient, File fromFile, String fromPath, File toFile, String toPath
      ) throws Exception {
        String text = "hello";
        Files.write(text.getBytes(Charset.forName("UTF-8")), fromFile);
        Assert.assertEquals(text, Files.readFirstLine(fromFile, Charset.forName("UTF-8")));
        sftpClient.stat(fromPath);

        toFile.delete();
        Assert.assertFalse(toFile.exists());
        expectNotExist(() -> sftpClient.stat(toPath));
        toFile.getParentFile().delete();
        Assert.assertFalse(toFile.getParentFile().exists());
        expectNotExist(() -> sftpClient.stat(getParentDir(toPath)));

        String returnedToPath = sftpClient.archive(fromPath);
        Assert.assertEquals(toFile.getAbsolutePath(), returnedToPath);

        Assert.assertEquals(text, Files.readFirstLine(toFile, Charset.forName("UTF-8")));
        Assert.assertEquals(text, IOUtils.toString(sftpClient.openForReading(toPath)));
        sftpClient.stat(toPath);

        Assert.assertFalse(fromFile.exists());
        expectNotExist(() -> sftpClient.stat(fromPath));
      }
    });
  }

  @Test
  public void testArchiveTargetDirExist() throws Exception {
    testArchiveHelper(new ArchiveHelperCallable() {
      @Override
      public void call(
          ChrootSFTPClient sftpClient, File fromFile, String fromPath, File toFile, String toPath
      ) throws Exception {
        String text = "hello";
        Files.write(text.getBytes(Charset.forName("UTF-8")), fromFile);
        Assert.assertEquals(text, Files.readFirstLine(fromFile, Charset.forName("UTF-8")));
        sftpClient.stat(fromPath);

        toFile.delete();
        Assert.assertFalse(toFile.exists());
        expectNotExist(() -> sftpClient.stat(toPath));
        toFile.getParentFile().mkdirs();
        Assert.assertTrue(toFile.getParentFile().exists());
        sftpClient.stat(getParentDir(toPath));

        String returnedToPath = sftpClient.archive(fromPath);
        Assert.assertEquals(toFile.getAbsolutePath(), returnedToPath);

        Assert.assertEquals(text, Files.readFirstLine(toFile, Charset.forName("UTF-8")));
        Assert.assertEquals(text, IOUtils.toString(sftpClient.openForReading(toPath)));
        sftpClient.stat(toPath);
        Assert.assertTrue(toFile.getParentFile().exists());
        sftpClient.stat(getParentDir(toPath));

        Assert.assertFalse(fromFile.exists());
        expectNotExist(() -> sftpClient.stat(fromPath));
      }
    });
  }

  @Test
  public void testArchiveSourceNotExist() throws Exception {
    testArchiveHelper(new ArchiveHelperCallable() {
      @Override
      public void call(
          ChrootSFTPClient sftpClient, File fromFile, String fromPath, File toFile, String toPath
      ) throws Exception {
        fromFile.delete();
        Assert.assertFalse(fromFile.exists());
        expectNotExist(() -> sftpClient.stat(fromPath));

        toFile.delete();
        Assert.assertFalse(toFile.exists());
        expectNotExist(() -> sftpClient.stat(toPath));

        expectNotExist(() -> {sftpClient.archive(fromPath); return null;});

        Assert.assertFalse(toFile.exists());
        expectNotExist(() -> sftpClient.stat(toPath));

        Assert.assertFalse(fromFile.exists());
        expectNotExist(() -> sftpClient.stat(fromPath));
      }
    });
  }

  @Test
  public void testArchiveTargetExist() throws Exception {
    testArchiveHelper(new ArchiveHelperCallable() {
      @Override
      public void call(
          ChrootSFTPClient sftpClient, File fromFile, String fromPath, File toFile, String toPath
      ) throws Exception {
        String fromText = "hello";
        Files.write(fromText.getBytes(Charset.forName("UTF-8")), fromFile);
        Assert.assertEquals(fromText, Files.readFirstLine(fromFile, Charset.forName("UTF-8")));
        sftpClient.stat(fromPath);

        toFile.getParentFile().mkdirs();
        String toText = "goodbye";
        Files.write(toText.getBytes(Charset.forName("UTF-8")), toFile);
        Assert.assertEquals(toText, Files.readFirstLine(toFile, Charset.forName("UTF-8")));
        sftpClient.stat(toPath);

        try {
          sftpClient.archive(fromPath);
          Assert.fail("Expected an SFTPException");
        } catch (SFTPException e) {
          Assert.assertEquals("File/Directory already exists", e.getMessage());
        }

        Assert.assertEquals(toText, Files.readFirstLine(toFile, Charset.forName("UTF-8")));
        Assert.assertEquals(toText, IOUtils.toString(sftpClient.openForReading(toPath)));
        sftpClient.stat(toPath);

        Assert.assertEquals(fromText, Files.readFirstLine(fromFile, Charset.forName("UTF-8")));
        Assert.assertEquals(fromText, IOUtils.toString(sftpClient.openForReading(fromPath)));
        sftpClient.stat(fromPath);
      }
    });
  }

  private interface ArchiveHelperCallable {
    void call(ChrootSFTPClient sftpClient, File fromFile, String fromPath, File toFile, String toPath)
        throws Exception;
  }

  private void testArchiveHelper(ArchiveHelperCallable callable) throws Exception {
    path = testFolder.getRoot().getAbsolutePath();
    File dataPath = testFolder.newFolder("dataDir");
    File archivePath = testFolder.newFolder("archiveDir");

    File fromFile = new File(dataPath, "file.txt");

    archivePath.delete();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient, archivePath.getAbsolutePath())) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String fromPath : new String[] {
          "dataDir/" + fromFile.getName(),
          "/dataDir/" + fromFile.getName(),
      }) {
        File toFile = new File(
            archivePath,
            dataPath.getName() + "/" + fromFile.getName());
        for (String toPath : new String[] {
            archivePath.getName() + "/" + dataPath.getName() + "/" + toFile.getName(),
            "/" + archivePath.getName() + "/" + dataPath.getName() + "/" + toFile.getName()
        }) {
          callable.call(sftpClient, fromFile, fromPath, toFile, toPath);
        }
      }
    }
  }

  @Test
  public void testArchiveConcatRelativeSeparator() throws Exception {
    // Can't use SSHD because we need to access the root of the disk for these tests, which we might not have permission
    // so we'll need to mock things instead
    String[] values = getConcatRelativeSeparatorValues();
    for (int i = 0; i < getConcatRelativeSeparatorValues().length; i+=3) {
      String userDir = values[i];
      String archiveDir = values[i+1];
      String expected = values[i+2];

      SFTPClient sftpClient = Mockito.mock(SFTPClient.class);
      Mockito.when(sftpClient.canonicalize(".")).thenReturn(userDir);
      Mockito.when(sftpClient.statExistence(Mockito.anyString())).thenReturn(FileAttributes.EMPTY);

      ChrootSFTPClient chrootSFTPClient = new ChrootSFTPClient(sftpClient, "/root", true, false, false);
      chrootSFTPClient.setArchiveDir(archiveDir, true);
      chrootSFTPClient.archive("/path");
      Mockito.verify(sftpClient).rename(Mockito.anyString(), Mockito.eq(Paths.get(expected, "/path").toString()));
    }
  }

  @Test
  public void testRename() throws Exception {
    String text = "hello";
    File sourceFile = testFolder.newFile("source.txt");
    File targetFile = testFolder.newFile("target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String source : new String[] {
          sourceFile.getName(),
          "/" + sourceFile.getName(),
      }) {
        for (String target : new String[] {
            targetFile.getName(),
            "/" + targetFile.getName(),
        }) {
          Files.write(text.getBytes(Charset.forName("UTF-8")), sourceFile);
          Assert.assertEquals(text, Files.readFirstLine(sourceFile, Charset.forName("UTF-8")));
          sftpClient.stat(source);

          targetFile.delete();
          Assert.assertFalse(targetFile.exists());
          expectNotExist(() -> sftpClient.stat(target));

          sftpClient.rename(source, target);
          expectNotExist(() -> sftpClient.stat(source));
          sftpClient.stat(target);
          Assert.assertEquals(text, Files.readFirstLine(targetFile, Charset.forName("UTF-8")));
          Assert.assertEquals(text, IOUtils.toString(sftpClient.openForReading(target)));
        }
      }
    }
  }

  @Test
  public void testRenameSourceNotExist() throws Exception {
    File sourceFile = testFolder.newFile("source.txt");
    File targetFile = testFolder.newFile("target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String source : new String[] {
          sourceFile.getName(),
          "/" + sourceFile.getName(),
      }) {
        for (String target : new String[] {
            targetFile.getName(),
            "/" + targetFile.getName(),
        }) {
          sourceFile.delete();
          Assert.assertFalse(sourceFile.exists());
          expectNotExist(() -> sftpClient.stat(source));

          targetFile.delete();
          Assert.assertFalse(targetFile.exists());
          expectNotExist(() -> sftpClient.stat(target));

          expectNotExist(() -> {
            sftpClient.rename(source, target);
            return null;
          });
          Assert.assertFalse(targetFile.exists());
          expectNotExist(() -> sftpClient.stat(target));
        }
      }
    }
  }

  @Test
  public void testRenameTargetExist() throws Exception {
    String sourceText = "hello";
    String targetText = "goodbye";
    File sourceFile = testFolder.newFile("source.txt");
    File targetFile = testFolder.newFile("target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String source : new String[] {
          sourceFile.getName(),
          "/" + sourceFile.getName(),
      }) {
        for (String target : new String[] {
            targetFile.getName(),
            "/" + targetFile.getName(),
        }) {
          Files.write(sourceText.getBytes(Charset.forName("UTF-8")), sourceFile);
          Assert.assertEquals(sourceText, Files.readFirstLine(sourceFile, Charset.forName("UTF-8")));
          sftpClient.stat(source);

          Files.write(targetText.getBytes(Charset.forName("UTF-8")), targetFile);
          Assert.assertEquals(targetText, Files.readFirstLine(targetFile, Charset.forName("UTF-8")));
          sftpClient.stat(target);

          sftpClient.rename(source, target);
          expectNotExist(() -> sftpClient.stat(source));
          sftpClient.stat(target);
          Assert.assertEquals(sourceText, Files.readFirstLine(targetFile, Charset.forName("UTF-8")));
          Assert.assertEquals(sourceText, IOUtils.toString(sftpClient.openForReading(target)));
        }
      }
    }
  }

  @Test
  public void testBooleanRenameOverwriteIsTrueTargetFileDoesntExist() throws Exception {
    String text = "hello";
    File sourceFile = testFolder.newFile("source.txt");
    File targetFile = testFolder.newFile("target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String source : new String[] {
          sourceFile.getName(),
          "/" + sourceFile.getName(),
      }) {
        for (String target : new String[] {
            targetFile.getName(),
            "/" + targetFile.getName(),
        }) {
          Files.write(text.getBytes(Charset.forName("UTF-8")), sourceFile);
          Assert.assertEquals(text, Files.readFirstLine(sourceFile, Charset.forName("UTF-8")));
          sftpClient.stat(source);

          targetFile.delete();
          Assert.assertFalse(targetFile.exists());
          expectNotExist(() -> sftpClient.stat(target));

          Assert.assertTrue(sftpClient.rename(source, target, true));
          expectNotExist(() -> sftpClient.stat(source));
          sftpClient.stat(target);
          Assert.assertEquals(text, Files.readFirstLine(targetFile, Charset.forName("UTF-8")));
        }
      }
    }
  }

  @Test
  public void testBooleanRenameOverwriteIsFalseTargetFileDoesntExist() throws Exception {
    String text = "hello";
    File sourceFile = testFolder.newFile("source.txt");
    File targetFile = testFolder.newFile("target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String source : new String[] {
          sourceFile.getName(),
          "/" + sourceFile.getName(),
      }) {
        for (String target : new String[] {
            targetFile.getName(),
            "/" + targetFile.getName(),
        }) {
          Files.write(text.getBytes(Charset.forName("UTF-8")), sourceFile);
          Assert.assertEquals(text, Files.readFirstLine(sourceFile, Charset.forName("UTF-8")));
          sftpClient.stat(source);

          targetFile.delete();
          Assert.assertFalse(targetFile.exists());
          expectNotExist(() -> sftpClient.stat(target));

          Assert.assertTrue(sftpClient.rename(source, target, false));
          expectNotExist(() -> sftpClient.stat(source));
          sftpClient.stat(target);
          Assert.assertEquals(text, Files.readFirstLine(targetFile, Charset.forName("UTF-8")));
        }
      }
    }
  }

  @Test
  public void testBooleanRenameOverwriteIsTrueTargetFileExists() throws Exception {
    String text = "hello";
    String targetText = "goodbye";
    File sourceFile = testFolder.newFile("source.txt");
    File targetFile = testFolder.newFile("target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String source : new String[] {
          sourceFile.getName(),
          "/" + sourceFile.getName(),
      }) {
        for (String target : new String[] {
            targetFile.getName(),
            "/" + targetFile.getName(),
        }) {
          Files.write(text.getBytes(Charset.forName("UTF-8")), sourceFile);
          Assert.assertEquals(text, Files.readFirstLine(sourceFile, Charset.forName("UTF-8")));
          sftpClient.stat(source);

          Files.write(targetText.getBytes(Charset.forName("UTF-8")), targetFile);
          Assert.assertEquals(targetText, Files.readFirstLine(targetFile, Charset.forName("UTF-8")));
          sftpClient.stat(target);

          Assert.assertTrue(sftpClient.rename(source, target, true));
          expectNotExist(() -> sftpClient.stat(source));
          sftpClient.stat(target);
          Assert.assertEquals(text, Files.readFirstLine(targetFile, Charset.forName("UTF-8")));
        }
      }
    }
  }

  @Test
  public void testBooleanRenameOverwriteIsFalseTargetFileExists() throws Exception {
    String text = "hello";
    String targetText = "goodbye";
    File sourceFile = testFolder.newFile("source.txt");
    File targetFile = testFolder.newFile("target.txt");

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    for (ChrootSFTPClient sftpClient : getClientsWithEquivalentRoots(sshClient)) {
      // We can specify a file as either a relative path "file" or an absolute path "/file" and they should be
      // equivalent
      for (String source : new String[] {
          sourceFile.getName(),
          "/" + sourceFile.getName(),
      }) {
        for (String target : new String[] {
            targetFile.getName(),
            "/" + targetFile.getName(),
        }) {
          Files.write(text.getBytes(Charset.forName("UTF-8")), sourceFile);
          Assert.assertEquals(text, Files.readFirstLine(sourceFile, Charset.forName("UTF-8")));
          sftpClient.stat(source);

          Files.write(targetText.getBytes(Charset.forName("UTF-8")), targetFile);
          Assert.assertEquals(targetText, Files.readFirstLine(targetFile, Charset.forName("UTF-8")));
          sftpClient.stat(target);

          Assert.assertFalse(sftpClient.rename(source, target, false));
          sftpClient.stat(source);
          sftpClient.stat(target);
          Assert.assertEquals(targetText, Files.readFirstLine(targetFile, Charset.forName("UTF-8")));
        }
      }
    }
  }

  private List<ChrootSFTPClient> getClientsWithEquivalentRoots(SSHClient sshClient) throws Exception {
    return getClientsWithEquivalentRoots(sshClient, null);
  }

  private List<ChrootSFTPClient> getClientsWithEquivalentRoots(SSHClient sshClient, String archiveDir)
      throws Exception {
    SFTPClient rawSftpClient = sshClient.newSFTPClient();
    // The following clients are all equivalent, just using different ways of declaring the paths
    List<ChrootSFTPClient> clients = new ArrayList<>();
    // root and archiveDir are both absolute paths
    clients.add(createChrootSFTPClient(rawSftpClient, path, false, archiveDir, false));
    // root is relative to home dir, archiveDir is absolute
    clients.add(createChrootSFTPClient(rawSftpClient, "/", true, archiveDir, false));
    // root is relative to home dir, archiveDir is absolute
    clients.add(createChrootSFTPClient(rawSftpClient, "", true, archiveDir, false));
    if (archiveDir != null) {
      String relArchiveDir = Paths.get(path).relativize(Paths.get(archiveDir)).toString();
      // root is absolute, archiveDir is relative to home dir
      clients.add(createChrootSFTPClient(rawSftpClient, path, false, "/" + relArchiveDir, true));
      // root and archiveDir are both relative to home dir
      clients.add(createChrootSFTPClient(rawSftpClient, "/", true, "/" + relArchiveDir, true));
      // root and archiveDir are both relative to home dir
      clients.add(createChrootSFTPClient(rawSftpClient, "", true, "/" + relArchiveDir, true));
      // root is absolute, archiveDir is relative to home dir
      clients.add(createChrootSFTPClient(rawSftpClient, path, false, relArchiveDir, true));
      // root and archiveDir are both relative to home dir
      clients.add(createChrootSFTPClient(rawSftpClient, "/", true, relArchiveDir, true));
      // root and archiveDir are both relative to home dir
      clients.add(createChrootSFTPClient(rawSftpClient, "", true, relArchiveDir, true));
    }
    return clients;
  }

  public ChrootSFTPClient createChrootSFTPClient(
      SFTPClient sftpClient,
      String root,
      boolean rootRelativeToUserDir,
      String archiveDir,
      boolean archiveDirRelativeToUserDir
  ) throws IOException {
    return createChrootSFTPClient(sftpClient, root, rootRelativeToUserDir, archiveDir, archiveDirRelativeToUserDir, false);
  }

  public ChrootSFTPClient createChrootSFTPClient(
      SFTPClient sftpClient,
      String root,
      boolean rootRelativeToUserDir,
      String archiveDir,
      boolean archiveDirRelativeToUserDir,
      boolean disableReadAheadStream
  ) throws IOException {
    ChrootSFTPClient client = new ChrootSFTPClient(sftpClient, root, rootRelativeToUserDir, false, disableReadAheadStream);
    client.setArchiveDir(archiveDir, archiveDirRelativeToUserDir);
    return client;
  }

  private void expectNotExist(Callable callable) throws Exception {
    try {
      callable.call();
      Assert.fail("Expected an SFTPException");
    } catch (SFTPException e) {
      Assert.assertEquals("No such file or directory", e.getMessage());
    }
  }

  private String getParentDir(String path) {
    int index = path.lastIndexOf("/");
    return path.substring(0, index);
  }

  private String[] getConcatRelativeSeparatorValues() {
    String[] values = new String[]{
        "/", "blah", "/blah",
        "/", "/blah", "/blah",
        "/", "/", "/",
        "/", "", "/",
        "/foo", "blah", "/foo/blah",
        "/foo", "/blah", "/foo/blah",
        "/foo/", "blah", "/foo/blah",
        "/foo/", "/blah", "/foo/blah",
        "/foo", "/", "/foo",
        "/foo", "", "/foo",
        "/foo/", "/", "/foo",
        "/foo/", "", "/foo",
    };
    Assert.assertEquals("values length should be divisible by 3", 0, values.length % 3);    // sanity check
    return values;
  }
}
