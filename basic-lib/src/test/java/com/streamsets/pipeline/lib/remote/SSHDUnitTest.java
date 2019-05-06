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

import com.github.fommil.ssh.SshRsaCrypto;
import com.google.common.base.Preconditions;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.Buffer;
import net.schmizz.sshj.common.SecurityUtils;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import org.apache.commons.io.IOUtils;
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
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.currentThread;

public abstract class SSHDUnitTest {

  protected static final String TESTUSER = "testuser";
  protected static final String TESTPASS = "testpass";

  protected SshServer sshd;
  protected int port;
  protected String path;
  protected String oldWorkingDir;
  protected AtomicInteger opened = new AtomicInteger(0);
  protected AtomicInteger closed = new AtomicInteger(0);
  protected AtomicBoolean closedAll = new AtomicBoolean(true);
  private KeyPair sshdHostKeyPair = null;

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  // SSHD uses the current working directory as the directory from which to serve files. So cd to the correct dir.
  private void cd(String dir) {
    oldWorkingDir = System.getProperty("user.dir");
    System.setProperty("user.dir", dir);
  }

  @Before
  public void before() {
    opened.set(0);
    closed.set(0);
    closedAll.set(true);
  }

  @After
  public void after() throws Exception {
    if (oldWorkingDir != null) {
      System.setProperty("user.dir", oldWorkingDir);
    }
    if (sshd != null && sshd.isOpen()) {
      sshd.close();
    }
  }

  // Need to make sure each test uses a different dir.
  public void setupSSHD(String homeDir) throws Exception {
    cd(homeDir);
    sshd = SshServer.setUpDefaultServer();
    sshd.setPort(0);
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
    port = sshd.getPort();
  }

  protected SSHClient createSSHClient() throws IOException {
    SSHClient sshClient = new SSHClient();
    sshClient.addHostKeyVerifier(new PromiscuousVerifier());
    sshClient.connect("localhost", port);
    Assert.assertTrue(sshClient.isConnected());
    sshClient.authPassword(TESTUSER, TESTPASS);
    Assert.assertTrue(sshClient.isAuthenticated());
    return sshClient;
  }

  protected byte[] getHostsFileEntry() {
    return getHostsFileEntry("[localhost]:" + port);
  }

  protected byte[] getHostsFileEntry(String host) {
    byte[] key = new Buffer.PlainBuffer().putPublicKey(sshdHostKeyPair.getPublic()).getCompactData();
    String entry = host + " ssh-rsa " + Base64.getEncoder().encodeToString(key);
    return entry.getBytes(Charset.forName("UTF-8"));
  }

  protected String getPublicKeyFingerprint() {
    return SecurityUtils.getFingerprint(sshdHostKeyPair.getPublic());
  }

  private static class PasswdAuth implements PasswordAuthenticator {

    @Override
    public boolean authenticate(String username, String password, ServerSession session)
        throws PasswordChangeRequiredException {
      return username.equals(TESTUSER) && password.equals(TESTPASS);
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

  protected KeyPair generateKeyPair() throws Exception {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    return keyGen.generateKeyPair();
  }

  private class HostKeyProvider implements KeyPairProvider {

    HostKeyProvider() throws Exception {
      sshdHostKeyPair = generateKeyPair();
    }

    @Override
    public KeyPair loadKey(String type) {
      Preconditions.checkArgument(type.equals("ssh-rsa"));
      return sshdHostKeyPair;
    }

    @Override
    public Iterable<String> getKeyTypes() {
      return Arrays.asList("ssh-rsa");
    }

    @Override
    public Iterable<KeyPair> loadKeys() {
      return Arrays.asList(sshdHostKeyPair);
    }
  }
}
