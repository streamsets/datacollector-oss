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
package com.streamsets.service.sshtunnel;

import com.google.common.collect.ImmutableList;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.KeyPair;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import org.apache.sshd.common.config.keys.AuthorizedKeyEntry;
import org.apache.sshd.common.config.keys.PublicKeyEntryResolver;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator;
import org.apache.sshd.server.forward.AcceptAllForwardingFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.xml.ws.Holder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.ref.Reference;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.interfaces.RSAPublicKey;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestSshTunnel {
  private java.security.KeyPair sshdKeyPair;
  private String sshdFingerprint;

  private String privKey;
  private String pubKey;
  private String privKeyPass;

  java.security.KeyPair createSshdHostKeyPair(int len) throws Exception {
    KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
    kpg.initialize(len);
    return kpg.generateKeyPair();
  }

  String getSshdHostFingerprint(java.security.KeyPair keyPair) throws Exception {
    RSAPublicKey rsapubkey = (RSAPublicKey) keyPair.getPublic();
    byte[] modulus = rsapubkey.getModulus().toByteArray();
    byte[] exponent = rsapubkey.getPublicExponent().toByteArray();
    byte[] tag = "ssh-rsa".getBytes();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    dos.writeInt(tag.length);
    dos.write(tag);
    dos.writeInt(exponent.length);
    dos.write(exponent);
    dos.writeInt(modulus.length);
    dos.write(modulus);
    byte[] encoded = os.toByteArray();
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] result = digest.digest(encoded);
    return "SHA256:" + Base64.getEncoder().encodeToString(result);
  }

  KeyPair createSshKeyPair(int len) {
    try {
      return KeyPair.genKeyPair(new JSch(), KeyPair.RSA, len);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  String getSshPrivateKeyText(KeyPair keyPair, String password) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    keyPair.writePrivateKey(baos, password.getBytes());
    return new String(baos.toByteArray());
  }

  String getSshPublicKeyText(KeyPair keyPair, String comment) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    keyPair.writePublicKey(baos, comment);
    return new String(baos.toByteArray());
  }

  @Before
  public void before() throws Exception {
    sshdKeyPair = createSshdHostKeyPair(4096);
    sshdFingerprint = getSshdHostFingerprint(sshdKeyPair);

    privKeyPass = UUID.randomUUID().toString();
    KeyPair keyPair = createSshKeyPair(4096);
    privKey = getSshPrivateKeyText(keyPair, privKeyPass);
    pubKey = getSshPublicKeyText(keyPair, "foo@cloud");
  }

  private int randomPort() {
    try {
      ServerSocket s = null;
      try {
        s = new ServerSocket(0);
        return s.getLocalPort();
      } finally {
        if (s != null) {
          s.close();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private SshServer createSshd(PublickeyAuthenticator publickeyAuthenticator, java.security.KeyPair sshdKeyPair) {
    SshServer sshd = SshServer.setUpDefaultServer();
    sshd.setHost("localhost");
    sshd.setPort(randomPort());

    KeyPairProvider keyPairProvider = KeyPairProvider.wrap(sshdKeyPair);
    sshd.setKeyPairProvider(keyPairProvider);

    sshd.setForwardingFilter(AcceptAllForwardingFilter.INSTANCE);
    sshd.setPublickeyAuthenticator(publickeyAuthenticator);
    return sshd;
  }

  public interface SshCommand {
    void run(String sshHost, int sshPort, String sshHostFingerPrint, Runnable sshdStopper) throws Exception;
  }

  public void runSshd(PublickeyAuthenticator authenticator, SshCommand command) throws Exception {
    SshServer sshd = createSshd(authenticator, sshdKeyPair);
    try {
      sshd.start();
      command.run(sshd.getHost(), sshd.getPort(), sshdFingerprint, () -> {
        try {
          sshd.stop();
        } catch (Exception ex) {
          throw new RuntimeException("Stopping SSHD: " + ex, ex);
        }
      });
    } finally {
      sshd.stop();
    }
  }

  enum Mode { RUN, FAIL_BEFORE_SSH_TUNNEL_CREATION, FAIL_BEFORE_CLIENT_CONNECTION, FAIL_DURING_CLIENT_CONNECTION }

  void testSshTunnel(String sshdHostFingerprint, String sshPubKey, String privKeyPass, Mode mode) throws Exception {
    String sshUser = "sshTunnelUser";

    List<AuthorizedKeyEntry> authorizedKeyEntries = AuthorizedKeyEntry.readAuthorizedKeys(new ByteArrayInputStream(
        sshPubKey.getBytes()), true);
    PublickeyAuthenticator
        publicKeysAuthenticator
        = PublickeyAuthenticator.fromAuthorizedEntries(PublicKeyEntryResolver.IGNORING, authorizedKeyEntries);

    AtomicBoolean connectedCheck = new AtomicBoolean();
    Holder<Exception> exceptionHolder = new Holder<>();

    CountDownLatch latch = new CountDownLatch(1);
    ServerSocket serverSocket = new ServerSocket(0);
    int appPort = serverSocket.getLocalPort();
    Thread appServer = new Thread(() -> {
      try {
        Socket socket = serverSocket.accept();
        connectedCheck.set(socket.isConnected());
        socket.close();
      } catch (Exception ex) {
        exceptionHolder.value = ex;
      } finally {
        latch.countDown();
      }
    });
    appServer.setDaemon(true);
    appServer.start();

    runSshd(publicKeysAuthenticator, (sshHost, sshPort, sshHostFingerPrint, sshdStopper) -> {
      String privKey = this.privKey;

      SshTunnel.Builder builder = SshTunnel.builder();
      builder.setSshHost(sshHost);
      builder.setSshPort(sshPort);
      if (sshdHostFingerprint != null) {
        builder.setSshHostFingerprints(Arrays.asList(sshdHostFingerprint));
      }
      builder.setSshUser(sshUser);
      builder.setSshPrivateKey(privKey);
      builder.setSshPublicKey(sshPubKey);
      builder.setSshPrivateKeyPassword(privKeyPass);
      builder.setUseCompression(true);
      SshTunnel tunnel = builder.build();

      if (mode == Mode.FAIL_BEFORE_SSH_TUNNEL_CREATION) {
        sshdStopper.run();
      }
      SshTunnelService.HostPort targetHostPort = new SshTunnelService.HostPort("localhost", appPort);
      Map<SshTunnelService.HostPort, SshTunnelService.HostPort> portMapping
          = tunnel.start(ImmutableList.of(targetHostPort));

      if (mode == Mode.FAIL_BEFORE_CLIENT_CONNECTION) {
        sshdStopper.run();
      }
      tunnel.healthCheck();

      SshTunnelService.HostPort forwarderPort = portMapping.get(targetHostPort);
      Socket socket = new Socket(forwarderPort.getHost(), forwarderPort.getPort());
      Assert.assertTrue(socket.isConnected());

      if (mode == Mode.FAIL_DURING_CLIENT_CONNECTION) {
        sshdStopper.run();
      }
      tunnel.healthCheck();

      socket.close();
      latch.await();
      tunnel.stop();

      Assert.assertTrue(connectedCheck.get());
      Assert.assertNull(exceptionHolder.value);

    });

    serverSocket.close();
  }

  @Test
  public void testSshTunnelOk() throws Exception {
    testSshTunnel(sshdFingerprint, pubKey, privKeyPass, Mode.RUN);
  }

  @Test
  public void testSshTunnelOkNoFingerprint() throws Exception {
    testSshTunnel(null, pubKey, privKeyPass, Mode.RUN);
  }

  @Test(expected = Exception.class)
  public void testSshTunnelWrongFingerprint() throws Exception {
    String wrongFingerprint = "ce:a7:c1:cf:17:3f:96:49:6a:53:1a:05:0b:ba:90:db";
    testSshTunnel(wrongFingerprint, pubKey, privKeyPass, Mode.RUN);
  }

  @Test(expected = Exception.class)
  public void testSshTunnelWrongSshPubKey() throws Exception {
    String wrongPubKey = getSshPublicKeyText(createSshKeyPair(4096), "wrong");
    testSshTunnel(sshdFingerprint, wrongPubKey, privKeyPass, Mode.RUN);
  }

  @Test(expected = Exception.class)
  public void testSshTunnelWrongSshPrivKeyPassword() throws Exception {
    String wrongPrivKeyPass = UUID.randomUUID().toString();
    testSshTunnel(sshdFingerprint, pubKey, wrongPrivKeyPass, Mode.RUN);
  }

  private void testSshTunnelConnectionFailure(Mode mode, Class<? extends Throwable> expectedExceptionClass)
      throws Exception {
    try {
      testSshTunnel(sshdFingerprint, pubKey, privKeyPass, mode);
      Assert.fail();
    } catch (Throwable ex) {
      Throwable originalEx = ex;
      while (ex != null && ex.getClass() != expectedExceptionClass) {
        ex = ex.getCause();
      }
      Assert.assertNotNull(String.format("Got '%s', expected '%s' as cause", originalEx, expectedExceptionClass), ex);
    }
  }

  @Test
  public void testSshTunnelFailBeforeTunnelCreation() throws Exception {
    testSshTunnelConnectionFailure(Mode.FAIL_BEFORE_SSH_TUNNEL_CREATION, StageException.class);
  }

  @Test
  public void testSshTunnelFailBeforeClientConnection() throws Exception {
    testSshTunnelConnectionFailure(Mode.FAIL_BEFORE_CLIENT_CONNECTION, StageException.class);
  }

  @Test
  public void testSshTunnelFailBeforeDuringClientConnection() throws Exception {
    testSshTunnelConnectionFailure(Mode.FAIL_DURING_CLIENT_CONNECTION, StageException.class);
  }


}
