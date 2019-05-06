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

import com.streamsets.pipeline.lib.tls.KeyStoreType;
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
import org.apache.ftpserver.ssl.SslConfigurationFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;

public abstract class FTPAndSSHDUnitTest extends SSHDUnitTest {

  protected DefaultFtpServer ftpServer;
  protected boolean supportMDTM = true;

  // Server's Certificate Keystore; can also be used as Client's Certificate Truststore
  protected File serverCertificateKeystore;
  // Client's Certificate Keystore; can also be used as Server's Certificate Truststore
  protected File clientCertificateKeystore;
  // Password for both Keystores
  protected static final String KEYSTORE_PASSWORD = "password";

  @BeforeClass
  public static void beforeClass() {
    Security.addProvider(new BouncyCastleProvider());
  }

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
    setupFTPSServer(homeDir, null, null, false);
  }

  protected void setupFTPSServer(
      String homeDir,
      KeyStoreType serverKeystoreType,
      KeyStoreType clientKeystoreType,
      boolean implicitSsl
  )
      throws Exception {
    FtpServerFactory serverFactory = new FtpServerFactory();
    ListenerFactory factory = new ListenerFactory();
    factory.setPort(0);

    if (serverKeystoreType != null) {
      serverCertificateKeystore = generateCertificateKeystore(serverKeystoreType);
      if (clientKeystoreType != null) {
        clientCertificateKeystore = generateCertificateKeystore(clientKeystoreType);
      }
      SslConfigurationFactory sslFactory = new SslConfigurationFactory();
      sslFactory.setKeystoreFile(serverCertificateKeystore);
      sslFactory.setKeystorePassword(KEYSTORE_PASSWORD);
      sslFactory.setClientAuthentication(clientKeystoreType != null ? "yes" : "none");
      sslFactory.setTruststoreFile(clientCertificateKeystore);
      sslFactory.setTruststorePassword(KEYSTORE_PASSWORD);
      factory.setSslConfiguration(sslFactory.createSslConfiguration());
      factory.setImplicitSsl(implicitSsl);
    }

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

  @Rule
  public TemporaryFolder keystoreFolder = new TemporaryFolder();

  protected File generateCertificateKeystore(KeyStoreType keystoreType) throws Exception {
    KeyPair keyPair = generateKeyPair();
    X509Certificate cert = generateCertificate(keyPair);

    KeyStore keyStore = KeyStore.getInstance(keystoreType.getJavaValue());
    keyStore.load(null, KEYSTORE_PASSWORD.toCharArray());
    keyStore.setKeyEntry("foo", keyPair.getPrivate(), KEYSTORE_PASSWORD.toCharArray(), new Certificate[]{cert});
    File keystoreFile = keystoreFolder.newFile("keystore " + System.currentTimeMillis() + ".jks");
    try (FileOutputStream fos = new FileOutputStream(keystoreFile)) {
      keyStore.store(fos, KEYSTORE_PASSWORD.toCharArray());
    }
    return keystoreFile;
  }

  private X509Certificate generateCertificate(KeyPair keyPair) throws Exception {
    Date from = new Date();
    Date to = new GregorianCalendar(2037, Calendar.DECEMBER, 31).getTime();
    X500Name subject = new X500Name("CN=localhost");
    SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());
    X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(
        subject,
        new BigInteger(64, new SecureRandom()),
        from,
        to,
        subject,
        subPubKeyInfo
    );
    AlgorithmIdentifier sigAlgId = new DefaultSignatureAlgorithmIdentifierFinder().find("SHA512WITHRSA");
    AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);
    ContentSigner contentSigner = new BcRSAContentSignerBuilder(sigAlgId, digAlgId)
        .build(PrivateKeyFactory.createKey(keyPair.getPrivate().getEncoded()));
    X509CertificateHolder certHolder = certBuilder.build(contentSigner);
    return new JcaX509CertificateConverter().setProvider("BC").getCertificate(certHolder);
  }
}
