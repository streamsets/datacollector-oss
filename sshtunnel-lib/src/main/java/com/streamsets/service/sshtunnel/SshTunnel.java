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

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.LocalPortForwarder;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.OpenSSHKeyFile;
import net.schmizz.sshj.userauth.method.AuthMethod;
import net.schmizz.sshj.userauth.method.AuthPublickey;
import net.schmizz.sshj.userauth.password.PasswordFinder;
import net.schmizz.sshj.userauth.password.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SSH Tunnel class to establish a port forwarding SSH tunnel.
 */
public class SshTunnel {
  private static final Logger LOG = LoggerFactory.getLogger(SshTunnel.class);

  private static class Configs implements Cloneable {
    private String sshHost;
    private int sshPort;
    private String sshUser;
    private List<String> sshHostFingerprints = new ArrayList<>();
    private String sshPrivateKey;
    private String sshPublicKey;
    private String sshPrivateKeyPassword;
    private boolean useCompression = true;
    private int readyTimeOutMillis = 2000;
    private int sshKeepAliveSecs = 30;

    @Override
    public Object clone() throws CloneNotSupportedException {
      return super.clone();
    }
  }

  /**
   * SSH tunnel builder.
   */
  public static class Builder {
    private final Configs configs;

    private Builder() {
      this.configs = new Configs();
    }

    public String getSshHost() {
      return configs.sshHost;
    }

    public Builder setSshHost(String sshHost) {
      configs.sshHost = sshHost;
      return this;
    }

    public int getSshPort() {
      return configs.sshPort;
    }

    public Builder setSshPort(int sshPort) {
      configs.sshPort = sshPort;
      return this;
    }

    public String getSshUser() {
      return configs.sshUser;
    }

    public Builder setSshUser(String sshUser) {
      configs.sshUser = sshUser;
      return this;
    }

    public List<String> getSshHostFingerprints() {
      return configs.sshHostFingerprints;
    }

    public Builder setSshHostFingerprints(List<String> sshHostFingerprint) {
      configs.sshHostFingerprints = sshHostFingerprint;
      return this;
    }

    public String getSshPrivateKey() {
      return configs.sshPrivateKey;
    }

    public Builder setSshPrivateKey(String privateKey) {
      configs.sshPrivateKey = privateKey;
      return this;
    }

    public String getSshPublicKey() {
      return configs.sshPublicKey;
    }

    public Builder setSshPublicKey(String publicKey) {
      configs.sshPublicKey = publicKey;
      return this;
    }

    public String getSshPrivateKeyPassword() {
      return configs.sshPrivateKeyPassword;
    }

    public Builder setSshPrivateKeyPassword(String keyPassword) {
      configs.sshPrivateKeyPassword = keyPassword;
      return this;
    }

    public boolean isUseCompression() {
      return configs.useCompression;
    }

    public Builder setUseCompression(boolean useCompression) {
      configs.useCompression = useCompression;
      return this;
    }


    public int getReadyTimeOut() {
      return configs.readyTimeOutMillis;
    }

    public Builder setReadyTimeOut(int readyTimeOutMillis) {
      Utils.checkArgument(readyTimeOutMillis > 0, "readTimeOutMillis must be greater than zero");
      configs.readyTimeOutMillis = readyTimeOutMillis;
      return this;
    }

    public int getSshKeepAlive() {
      return configs.sshKeepAliveSecs;
    }

    public Builder setSshKeepAlive(int sshKeepAliveSecs) {
      Utils.checkArgument(sshKeepAliveSecs >= 0, "sshKeepAliveSecs must be greater or equal than zero");
      configs.sshKeepAliveSecs = sshKeepAliveSecs;
      return this;
    }


    /**
     * Builds an SSH tunnel, the tunnel needs to be started.
     */
    public SshTunnel build() {
      Utils.checkState(configs.sshHost != null, "sshHost not set");
      Utils.checkState(configs.sshPort > 0, "sshPort not set");
      Utils.checkState(configs.sshUser != null, "sshUser not set");
      Utils.checkState(configs.sshPrivateKey != null, "sshPrivateKey not set");
      Utils.checkState(configs.sshPublicKey != null, "sshPublicKey not set");
      Utils.checkState(configs.sshPrivateKeyPassword != null, "sshPrivateKeyPassword not set");
      try {
        // we clone the configs so the builder can be reconfigured and reutilized without affecting already created
        // SshTunnel instances.
        return new SshTunnel((Configs) configs.clone());
      } catch (CloneNotSupportedException ex) {
        throw new RuntimeException("Cannot happen: " + ex, ex);
      }
    }
  }

  /**
   * Returns an SSH Tunnel builder.
   * <p/>
   * The builder can be reused to build multiple tunnels, its configuration can be changed bettween build() invocations.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static class Forwarder {
    private final ServerSocket serverSocketForwarder;
    private final LocalPortForwarder localPortForwarder;
    private final SshTunnelService.HostPort target;
    private final SshTunnelService.HostPort forwarder;
    private final Thread thread;

    public Forwarder(
        SshTunnelService.HostPort target,
        SshTunnelService.HostPort forwarder,
        Thread thread,
        ServerSocket serverSocketForwarder,
        LocalPortForwarder localPortForwarder
    ) {
      this.serverSocketForwarder = serverSocketForwarder;
      this.localPortForwarder = localPortForwarder;
      this.thread = thread;
      this.target = target;
      this.forwarder = forwarder;
    }

    public SshTunnelService.HostPort getTarget() {
      return target;
    }

    public SshTunnelService.HostPort getForwarder() {
      return forwarder;
    }

    public Thread getThread() {
      return thread;
    }

    public ServerSocket getServerSocketForwarder() {
      return serverSocketForwarder;
    }

    public LocalPortForwarder getLocalPortForwarder() {
      return localPortForwarder;
    }
  }

  private volatile byte state; //0 created, 1 started, 2 stopped
  private final Configs configs;
  private String tunnelEntryHost;
  private SSHClient sshClient;
  private List<Forwarder> forwarders;
  private volatile IOException forwarderEx;

  private SshTunnel(Configs configs) {
    this.configs = configs;
    tunnelEntryHost = "localhost";
  }

  static boolean isPortOpen(String ip, int port) {
    Socket socket = new Socket();
    try {
      socket.connect(new InetSocketAddress(ip, port), 1000);
      return true;
    } catch (Exception ex) {
      return false;
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        // Ignore
      }
    }
  }

  /**
   * Starts an SSH ports forwarding tunnel to the specified target hosts and ports pairs via the SSH server.
   * Returns a Map with mapping from the target hosts ports to the forwarder host ports.
   */
  public synchronized Map<SshTunnelService.HostPort, SshTunnelService.HostPort> start(
      List<SshTunnelService.HostPort> targetHostsPorts
  ) {
    Utils.checkState(state == 0, "Already started");
    Utils.checkNotNull(targetHostsPorts, "targetHostsPorts");
    Utils.checkArgument(!targetHostsPorts.isEmpty(), "targetHostsPorts cannot be empty");
    try {
      // establish the SSH Tunnel

      sshClient = new SSHClient();

      if (configs.useCompression) {
        sshClient.useCompression();
      }

      if (configs.sshHostFingerprints == null || configs.sshHostFingerprints.isEmpty()) {
        LOG.info("Connecting to '{}:{}' without SSH fingerprint verification", configs.sshHost, configs.sshPort);
        sshClient.addHostKeyVerifier(new PromiscuousVerifier());
      } else {
        for (String fingerPrint : configs.sshHostFingerprints) {
          sshClient.addHostKeyVerifier(fingerPrint);
        }
      }
      try {
        sshClient.connect(configs.sshHost, configs.sshPort);
      } catch (java.net.ConnectException ex) {
        throw new IOException(String.format(
            "Could not connect via SSH to '%s:%d', error: %s",
            configs.sshHost,
            configs.sshPort,
            ex
        ), ex);
      }

      OpenSSHKeyFile sshKey = new OpenSSHKeyFile();

      sshKey.init(configs.sshPrivateKey, configs.sshPublicKey, new PasswordFinder() {
        @Override
        public char[] reqPassword(Resource<?> resource) {
          return configs.sshPrivateKeyPassword.toCharArray();
        }

        @Override
        public boolean shouldRetry(Resource<?> resource) {
          return false;
        }
      });

      List<AuthMethod> methods = (List) Arrays.asList(new AuthPublickey(sshKey));

      sshClient.auth(configs.sshUser, methods);
      sshClient.getConnection().getKeepAlive().setKeepAliveInterval(configs.sshKeepAliveSecs);

      // create the port forwarders
      Map<SshTunnelService.HostPort, SshTunnelService.HostPort> mapping = new HashMap<>();
      forwarders = new ArrayList<>();

      try {
        for (SshTunnelService.HostPort targetHostPort : targetHostsPorts) {
          Forwarder forwarder = createPortForwarder(sshClient, targetHostPort);
          forwarders.add(forwarder);
          mapping.put(forwarder.getTarget(), forwarder.getForwarder());
        }
      } catch (Exception ex) {
        stopInternal();
        throw ex;
      }
      state = 1;
      return Collections.unmodifiableMap(mapping);
    } catch (Exception ex) {
      state = 2;
      throw new StageException(Errors.SSH_TUNNEL_01, ex);
    }
  }

  private Forwarder createPortForwarder(SSHClient sshClient, SshTunnelService.HostPort targetHostPort)
      throws IOException, InterruptedException {
    ServerSocket serverSocketForwarder = new ServerSocket();
    serverSocketForwarder.setReuseAddress(true);
    try {
      // letting ServerSocket to find a free port
      serverSocketForwarder.bind(new InetSocketAddress(tunnelEntryHost, 0));
    } catch (BindException ex) {
      throw new IOException(String.format("Could not bind to '%s' on an ephemeral port, error: %s",
          tunnelEntryHost, ex
      ), ex);
    }
    int forwarderLocalPort = serverSocketForwarder.getLocalPort();

    LocalPortForwarder.Parameters params = new LocalPortForwarder.Parameters(
        tunnelEntryHost,
        forwarderLocalPort,
        targetHostPort.getHost(),
        targetHostPort.getPort()
    );

    LocalPortForwarder localPortForwarder = sshClient.newLocalPortForwarder(params, serverSocketForwarder);

    Object lock = new Object();

    Thread forwarderThread = new Thread(() -> {
      try {
        localPortForwarder.listen();
      } catch (IOException ex) {
        forwarderEx = ex;
        synchronized (lock) {
          lock.notify();
        }
      }
    });

    forwarderThread.setDaemon(true);
    forwarderThread.setPriority(Thread.MIN_PRIORITY);
    forwarderThread.start();

    // waiting for 1 sec to see if something when wrong starting the port forwarder in the forwarderThread
    synchronized (lock) {
      lock.wait(1000);
    }

    SshTunnelService.HostPort forwarderPort = new SshTunnelService.HostPort(tunnelEntryHost, forwarderLocalPort);
    verifyForwarderIsReady(targetHostPort, forwarderPort);
    LOG.debug(
        "Started forwarder from '{}:{}' to '{}:{}'",
        forwarderPort.getHost(),
        forwarderPort.getPort(),
        targetHostPort.getHost(),
        targetHostPort.getPort()
    );
    return new Forwarder(targetHostPort, forwarderPort, forwarderThread, serverSocketForwarder, localPortForwarder);
  }

  void verifyForwarderIsReady(SshTunnelService.HostPort targetHostPort, SshTunnelService.HostPort forwarderPort)
      throws IOException {
    if (forwarderEx != null) {
      throw forwarderEx;
    }
    int retries = configs.readyTimeOutMillis / 100;
    while ((retries--) > 0 && !isPortOpen(forwarderPort.getHost(), forwarderPort.getPort())) {
      try {
        Thread.sleep(40);
      } catch (InterruptedException ex) {
        throw new IOException("Forwarder start interrupted : " + ex, ex);
      }
    }

    if (retries <= 0) {
      throw new IOException(
          String.format(
              "Forwarder from '%s:%d' to '%s:%d' failed to start",
              forwarderPort.getHost(),
              forwarderPort.getPort(),
              targetHostPort.getHost(),
              targetHostPort.getPort()
          ));
    }
  }

  /**
   * Checks the health of the SSH tunnel throwing a StageException if unhealthy.
   */
  public void healthCheck() {
    Utils.checkState(state == 1, "Not running");
    if (forwarderEx != null) {
      throw new StageException(Errors.SSH_TUNNEL_02, forwarderEx);
    }
  }

  private synchronized void stopInternal() {
    for (Forwarder forwarder : forwarders) {
      LOG.debug(
          "Stopping forwarderThread from '{}:{}' to '{}:{}'",
          forwarder.getForwarder().getHost(),
          forwarder.getForwarder().getPort(),
          forwarder.getTarget().getHost(),
          forwarder.getTarget().getPort()
      );
      forwarder.getThread().interrupt();
      try {
        forwarder.getLocalPortForwarder().close();
      } catch (IOException ex) {
        LOG.warn(
            "Error closing LocalPortForwarder for '{}:{}': {}",
            forwarder.getTarget().getHost(),
            forwarder.getTarget().getHost(),
            ex,
            ex
        );
      }
      try {
        forwarder.getServerSocketForwarder().close();
      } catch (IOException ex) {
        LOG.warn(
            "Error closing local ServerSocket for '{}:{}': {}",
            forwarder.getTarget().getHost(),
            forwarder.getTarget().getHost(),
            ex,
            ex
        );
      }
    }
    if (sshClient != null) {
      try {
        LOG.debug("Closing tunnel to '{}:{}'", configs.sshHost, configs.sshPort);
        sshClient.disconnect();
      } catch (Exception ex) {
        LOG.warn("Error while disconnecting SSH client, error: {}", ex, ex);
      } finally {
        sshClient = null;
      }
    }
  }

  /**
   * Stops the SSH tunnel.
   */
  public synchronized void stop() {
    if (state == 1) {
      stopInternal();
    }
    state = 2;
  }

}
