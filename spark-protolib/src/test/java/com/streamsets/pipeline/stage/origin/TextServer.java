/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TextServer {
  private static final Logger LOG = LoggerFactory.getLogger(TextServer.class);
  private final ServerSocket serverSocket;
  private final List<String> messages;
  private Thread servingThread;

  public TextServer(String msg) throws IOException {
    this(Arrays.asList(new String[]{msg}));
  }
  public TextServer(List < String > messages) throws IOException {
    serverSocket = new ServerSocket(0);
    serverSocket.setReuseAddress(true);
    this.messages = messages;
  }

  public int getPort() {
    return serverSocket.getLocalPort();
  }
  public void stop() {
    if (servingThread != null) {
      servingThread.interrupt();
    }
  }
  public void start() {
    servingThread = new Thread() {
      public void run() {
        try {
          while (true) {
            Socket clientSocket = serverSocket.accept();
            LOG.info("Got client: " + clientSocket.getInetAddress());
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            clientSocket.setTcpNoDelay(true);
            while(clientSocket.isConnected()) {
              for (String message: messages) {
                out.write(message + "\n");
                out.flush();
              }
              LOG.info("Sent " + messages.size());
              TimeUnit.SECONDS.sleep(1);
            }
          }
        } catch (InterruptedException ie) {
          // ignored
        } catch (Throwable throwable) {
          String msg = "Error in TextServer, exiting: " + throwable;
          LOG.error(msg, throwable);
        }
      }
    };
    servingThread.setDaemon(true);
    servingThread.setName(getClass().getName());
    servingThread.start();
  }
}
