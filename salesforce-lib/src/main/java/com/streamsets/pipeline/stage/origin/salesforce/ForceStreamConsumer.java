/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.salesforce;

import com.sforce.soap.partner.PartnerConnection;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.salesforce.Errors;
import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ForceStreamConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(ForceSource.class);
  private final BlockingQueue<Object> entityQueue;
  // This URL is used only for logging in. The LoginResult
  // returns a serverUrl which is then used for constructing
  // the streaming URL. The serverUrl points to the endpoint
  // where your organization is hosted.

  // The long poll duration.
  private static final int CONNECTION_TIMEOUT = 20 * 1000;  // milliseconds
  private static final int READ_TIMEOUT = 120 * 1000; // milliseconds
  private static final int SUBSCRIBE_TIMEOUT = 10 * 1000; // milliseconds

  private final PartnerConnection connection;
  private final String bayeuxChannel;
  private final String streamingEndpointPath;
  private final long replayFrom;

  private HttpClient httpClient;
  private BayeuxClient client;

  private AtomicBoolean subscribed = new AtomicBoolean(false);

  public ForceStreamConsumer(
      BlockingQueue<Object> entityQueue,
      PartnerConnection connection,
      String apiVersion,
      String pushTopic,
      String lastSourceOffset
  ) {
    this.entityQueue = entityQueue;
    this.connection = connection;
    this.bayeuxChannel = "/topic/" + pushTopic;
    String streamingEndpointPrefix = apiVersion.equals("36.0") ? "/cometd/replay/" : "/cometd/";
    this.streamingEndpointPath = streamingEndpointPrefix + apiVersion;
    this.replayFrom = Long.valueOf(lastSourceOffset.substring(lastSourceOffset.indexOf(':') + 1));
  }

  public void start() throws StageException {
    LOG.info("Running streaming client");

    try {
      client = makeClient();

      Map<String, Long> replayMap = new HashMap<>();
      replayMap.put(bayeuxChannel, replayFrom);
      client.addExtension(new ForceReplayExtension<>(replayMap, entityQueue));

      client.getChannel(Channel.META_HANDSHAKE).addListener(new ClientSessionChannel.MessageListener() {

        public void onMessage(ClientSessionChannel channel, Message message) {

          LOG.info("[CHANNEL:META_HANDSHAKE]: " + message);

          boolean success = message.isSuccessful();
          if (!success) {
            String error = (String) message.get("error");
            if (error != null) {
              LOG.info("Error during HANDSHAKE: " + error);
              try {
                entityQueue.offer(
                    new StageException(Errors.FORCE_09, error),
                    1000L/* conf.requestTimeoutMillis */,
                    TimeUnit.MILLISECONDS
                );
              } catch (InterruptedException e) {
                LOG.error(Errors.FORCE_10.getMessage(), e);
              }
            }

            Exception exception = (Exception) message.get("exception");
            if (exception != null) {
              LOG.info("Exception during HANDSHAKE: ");
              exception.printStackTrace();
              try {
                entityQueue.offer(
                    new StageException(Errors.FORCE_09, exception),
                    1000L/* conf.requestTimeoutMillis */,
                    TimeUnit.MILLISECONDS
                );
              } catch (InterruptedException e) {
                LOG.error(Errors.FORCE_10.getMessage(), e);
              }
            }
          }
        }

      });

      client.getChannel(Channel.META_CONNECT).addListener(new ClientSessionChannel.MessageListener() {
        public void onMessage(ClientSessionChannel channel, Message message) {

          LOG.info("[CHANNEL:META_CONNECT]: " + message);

          boolean success = message.isSuccessful();
          if (!success) {
            String error = (String) message.get("error");
            if (error != null) {
              LOG.info("Error during CONNECT: " + error);
              try {
                entityQueue.offer(
                    new StageException(Errors.FORCE_09, error),
                    1000L/* conf.requestTimeoutMillis */,
                    TimeUnit.MILLISECONDS
                );
              } catch (InterruptedException e) {
                LOG.error(Errors.FORCE_10.getMessage(), e);
              }
            }
          }
        }

      });

      client.getChannel(Channel.META_SUBSCRIBE).addListener(new ClientSessionChannel.MessageListener() {

        public void onMessage(ClientSessionChannel channel, Message message) {
          LOG.info("[CHANNEL:META_SUBSCRIBE]: " + message);
          boolean success = message.isSuccessful();
          if (!success) {
            String error = (String) message.get("error");
            if (error != null) {
              LOG.info("Error during SUBSCRIBE: " + error);
              try {
                entityQueue.offer(
                    new StageException(Errors.FORCE_09, error),
                    1000L/* conf.requestTimeoutMillis */,
                    TimeUnit.MILLISECONDS
                );
              } catch (InterruptedException e) {
                LOG.error(Errors.FORCE_10.getMessage(), e);
              }
            }
          }
          subscribed.set(true);
        }
      });


      client.handshake();
      LOG.info("Waiting for handshake");

      boolean handshaken = client.waitFor(10 * 1000, BayeuxClient.State.CONNECTED);
      if (!handshaken) {
        LOG.error("Failed to handshake: " + client);
        throw new StageException(Errors.FORCE_09, "Timed out waiting for handshake");
      }

      LOG.info("Subscribing for channel: " + bayeuxChannel);

      client.getChannel(bayeuxChannel).subscribe(new ClientSessionChannel.MessageListener() {
        @Override
        public void onMessage(ClientSessionChannel channel, Message message) {
          LOG.info("Received Message: " + message);
          try {
            boolean accepted = entityQueue.offer(message, 1000L/* conf.requestTimeoutMillis */, TimeUnit.MILLISECONDS);
            if (!accepted) {
              LOG.error("Response buffer full, dropped record.");
            }
          } catch (InterruptedException e) {
            LOG.error(Errors.FORCE_10.getMessage(), e);
          }
        }
      });

      long start = System.currentTimeMillis();
      while (!subscribed.get() && System.currentTimeMillis() - start < SUBSCRIBE_TIMEOUT) {
        Thread.sleep(1000);
      }
    } catch (Exception e) {
      LOG.error("Exception making client", e.toString(), e);
      throw new StageException(Errors.FORCE_09, e);
    }
  }

  private BayeuxClient makeClient() throws Exception {
    httpClient = new HttpClient(new SslContextFactory());
    httpClient.setConnectTimeout(CONNECTION_TIMEOUT);
    httpClient.setIdleTimeout(READ_TIMEOUT);
    httpClient.start();

    final String sessionid = connection.getConfig().getSessionId();
    String soapEndpoint = connection.getConfig().getServiceEndpoint();
    String endpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("/services/Soap/"));
    LOG.info("Server URL: {} ", endpoint);
    LOG.info("Session ID: {}", sessionid);

    Map<String, Object> options = new HashMap<>();
    options.put(ClientTransport.MAX_NETWORK_DELAY_OPTION, READ_TIMEOUT);
    LongPollingTransport transport = new LongPollingTransport(options, httpClient) {

      @Override
      protected void customize(Request request) {
        super.customize(request);
        request.header(HttpHeader.AUTHORIZATION, "OAuth " + sessionid);
      }
    };

    String streamingEndpoint = salesforceStreamingEndpoint(endpoint);

    LOG.info("Streaming Endpoint: {}", streamingEndpoint);

    return new BayeuxClient(streamingEndpoint, transport);
  }

  public void stop() throws Exception {
    client.getChannel(bayeuxChannel).unsubscribe();

    client.disconnect();
    boolean disconnected = client.waitFor(10 * 1000, BayeuxClient.State.DISCONNECTED);

    LOG.info("Bayeux client disconnected: {}", disconnected);

    httpClient.stop();
  }

  private String salesforceStreamingEndpoint(String endpoint) throws MalformedURLException {
    return new URL(endpoint + streamingEndpointPath).toExternalForm();
  }
}
