/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.salesforce;

import com.sforce.soap.partner.PartnerConnection;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.ForceSourceConfigBean;
import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSession;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ForceStreamConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(ForceStreamConsumer.class);
  private static final String REPLAY_ID_EXPIRED = "400::The replayId \\{\\d+} you provided was invalid.  "
      + "Please provide a valid ID, -2 to replay all events, or -1 to replay only new events.";
  private final BlockingQueue<Message> messageQueue;

  // The long poll duration.
  private static final int CONNECTION_TIMEOUT = 20 * 1000;  // milliseconds
  private static final int READ_TIMEOUT = 120 * 1000; // milliseconds
  private static final int SUBSCRIBE_TIMEOUT = 10 * 1000; // milliseconds

  private final PartnerConnection connection;
  private final String bayeuxChannel;
  private final String streamingEndpointPath;
  private final ForceSourceConfigBean conf;

  private HttpClient httpClient;
  private BayeuxClient client;

  private Pattern replayIdExpiredPattern = Pattern.compile(REPLAY_ID_EXPIRED);
  private AtomicBoolean subscribed = new AtomicBoolean(false);
  private ClientSession.Extension forceReplayExtension;

  public ForceStreamConsumer(
      BlockingQueue<Message> messageQueue,
      PartnerConnection connection,
      ForceSourceConfigBean conf
  ) {
    this.conf = conf;
    this.messageQueue = messageQueue;
    this.connection = connection;
    this.bayeuxChannel = "/topic/" + conf.pushTopic;
    String streamingEndpointPrefix = conf.apiVersion.equals("36.0") ? "/cometd/replay/" : "/cometd/";
    this.streamingEndpointPath = streamingEndpointPrefix + conf.apiVersion;
  }

  private boolean isReplayIdExpired(String message) {
    Matcher matcher = replayIdExpiredPattern.matcher(message);
    return matcher.find();
  }

  private ClientSession.Extension getForceReplayExtension(long replayId) {
    Map<String, Long> replayMap = new HashMap<>();
    replayMap.put(bayeuxChannel, replayId);
    return new ForceReplayExtension<>(replayMap, messageQueue);
  }

  public void subscribeForNotifications(String offset) throws InterruptedException {
    LOG.info("Subscribing for channel: " + bayeuxChannel);

    if (forceReplayExtension != null) {
      client.removeExtension(forceReplayExtension);
    }
    long replayId = Long.valueOf(offset.substring(offset.indexOf(':') + 1));
    forceReplayExtension = getForceReplayExtension(replayId);
    client.addExtension(forceReplayExtension);

    client.getChannel(bayeuxChannel).subscribe(new ClientSessionChannel.MessageListener() {
      @Override
      public void onMessage(ClientSessionChannel channel, Message message) {
        LOG.info("Placing message on queue: {}", message);
        try {
          messageQueue.put(message);
        } catch (InterruptedException e) {
          LOG.error(Errors.FORCE_10.getMessage(), e);
          Thread.currentThread().interrupt();
        }
      }
    });

    long start = System.currentTimeMillis();
    while (!subscribed.get() && System.currentTimeMillis() - start < SUBSCRIBE_TIMEOUT) {
      Thread.sleep(1000);
    }
  }

  public void start() throws StageException {
    LOG.info("Running streaming client");

    try {
      client = makeClient();

      client.getChannel(Channel.META_HANDSHAKE).addListener(new ClientSessionChannel.MessageListener() {

        public void onMessage(ClientSessionChannel channel, Message message) {
          LOG.info("[CHANNEL:META_HANDSHAKE]: " + message);

          // Pass these back to the source as we need to resubscribe or propagate the error
          try {
            messageQueue.put(message);
          } catch (InterruptedException e) {
            LOG.error(Errors.FORCE_10.getMessage(), e);
            Thread.currentThread().interrupt();
          }
        }

      });

      client.getChannel(Channel.META_CONNECT).addListener(new ClientSessionChannel.MessageListener() {
        public void onMessage(ClientSessionChannel channel, Message message) {
          // Just log for troubleshooting - Bayeux client will rehandshake
          LOG.info("[CHANNEL:META_CONNECT]: " + message);
        }

      });

      client.getChannel(Channel.META_SUBSCRIBE).addListener(new ClientSessionChannel.MessageListener() {

        public void onMessage(ClientSessionChannel channel, Message message) {
          LOG.info("[CHANNEL:META_SUBSCRIBE]: " + message);
          if (!message.isSuccessful()) {
            String error = (String) message.get("error");
            if (error != null) {
              try {
                if (isReplayIdExpired(error)) {
                  // Retry subscription for all available events
                  LOG.info("Event ID was not available. Subscribing for available events.");
                  subscribeForNotifications(ForceSource.READ_EVENTS_FROM_START);
                  return;
                } else {
                  messageQueue.put(message);
                }
              } catch (InterruptedException e) {
                LOG.error(Errors.FORCE_10.getMessage(), e);
                Thread.currentThread().interrupt();
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
    } catch (Exception e) {
      LOG.error("Exception making client", e.toString(), e);
      throw new StageException(Errors.FORCE_09, e);
    }
  }

  private BayeuxClient makeClient() throws Exception {
    httpClient = new HttpClient(new SslContextFactory());
    httpClient.setConnectTimeout(CONNECTION_TIMEOUT);
    httpClient.setIdleTimeout(READ_TIMEOUT);
    if (conf.useProxy) {
      httpClient.getProxyConfiguration().getProxies().add(
          new HttpProxy(conf.proxyHostname, conf.proxyPort));
      if (conf.useProxyCredentials) {
        URI proxyURI = new URI("http", null, conf.proxyHostname,
            conf.proxyPort, null, null, null);
        httpClient.getAuthenticationStore().addAuthentication(new BasicAuthentication(
              proxyURI,
              conf.proxyRealm.get(),
              conf.proxyUsername.get(),
              conf.proxyPassword.get()
        ));
      }
    }
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
