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

package com.streamsets.pipeline.stage.pubsub.origin;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.ExecutorProvider;
import com.google.api.gax.grpc.InstantiatingExecutorProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.PagedResponseWrappers;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.iam.v1.TestIamPermissionsResponse;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.pubsub.lib.Errors;
import com.streamsets.pipeline.stage.pubsub.lib.Groups;
import com.streamsets.pipeline.stage.pubsub.lib.MessageReceiverImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.stage.pubsub.lib.Errors.PUBSUB_01;
import static com.streamsets.pipeline.stage.pubsub.lib.Errors.PUBSUB_02;

public class PubSubSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);
  private static final String CREDENTIALS_PATH = "conf.credentials.path";
  private static final String PUBSUB_SUBSCRIPTIONS_GET_PERMISSION = "pubsub.subscriptions.get";

  private final PubSubConfig conf;

  private CredentialsProvider credentialsProvider;
  private List<Subscriber> subscribers = new ArrayList<>();
  private DataParserFactory parserFactory;

  public PubSubSource(PubSubConfig conf) {
    this.conf = conf;
  }

  @Override
  public void destroy() {
    LOG.info("About to stop subscribers");
    try {
      subscribers.forEach(Subscriber::stopAsync);
    } finally {
      LOG.info("Stopped {} subscribers.", conf.maxThreads);
      subscribers.clear();
    }
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (conf.dataFormatConfig.init(getContext(), conf.dataFormat, Groups.PUBSUB.name(), "conf.dataFormat.", issues)) {
      parserFactory = conf.dataFormatConfig.getParserFactory();
    }

    Credentials credentials = getCredentials(issues);
    credentialsProvider = new FixedCredentialsProvider() {
      @Nullable
      @Override
      public Credentials getCredentials() {
        return credentials;
      }
    };

    issues.addAll(testPermissions(conf));

    return issues;
  }

  private List<ConfigIssue> testPermissions(PubSubConfig conf) {
    List<ConfigIssue> issues = new ArrayList<>();

    TopicAdminSettings settings;
    try {
      settings = TopicAdminSettings.defaultBuilder().setCredentialsProvider(credentialsProvider).build();
    } catch (IOException e) {
      LOG.error(Errors.PUBSUB_04.getMessage(), e.toString(), e);
      issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(), CREDENTIALS_PATH, Errors.PUBSUB_04, e.toString()));
      return issues;
    }

    try (TopicAdminClient topicAdminClient = TopicAdminClient.create(settings)) {
      PagedResponseWrappers.ListTopicsPagedResponse listTopicsResponse = topicAdminClient.listTopics(ProjectName
          .newBuilder()
          .setProject(conf.credentials.projectId)
          .build());

      for (Topic topic : listTopicsResponse.iterateAll()) {
        PagedResponseWrappers.ListTopicSubscriptionsPagedResponse listSubscriptionsResponse = topicAdminClient
            .listTopicSubscriptions(
            TopicName.create(conf.credentials.projectId, topic.getNameAsTopicName().getTopic()));
        for (String s : listSubscriptionsResponse.iterateAll()) {
          LOG.info("Subscription '{}' exists for topic '{}'", s, topic.getName());
        }
      }

      List<String> permissions = new LinkedList<>();
      permissions.add(PUBSUB_SUBSCRIPTIONS_GET_PERMISSION);
      SubscriptionName subscriptionName = SubscriptionName.create(conf.credentials.projectId, conf.subscriptionId);
      TestIamPermissionsResponse testedPermissions =
          topicAdminClient.testIamPermissions(subscriptionName.toString(), permissions);
      if (testedPermissions.getPermissionsCount() != 1) {
        issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(), CREDENTIALS_PATH, Errors.PUBSUB_03));
      }
    } catch (Exception e) {
      LOG.error(Errors.PUBSUB_04.getMessage(), e.toString(), e);
      issues.add(getContext().createConfigIssue(
          Groups.CREDENTIALS.name(),
          CREDENTIALS_PATH,
          Errors.PUBSUB_04,
          e.toString()
      ));
    }
    return issues;
  }

  private Credentials getCredentials(List<ConfigIssue> issues) {
    Credentials credentials = null;

    File credentialsFile;
    if (Paths.get(conf.credentials.path).isAbsolute()) {
      credentialsFile = new File(conf.credentials.path);
    } else {
      credentialsFile = new File(getContext().getResourcesDirectory(), conf.credentials.path);
    }

    try (InputStream in = new FileInputStream(credentialsFile)) {
      credentials = ServiceAccountCredentials.fromStream(in);
    } catch (FileNotFoundException e) {
      LOG.error(PUBSUB_01.getMessage(), credentialsFile.getPath(), e);
      issues.add(getContext().createConfigIssue(
          Groups.CREDENTIALS.name(),
          CREDENTIALS_PATH,
          PUBSUB_01,
          credentialsFile.getPath()
      ));
    } catch (IOException | IllegalArgumentException e) {
      LOG.error(PUBSUB_02.getMessage(), e);
      issues.add(getContext().createConfigIssue(
          Groups.CREDENTIALS.name(),
          CREDENTIALS_PATH,
          PUBSUB_02
      ));
    }

    return credentials;
  }

  @Override
  public int getNumberOfThreads() {
    return conf.maxThreads;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
    int batchSize = Math.min(maxBatchSize, conf.basic.maxBatchSize);

    SubscriptionName subscriptionName = SubscriptionName.create(conf.credentials.projectId, conf.subscriptionId);

    for (int i = 0; i < conf.maxThreads; i++) {
      ExecutorProvider executorProvider = InstantiatingExecutorProvider.newBuilder()
          .setExecutorThreadCount(1).build();

      Subscriber s = Subscriber.defaultBuilder(subscriptionName, new MessageReceiverImpl(getContext(), parserFactory, batchSize, conf.basic.maxWaitTime))
          .setCredentialsProvider(credentialsProvider)
          .setExecutorProvider(executorProvider)
          .build();
      s.addListener(new Subscriber.Listener() {
        @Override
        public void failed(Subscriber.State from, Throwable failure) {
          LOG.error("Subscriber state: {}", from.toString());
          LOG.error("There was an error: {}", failure.toString(), failure);
        }
      }, MoreExecutors.directExecutor());
      subscribers.add(s);
    }

    try {
      subscribers.forEach(Subscriber::startAsync);
    } finally {
      LOG.info("Started {} subscribers.", conf.maxThreads);
    }
    while (!getContext().isStopped()) {
      ThreadUtil.sleep(1000);
    }
  }
}
