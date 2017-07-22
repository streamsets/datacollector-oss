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

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.Schema;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.lib.event.EventCreator;
import com.streamsets.pipeline.stage.bigquery.lib.BigQueryDelegate;
import com.streamsets.pipeline.stage.bigquery.lib.Groups;
import com.streamsets.pipeline.stage.pubsub.lib.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.streamsets.pipeline.stage.pubsub.lib.Errors.PUBSUB_01;
import static com.streamsets.pipeline.stage.pubsub.lib.Errors.PUBSUB_02;

public class PubSubSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);

  private static final String QUERY = "query";
  private static final String TIMESTAMP = "timestamp";
  private static final String ROW_COUNT = "rows";
  private static final String SOURCE_OFFSET = "offset";
  private static final EventCreator QUERY_SUCCESS = new EventCreator.Builder("big-query-success", 1)
      .withRequiredField(QUERY)
      .withRequiredField(TIMESTAMP)
      .withRequiredField(ROW_COUNT)
      .withRequiredField(SOURCE_OFFSET)
      .build();

  private final PubSubConfig conf;

  private BigQueryDelegate delegate;
  private QueryResult result;
  private Schema schema;
  private int totalCount;

  public PubSubSource(PubSubConfig conf) {
    this.conf = conf;
  }

  @Override
  public void destroy() {
    result = null;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    getCredentials(issues).ifPresent(c -> delegate = new Publisher());

    return issues;
  }

  private Optional<Credentials> getCredentials(List<ConfigIssue> issues) {
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
          "conf.credentials.path",
          PUBSUB_01,
          credentialsFile.getPath()
      ));
    } catch (IOException | IllegalArgumentException e) {
      LOG.error(PUBSUB_02.getMessage(), e);
      issues.add(getContext().createConfigIssue(
          Groups.CREDENTIALS.name(),
          "conf.credentials.path",
          PUBSUB_02
      ));
    }

    return Optional.ofNullable(credentials);
  }

  @Override
  public int getNumberOfThreads() {
    return 0;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {

  }
}
