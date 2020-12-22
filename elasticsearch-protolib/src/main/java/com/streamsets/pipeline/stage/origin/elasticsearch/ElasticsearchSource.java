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
package com.streamsets.pipeline.stage.origin.elasticsearch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.el.OffsetEL;
import com.streamsets.pipeline.lib.elasticsearch.ElasticsearchStageDelegate;
import com.streamsets.pipeline.lib.elasticsearch.PathEscape;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchSourceConfig;
import com.streamsets.pipeline.stage.config.elasticsearch.Errors;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;

public class ElasticsearchSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSource.class);
  private static final Joiner URL_JOINER = Joiner.on("/").skipNulls();
  private static final Gson GSON = new GsonBuilder().setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES).create();
  private static final JsonParser JSON_PARSER = new JsonParser();
  private static final String OFFSET_PLACEHOLDER = "\\$\\{offset}";

  private final ElasticsearchSourceConfig conf;
  private final Map<String, String> params = new HashMap<>();
  private DataParserFactory parserFactory;
  private ElasticsearchStageDelegate delegate;
  private int batchSize;
  private String index;
  private String mapping;

  public ElasticsearchSource(ElasticsearchSourceConfig conf) {
    this.conf = conf;
    if (conf.params != null) {
      params.putAll(conf.params);
    }
    params.put("scroll", conf.cursorTimeout);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    parserFactory = new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON)
        .setCharset(Charsets.UTF_8)
        .setMaxDataLen(-1)
        .setMode(JsonMode.ARRAY_OBJECTS)
        .build();

    if (conf.isIncrementalMode && !conf.query.contains(OffsetEL.OFFSET)) {
      issues.add(getContext().createConfigIssue(
          ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
          "conf.query",
          Errors.ELASTICSEARCH_25
      ));
    }

    delegate = new ElasticsearchStageDelegate(getContext(), conf);

    issues = delegate.init("conf", issues);

    PathEscape pathEscape = loadPathEscape();
    index = escapePath(pathEscape, conf.index, "conf", "index", issues);
    mapping = escapePath(pathEscape, conf.mapping, "conf", "mapping", issues);

    delegate.validateQuery(
        "conf", index, conf.query, conf.isIncrementalMode, OFFSET_PLACEHOLDER, conf.initialOffset, issues
    );

    return issues;
  }

  @VisibleForTesting
  PathEscape loadPathEscape() {
    ServiceLoader<PathEscape> loader = ServiceLoader.load(PathEscape.class);
    Iterator<PathEscape> it = loader.iterator();
    if (!it.hasNext()) {
      throw new StageException(Errors.ELASTICSEARCH_51);
    }
    PathEscape pathEscape = it.next();
    if (it.hasNext()) {
      throw new StageException(Errors.ELASTICSEARCH_52);
    }
    return pathEscape;
  }

  private String escapePath(
      final PathEscape pathEscape,
      final String path,
      final String prefix,
      final String conf,
      final List<ConfigIssue> issues
  ) {
    String result = path;
    try {
      result = pathEscape.escape(path);
    } catch (final URISyntaxException ex) {
      issues.add(getContext().createConfigIssue(
          ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
          prefix + "." + conf,
          Errors.ELASTICSEARCH_50,
          path,
          ex.getMessage()
      ));
    }
    return result;
  }

  @Override
  public void destroy() {
    delegate.destroy();
    super.destroy();
  }

  @Override
  public int getNumberOfThreads() {
    return conf.numSlices;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
    batchSize = Math.min(conf.maxBatchSize, maxBatchSize);
    if (!getContext().isPreview() && conf.maxBatchSize > maxBatchSize) {
      getContext().reportError(Errors.ELASTICSEARCH_35, maxBatchSize);
    }

    ExecutorService executor = Executors.newFixedThreadPool(getNumberOfThreads());
    CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);

    Map<String, ElasticsearchSourceOffset> latestOffsets = prepareOffsets(lastOffsets);

    for (int i = 0; i < getNumberOfThreads(); i++) {
      ElasticsearchSourceOffset lastOffset = latestOffsets.get(String.valueOf(i));

      // Pipeline started for the first time.
      if (lastOffset == null) {
        lastOffset = new ElasticsearchSourceOffset();
      }

      // Pipeline in incremental mode and started for the first time.
      if (conf.isIncrementalMode && lastOffset.getTimeOffset() == null) {
        lastOffset.setTimeOffset(conf.initialOffset);
      }
      completionService.submit(new ElasticsearchTask(i, lastOffset), null);
    }

    // Wait for completion or error
    int numThreadsRemaining = getNumberOfThreads();
    try {
      while (!getContext().isStopped() && numThreadsRemaining-- > 0) {
        completionService.take().get();
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted data generation thread", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      Throwable cause = Throwables.getRootCause(e);
      Throwables.propagateIfInstanceOf(cause, StageException.class);
      throw new StageException(
          Errors.ELASTICSEARCH_22,
          Optional.ofNullable(cause.getMessage()).orElse("no details provided"),
          cause
      );
    } finally {
      // Terminate executor that will also clear up threads that were created
      LOG.info("Shutting down executor service");
      executor.shutdownNow();
    }
  }

  @NotNull
  private Map<String, ElasticsearchSourceOffset> prepareOffsets(Map<String, String> lastOffsets) throws StageException {
    // Remove poll origin offset if it's there - Control Hub could inject it at the begging
    if(lastOffsets.containsKey(Source.POLL_SOURCE_OFFSET_KEY)) {
      getContext().commitOffset(Source.POLL_SOURCE_OFFSET_KEY, null);
    }

    Map<String, ElasticsearchSourceOffset> latestOffsets = lastOffsets.entrySet()
        .stream()
        .filter(Objects::nonNull)
        // This origin never supported single threaded offsets, so any remaining ones are remnants from offset file
        // upgrader and we simply ignore them.
        .filter(entry -> !entry.getKey().equals(Source.POLL_SOURCE_OFFSET_KEY))
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> GSON.fromJson(e.getValue(), ElasticsearchSourceOffset.class)
        ));


    if (latestOffsets.size() > 0 && latestOffsets.size() != getNumberOfThreads()) {
      // The level of parallelism has changed, which changes sharding.
      // Since this would cause duplication, alert the user to reset the pipeline
      // and adjust the initial offset accordingly.
      throw new StageException(Errors.ELASTICSEARCH_26, latestOffsets.size(), getNumberOfThreads());
    }

    return latestOffsets;
  }

  public class ElasticsearchTask implements Runnable {
    private static final String SCROLL_ID = "_scroll_id";
    private static final String HITS = "hits";
    private static final String SOURCE_FIELD = "/_source/";
    private final int threadId;
    private final int maxSlices;
    private ElasticsearchSourceOffset offset;

    ElasticsearchTask(int threadId, ElasticsearchSourceOffset offset) {
      this.threadId = threadId;
      this.maxSlices = getNumberOfThreads();
      this.offset = offset;
    }

    @Override
    public void run() {
      // Override thread name, so that it's easier to find threads from this origin
      Thread.currentThread().setName("ElasticsearchTask-" + threadId);

      while (!getContext().isStopped()) {
        long nextIncrementalExecution = System.currentTimeMillis() + conf.queryInterval * 1000;
        BatchContext batchContext = getContext().startBatch();
        BatchMaker batchMaker = batchContext.getBatchMaker();

        while (true) {
          try {
            produceBatch(batchMaker);
            break;
          } catch (IOException e) {
            LOG.error(Errors.ELASTICSEARCH_22.getMessage(), e);
            throw Throwables.propagate(new StageException(
                Errors.ELASTICSEARCH_22,
                Optional.ofNullable(e.getMessage()).orElse("no details provided"),
                e
            ));
          } catch (StageException e) {
            if (e.getErrorCode() == Errors.ELASTICSEARCH_23 && conf.deleteCursor) {
              // Since we've chosen to clean up cursors automatically, we will simply
              // reset this expired one for the user and continue rather than stop the pipeline.
              LOG.warn("Cursor expired, automatically resetting since delete cursor is enabled.");
              offset.setScrollId(null);
            } else {
              throw Throwables.propagate(e);
            }
          }
        }

        getContext().processBatch(batchContext, String.valueOf(threadId), GSON.toJson(offset));
        if (isFinished(nextIncrementalExecution)) {
          break; // Pipeline is finished.
        }
      }

      cleanup();
    }

    private boolean isFinished(long nextIncrementalExecution) {
      if (offset.getScrollId() == null) {
        if (!conf.isIncrementalMode) {
          return true;
        }

        // Wait for next incremental mode interval
        while (System.currentTimeMillis() < nextIncrementalExecution && !getContext().isStopped()) {
          if (!ThreadUtil.sleep(100)) {
            // Some other thread interrupted this thread when we were in the sleep, but sleep resets the interrupt flag.
            // So we will set the interrupt flag here ourselves.
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
      return false;
    }

    private void cleanup() {
      if (getContext().isPreview() || conf.deleteCursor) {
        try {
          deleteScroll(offset.getScrollId());
        } catch (IOException | StageException e) {
          LOG.warn("Error deleting cursor, will expire automatically.", e);
        }
      }
    }

    private void produceBatch(BatchMaker batchMaker) throws IOException, StageException {
      JsonObject response;
      if (offset.getScrollId() == null) {
        response = getInitialResponse(batchSize, offset.getTimeOffset());
      } else {
        response = getResults(offset.getScrollId());
      }

      if (!response.has(SCROLL_ID)) {
        getContext().reportError(Errors.ELASTICSEARCH_21);
      }
      offset.setScrollId(response.get(SCROLL_ID).getAsString());
      JsonArray hits = response.getAsJsonObject("hits").getAsJsonArray(HITS);

      if (hits.size() == 0) {
        LOG.debug("Worker {}: Hits empty, worker finished.", threadId);
        deleteScroll(offset.getScrollId());
        offset.setScrollId(null);
      } else {
        LOG.trace("Worker {}: Hits: '{}'", threadId, hits.size());
        try (DataParser parser = parserFactory.getParser(offset.getScrollId(), GSON.toJson(hits))) {
          Record record;
          while ((record = parser.parse()) != null) {
            offset = updateTimeOffset(offset, record);
            batchMaker.addRecord(record);
          }
        }
      }
    }

    private ElasticsearchSourceOffset updateTimeOffset(ElasticsearchSourceOffset offset, Record record) {
      if (!conf.isIncrementalMode) {
        return offset;
      }

      final String offsetFieldPath = SOURCE_FIELD + conf.offsetField.replaceAll("\\.", "/");
      if (!record.has(offsetFieldPath)) {
        getContext().reportError(Errors.ELASTICSEARCH_24, offsetFieldPath);
      }

      offset.setTimeOffset(record.get(offsetFieldPath).getValueAsString());
      return offset;
    }

    private JsonObject getInitialResponse(int batchSize, String timeOffset) throws IOException, StageException{
      String requestBodyString = conf.query;
      if (conf.isIncrementalMode) {
        try {
          Long.parseLong(timeOffset);
          requestBodyString = conf.query.replaceAll(OFFSET_PLACEHOLDER, timeOffset);
        } catch (NumberFormatException e) {
          // String offset
          requestBodyString = conf.query.replaceAll(OFFSET_PLACEHOLDER, '"' + timeOffset + '"');
        }
      }

      JsonObject requestBody = JSON_PARSER.parse(requestBodyString).getAsJsonObject();
      requestBody.addProperty("size", batchSize);

      if (maxSlices > 1) {
        JsonObject slice = new JsonObject();
        slice.addProperty("id", threadId);
        slice.addProperty("max", maxSlices);
        requestBody.add("slice", slice);
      }

      final String requestBodyJson = GSON.toJson(requestBody);
      LOG.debug("Request Body: '{}'", requestBodyJson);
      HttpEntity entity = new StringEntity(requestBodyJson, ContentType.APPLICATION_JSON);

      final String endpoint = URL_JOINER.join(
          "",
          index != null && index.trim().isEmpty() ? null : index,
          mapping != null && mapping.trim().isEmpty() ? null : mapping,
          "_search"
      );
      LOG.debug("Built endpoint path: '{}'", endpoint);

      Response response = delegate.performRequest(
          "POST",
          endpoint,
          params,
          entity,
          delegate.getAuthenticationHeader(conf.connection.securityConfig.securityUser.get(),
              conf.connection.securityConfig.securityPassword.get())
      );

      Reader reader = new InputStreamReader(response.getEntity().getContent());
      return JSON_PARSER.parse(reader).getAsJsonObject();
    }

    private JsonObject getResults(String scrollId) throws StageException {
      HttpEntity entity = new StringEntity(
          String.format("{\"scroll\":\"%s\",\"scroll_id\":\"%s\"}", conf.cursorTimeout, scrollId),
          ContentType.APPLICATION_JSON
      );

      try {
        Response response = delegate.performRequest("POST",
            "/_search/scroll",
            conf.params,
            entity,
            delegate.getAuthenticationHeader(conf.connection.securityConfig.securityUser.get(),
                conf.connection.securityConfig.securityPassword.get())
        );

        return parseEntity(response.getEntity());
      } catch (IOException e) {
        LOG.debug("Expired scroll_id: '{}'", scrollId);
        LOG.error(Errors.ELASTICSEARCH_23.getMessage(), e);
        throw new StageException(Errors.ELASTICSEARCH_23);
      }
    }

    private void deleteScroll(String scrollId) throws IOException, StageException {
      if (scrollId == null) {
        return;
      }

      HttpEntity entity = new StringEntity(
          String.format("{\"scroll_id\":[\"%s\"]}", scrollId),
          ContentType.APPLICATION_JSON
      );
      delegate.performRequest(
        "DELETE",
        "/_search/scroll",
        conf.params,
        entity,
        delegate.getAuthenticationHeader(conf.connection.securityConfig.securityUser.get(), conf.connection.securityConfig.securityPassword.get())
      );
    }

    private JsonObject parseEntity(HttpEntity entity) throws IOException {
      Reader reader = new InputStreamReader(entity.getContent());
      return JSON_PARSER.parse(reader).getAsJsonObject();
    }
  }
}
