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

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchSourceConfig;
import com.streamsets.pipeline.stage.config.elasticsearch.Errors;
import com.streamsets.pipeline.stage.elasticsearch.common.ElasticsearchBaseIT;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticsearchSourceIT extends ElasticsearchBaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSourceIT.class);
  private static RestClient restClient;

  @BeforeClass
  public static void setUp() throws Exception {
    ElasticsearchBaseIT.setUp();

    // Create index and add data
    HttpHost host = new HttpHost("127.0.0.1", esHttpPort);
    restClient = RestClient.builder(host).build();

    HttpEntity entity = new StringEntity(
        "{\"mappings\":{\"tweet\":{\"properties\":{\"message\":{\"type\":\"text\"},\"timestamp\":{\"type\":\"date\"}}}}}"
    );
    restClient.performRequest(
        "PUT",
        "/twitter",
        new HashMap<>(),
        entity
    );

    final int numTestDocs = 100;
    for (int i = 1; i <= numTestDocs; i++) {
      restClient.performRequest("PUT", "/twitter/tweet/" + i, new HashMap<>(), makeTweet());
    }

    await().atMost(5, SECONDS).until(() -> dataIsAvailable(restClient, numTestDocs));

    LOG.info("Finished setup");
  }

  private static boolean dataIsAvailable(RestClient restClient, int size) {
    try {
      Response r = restClient.performRequest("GET", "/twitter/tweet/_search?size=" + size);
      Reader reader = new InputStreamReader(r.getEntity().getContent());
      JsonArray hits = new JsonParser()
          .parse(reader)
          .getAsJsonObject()
          .getAsJsonObject("hits")
          .getAsJsonArray("hits");
      return hits.size() == size;
    } catch (IOException ignored) {
      return false;
    }
  }

  private static HttpEntity makeTweet() {
    final String body = String.format(
        "{\"message\":\"%s\",\"timestamp\":%d}",
        UUID.randomUUID().toString(),
        System.currentTimeMillis()
    );
    LOG.debug("Tweet: '{}'", body);
    return new StringEntity(
        body,
        ContentType.APPLICATION_JSON
    );
  }


  @AfterClass
  public static void cleanUp() throws Exception {
    ElasticsearchBaseIT.cleanUp();
  }

  @Test
  public void testBatchModeSingleBatch() throws Exception {
    ElasticsearchSourceConfig conf = new ElasticsearchSourceConfig();
    conf.connection.useSecurity = false;
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;

    conf.index = "twitter";
    conf.mapping = "tweet";

    ElasticsearchSource source = new ElasticsearchSource(conf);
    PushSourceRunner runner = new PushSourceRunner.Builder(ElasticsearchDSource.class, source)
        .addOutputLane("lane")
        .build();

    runner.runInit();
    try {
      runner.runProduce(Collections.emptyMap(), 5, output -> {
        runner.setStop();
        assertNotEquals(null, output.getNewOffset());
        List<Record> records = output.getRecords().get("lane");
        assertEquals(5, records.size());
      });
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testBatchModeToCompletion() throws Exception {
    ElasticsearchSourceConfig conf = new ElasticsearchSourceConfig();
    conf.connection.useSecurity = false;
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;

    conf.index = "twitter";
    conf.mapping = "tweet";
    conf.numSlices = 2;

    ElasticsearchSource source = new ElasticsearchSource(conf);
    PushSourceRunner runner = new PushSourceRunner.Builder(ElasticsearchDSource.class, source)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      final Map<String, String> offsets = new HashMap<>();
      final List<Record> records = Collections.synchronizedList(new ArrayList<>(5));
      AtomicInteger batchCount = new AtomicInteger(0);
      runner.runProduce(offsets, 5, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();
      });
      runner.waitOnProduce();
      // Last batch is empty signaling finish
      assertEquals(22, batchCount.get());
      assertEquals(100, records.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCursorExpiredNoDelete() throws Exception {
    ElasticsearchSourceConfig conf = new ElasticsearchSourceConfig();
    conf.connection.useSecurity = false;
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;

    conf.index = "twitter";
    conf.mapping = "tweet";
    conf.deleteCursor = false;

    ElasticsearchSource source = new ElasticsearchSource(conf);
    PushSourceRunner runner = new PushSourceRunner.Builder(ElasticsearchDSource.class, source)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("lane")
        .build();

    runner.runInit();


    try {
      final Map<String, String> offsets = new HashMap<>();
      AtomicInteger batchCounter = new AtomicInteger(0);
      runner.runProduce(offsets, 5, output -> {
        offsets.put("0", output.getNewOffset());
        int batchCount = batchCounter.incrementAndGet();
        if (batchCount == 1) {
          deleteAllCursors();
        } else if (batchCount > 1) {
          fail("Expected a StageException due to expired cursor");
        }
      });

      runner.waitOnProduce();
      assertEquals(1, batchCounter.get());
    } catch (Exception e) {
      List<Throwable> stageExceptions = Throwables.getCausalChain(e)
          .stream()
          .filter(t -> StageException.class.isAssignableFrom(t.getClass()))
          .collect(Collectors.toList());
      assertEquals(1, stageExceptions.size());
      assertEquals(Errors.ELASTICSEARCH_23, ((StageException) stageExceptions.get(0)).getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCursorExpiredDefault() throws Exception {
    ElasticsearchSourceConfig conf = new ElasticsearchSourceConfig();
    conf.connection.useSecurity = false;
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;

    conf.index = "twitter";
    conf.mapping = "tweet";
    conf.deleteCursor = true;

    ElasticsearchSource source = new ElasticsearchSource(conf);
    PushSourceRunner runner = new PushSourceRunner.Builder(ElasticsearchDSource.class, source)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    runner.runInit();

    final Map<String, String> offsets = new HashMap<>();
    try {
      AtomicInteger batchCounter = new AtomicInteger(0);
      runner.runProduce(offsets, 5, output -> {
        offsets.put("0", output.getNewOffset());
        int batch = batchCounter.incrementAndGet();
        if (batch == 1) {
          deleteAllCursors(); // Because getNewOffset does not work.
        } else if (batch == 2) {
          assertEquals(5, output.getRecords().get("lane").size());
          runner.setStop();
        }
      });
      runner.waitOnProduce();
      assertEquals(2, batchCounter.get());
    } finally {
      runner.runDestroy();
    }
  }

  private void deleteAllCursors() {
    try {
      restClient.performRequest("DELETE", "/_search/scroll/_all");
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Test
  public void testInvalidIncrementalQuery() throws Exception {
    ElasticsearchSourceConfig conf = new ElasticsearchSourceConfig();
    conf.connection.useSecurity = false;
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;

    conf.index = "twitter";
    conf.mapping = "tweet";
    conf.query = "{}";
    conf.isIncrementalMode = true;
    conf.offsetField = "timestamp";
    conf.initialOffset = "now-1d/d";

    ElasticsearchSource source = new ElasticsearchSource(conf);
    PushSourceRunner runner = new PushSourceRunner.Builder(ElasticsearchDSource.class, source)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_25.getCode()));
  }

  @Test
  public void testIncrementalMode() throws Exception {
    ElasticsearchSourceConfig conf = new ElasticsearchSourceConfig();
    conf.connection.useSecurity = false;
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;

    conf.index = "twitter";
    conf.mapping = "tweet";
    conf.query =
        "{\"sort\":[{\"timestamp\":{\"order\":\"asc\"}}],\"query\":{\"range\":{\"timestamp\":{\"gt\":${offset}}}}}";
    conf.isIncrementalMode = true;
    conf.offsetField = "timestamp";
    conf.initialOffset = "now-1d/d";

    ElasticsearchSource source = new ElasticsearchSource(conf);
    PushSourceRunner runner = new PushSourceRunner.Builder(ElasticsearchDSource.class, source)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      final Map<String, String> offsets = new HashMap<>();
      final List<Record> records = new ArrayList<>(5);
      AtomicInteger batchCount = new AtomicInteger(0);
      AtomicInteger lastBatchRecordCount = new AtomicInteger(-1);
      runner.runProduce(offsets, 5, output -> {
        offsets.put("0", output.getNewOffset());
        List<Record> list = output.getRecords().get("lane");
        records.addAll(list);
        if (batchCount.incrementAndGet() == 21) {
          // No new records, but we should have made a request.
          // 20th batch would have finished the first query.
          lastBatchRecordCount.set(list.size());
          runner.setStop();
        }
      });
      runner.waitOnProduce();
      assertEquals(0, lastBatchRecordCount.get());
      assertEquals(21, batchCount.get());
      assertEquals(100, records.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testIncrementalModeWithQueryInterval() throws Exception {
    ElasticsearchSourceConfig conf = new ElasticsearchSourceConfig();
    conf.connection.useSecurity = false;
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;

    conf.index = "twitter";
    conf.mapping = "tweet";
    conf.query =
        "{\"sort\":[{\"timestamp\":{\"order\":\"asc\"}}],\"query\":{\"range\":{\"timestamp\":{\"gt\":${offset}}}}}";
    conf.isIncrementalMode = true;
    conf.offsetField = "timestamp";
    conf.initialOffset = "now-1d/d";
    conf.queryInterval = 5;

    ElasticsearchSource source = new ElasticsearchSource(conf);
    PushSourceRunner runner = new PushSourceRunner.Builder(ElasticsearchDSource.class, source)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      final Map<String, String> offsets = new HashMap<>();
      final List<Record> records = new ArrayList<>(5);
      AtomicInteger batchCount = new AtomicInteger(0);
      AtomicInteger lastBatchRecordCount = new AtomicInteger(-1);
      runner.runProduce(offsets, 5, output -> {
        offsets.put("0", output.getNewOffset());
        List<Record> list = output.getRecords().get("lane");
        records.addAll(list);
        if (batchCount.incrementAndGet() == 22) {
          lastBatchRecordCount.set(list.size());
          runner.setStop();
        }
      });

      await().atLeast(5, SECONDS).atMost(6, SECONDS).until(() -> {
        try {
          runner.waitOnProduce();
        } catch (final Exception ex) {
          throw new RuntimeException(ex);
        }
      });

      assertEquals(22, batchCount.get());
      assertEquals(0, lastBatchRecordCount.get());
      assertEquals(100, records.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testParallelismChanged() throws Exception {
    ElasticsearchSourceConfig conf = new ElasticsearchSourceConfig();
    conf.connection.useSecurity = false;
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;

    conf.index = "twitter";
    conf.mapping = "tweet";
    conf.query =
        "{\"sort\":[{\"timestamp\":{\"order\":\"asc\"}}],\"query\":{\"range\":{\"timestamp\":{\"gt\":${offset}}}}}";
    conf.isIncrementalMode = true;
    conf.offsetField = "timestamp";
    conf.initialOffset = "now-1d/d";
    conf.numSlices = 2;

    ElasticsearchSource source = new ElasticsearchSource(conf);
    PushSourceRunner runner = new PushSourceRunner.Builder(ElasticsearchDSource.class, source)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      final Map<String, String> offsets = new HashMap<>();
      final Gson GSON = new GsonBuilder().setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES).create();
      String offset = GSON.toJson(new ElasticsearchSourceOffset(null, String.valueOf(System.currentTimeMillis())));
      offsets.put("0", offset);
      AtomicInteger batchCount = new AtomicInteger(0);
      runner.runProduce(offsets, 5, output -> batchCount.incrementAndGet());
      runner.waitOnProduce();
      assertEquals(0, batchCount.get());
    } catch (Exception e) {
      List<Throwable> stageExceptions = Throwables.getCausalChain(e)
          .stream()
          .filter(t -> StageException.class.isAssignableFrom(t.getClass()))
          .collect(Collectors.toList());
      assertEquals(1, stageExceptions.size());
      assertEquals(Errors.ELASTICSEARCH_26, ((StageException) stageExceptions.get(0)).getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }
}
