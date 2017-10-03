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
package com.streamsets.pipeline.stage.origin.redis;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseRedisSource extends BaseSource {

  protected static final Logger LOG = LoggerFactory.getLogger(BaseRedisSource.class);
  protected final RedisOriginConfigBean conf;
  protected DataParserFactory parserFactory;
  protected Jedis redisClient;
  private ErrorRecordHandler errorRecordHandler;

  /**
   * Creates a new instance of redis source.
   *
   * @param redisOriginConfigBean origin configuration
   */
  public BaseRedisSource(RedisOriginConfigBean redisOriginConfigBean) {
    this.conf = redisOriginConfigBean;
  }

  @Override
  protected List<ConfigIssue> init() {
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    List<ConfigIssue> issues = new ArrayList<>();
    conf.dataFormatConfig.init(getContext(),
        conf.dataFormat,
        Groups.REDIS.name(),
        RedisOriginConfigBean.DATA_FROMAT_CONFIG_BEAN_PREFIX,
        issues
    );

    parserFactory = conf.dataFormatConfig.getParserFactory();

    return issues;
  }

  protected List<Record> processRedisMessage(String messageId, String payload) throws StageException {
    List<Record> records = new ArrayList<>();
    try (DataParser parser = parserFactory.getParser(messageId, payload)) {
      Record record = parser.parse();
      while (record != null) {
        records.add(record);
        record = parser.parse();
      }
    } catch (IOException | DataParserException ex) {
      errorRecordHandler.onError(new OnRecordErrorException(Errors.REDIS_03, messageId, ex.toString(), ex));
    }
    return records;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroy() {
    // Clean up any open resources.
    if (null != redisClient) {
      redisClient.disconnect();
      redisClient.close();
      redisClient = null;
    }
    super.destroy();
  }

  protected boolean createRedisClient(List<ConfigIssue> issues) {
    boolean isOk = true;
    if (null == redisClient) {
      try {
        redisClient = new Jedis(URI.create(conf.uri), conf.connectionTimeout * 1000); // connectionTimeout value is in seconds
      } catch (Exception e) {
        LOG.error("Can't create redis client", e);
        issues.add(getContext().createConfigIssue(Groups.REDIS.name(), "uri", Errors.REDIS_01, conf.uri, e.toString()));
        isOk = false;
      }
    }
    return isOk;
  }
}

