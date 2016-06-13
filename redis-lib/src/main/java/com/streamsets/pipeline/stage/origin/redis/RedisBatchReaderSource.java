/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import redis.clients.jedis.exceptions.JedisDataException;

public class RedisBatchReaderSource extends BaseRedisSource {
    public static final String SCRIPT = "local batchsize = tonumber(ARGV[1])\n" +
            "local result = redis.call('lrange', KEYS[1], 0, batchsize)\n" +
            "redis.call('ltrim', KEYS[1], batchsize + 1, -1)\n" +
            "return result";
    private final String queueName;
    private String shaKey;

    /**
     * Creates a new instance of redis source.
     *
     * @param redisOriginConfigBean origin configuration
     */
    public RedisBatchReaderSource(RedisOriginConfigBean redisOriginConfigBean) {
        super(redisOriginConfigBean);

        this.queueName = redisOriginConfigBean.queueName;
    }


    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();

        if (createRedisClient(issues)) {
            checkConnection(issues);

            redisClient.scriptFlush();
            redisClient.disconnect();
            redisClient.close();
        }

        // If issues is not empty, the UI will inform the user of each configuration issue in the list.
        return issues;
    }

    @Override
    public void destroy() {
        redisClient.scriptFlush();

        super.destroy();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        createRedisClient();

        // Offsets can vary depending on the data source. Here we use an integer as an example only.
        long nextSourceOffset = 0;
        if (lastSourceOffset != null) {
            nextSourceOffset = Long.parseLong(lastSourceOffset);
        }

        ArrayList<String> keys = new ArrayList<>();
        keys.add(queueName);
        ArrayList<String> args = new ArrayList<>();
        args.add(Integer.toString(maxBatchSize));
        HashSet<String> rawResult = new HashSet<>();
        try {
            rawResult = new HashSet<>((List<String>) redisClient.evalsha(shaKey, keys, args));
        } catch (JedisDataException e) {
            if (e.getMessage().startsWith("NOSCRIPT")) {
                // script unregistered by 3rd party
                LOG.error(e.getMessage(), e);
                throw e;
            }
            LOG.error(e.getMessage(), e);
        }

        for (String message : rawResult) {
            List<Record> records = processRedisMessage("id::" + nextSourceOffset, message);
            for (Record record : records) {
                batchMaker.addRecord(record);
            }
            ++nextSourceOffset;
        }

        return lastSourceOffset;
    }

    private void createRedisClient() {
        List<ConfigIssue> issues = new ArrayList<>();
        if (createRedisClient(issues)) {
            if (checkConnection(issues)) {
                registerScripts();
            }
        }
    }

    private void registerScripts() {
        shaKey = redisClient.scriptLoad(SCRIPT);
    }

    private boolean checkConnection(List<ConfigIssue> issues) {
        String response = redisClient.ping();
        if (response.isEmpty()) {
            issues.add(getContext().createConfigIssue(
                    RedisOriginGroups.REDIS.name(),
                    "host",
                    RedisErrors.REDIS_01,
                    conf.uri
            ));
            return false;
        }
        return true;
    }
}
