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

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import redis.clients.jedis.JedisPubSub;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisSubscriptionSource extends BaseRedisSource {
  private final List<String> subscriptionChannels;
  private final List<String> subscriptionPatterns;
  private ConcurrentLinkedQueue<String> buffer = new ConcurrentLinkedQueue<>();
  private Thread subscribeThread;
  private Thread psubscribeThread;
  private RedisListener redisListener;
  private boolean checkBatchSize = true;

  /**
   * Creates a new instance of redis source.
   *
   * @param redisOriginConfigBean origin configuration
   */
  public RedisSubscriptionSource(RedisOriginConfigBean redisOriginConfigBean) {
    super(redisOriginConfigBean);
    this.subscriptionChannels = redisOriginConfigBean.subscriptionChannels;
    this.subscriptionPatterns = redisOriginConfigBean.subscriptionPatterns;
  }

  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    // Validate either non-empty subscriptionChannels or non-empty subscriptionPatterns
    if(this.subscriptionChannels.size() > 0 || this.subscriptionPatterns.size() > 0) {
      // check if inputs are empty
      if (this.subscriptionChannels.size() > 0) {
        for (int i = 0; i < this.subscriptionChannels.size(); i++) {
          if (this.subscriptionChannels.get(i).isEmpty()) {
            this.subscriptionChannels.remove(i);
            i--;
          }
        }
      }
      if (this.subscriptionPatterns.size() > 0) {
        for (int i = 0; i < this.subscriptionPatterns.size(); i++) {
          if (this.subscriptionPatterns.get(i).isEmpty()) {
            this.subscriptionPatterns.remove(i);
            i--;
          }
        }
      }
    }

    if (createRedisClient(issues)) {
      if(this.subscriptionChannels.size() < 1 && this.subscriptionPatterns.size() < 1) {
        issues.add(getContext().createConfigIssue(Groups.REDIS.name(), this.subscriptionChannels.toString(), Errors.REDIS_04, conf.subscriptionChannels));
      } else {
        checkSubscribe(issues);
      }
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  @Override
  public void destroy() {
    redisListener = null;
    super.destroy();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    // Offsets can vary depending on the data source. Here we use an integer as an example only.
    long nextSourceOffset = 0;
    lastSourceOffset = lastSourceOffset == null ? "" : lastSourceOffset;
    if (!lastSourceOffset.equals("")) {
      nextSourceOffset = Long.parseLong(lastSourceOffset);
    }

    int recordCounter = 0;
    long startTime = System.currentTimeMillis();
    int maxRecords = Math.min(maxBatchSize, conf.maxBatchSize);
    if (!getContext().isPreview() && checkBatchSize && conf.maxBatchSize > maxBatchSize) {
      getContext().reportError(Errors.REDIS_06, maxBatchSize);
      checkBatchSize = false;
    }

    while (recordCounter < maxRecords && (startTime + conf.maxWaitTime) > System.currentTimeMillis()) {
      String message = buffer.poll();
      if (null == message) {
        try {
          Thread.sleep(100);
        } catch (Exception e) {
          LOG.debug(e.getMessage(), e);
          break;
        }
      } else {
        List<Record> records = processRedisMessage("id::" + nextSourceOffset, message);
        for (Record record : records) {
          batchMaker.addRecord(record);
        }
        recordCounter += records.size();
        ++nextSourceOffset;
      }
    }
    return lastSourceOffset;
  }

  private boolean checkSubscribe(final List<ConfigIssue> issues) {
    final AtomicBoolean isOk = new AtomicBoolean(false);
    redisListener = new RedisListener();

    Thread.UncaughtExceptionHandler eh = new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        issues.add(getContext().createConfigIssue(Groups.REDIS.name(),
            "subscriptionChannels",
            Errors.REDIS_02,
            subscriptionChannels,
            e.toString()
        ));
        isOk.set(false);
      }
    };

    try {
      startChannelSubscriberThread(eh);
      startPatternSubscriberThread(eh);
      int i = 0;
      while (!isOk.get() && i < conf.connectionTimeout) {
        if (redisListener.isSubscribed()) {
          isOk.set(true);
          break;
        }
        Thread.sleep(1000);
        i++;
      }
      if( i == conf.connectionTimeout) {
        issues.add(getContext().createConfigIssue(Groups.REDIS.name(),
            "uri",
            Errors.REDIS_05,
            conf.uri
        ));

      }
    } catch (Exception e) {
      issues.add(getContext().createConfigIssue(Groups.REDIS.name(),
          "subscriptionChannels",
          Errors.REDIS_02,
          subscriptionChannels,
          e.toString()
      ));
      LOG.debug(e.getMessage(), e);
      isOk.set(false);
    }
    return isOk.get();
  }

  private void startPatternSubscriberThread(Thread.UncaughtExceptionHandler eh) {
    if (null != subscriptionPatterns && !subscriptionPatterns.isEmpty()) {
      Runnable psubscribeRunnable = new Runnable() {
        @Override
        public void run() {
          while (!Thread.currentThread().isInterrupted()) {
            redisClient.psubscribe(redisListener,
                subscriptionPatterns.toArray(new String[subscriptionPatterns.size()])
            );
          }
        }
      };
      psubscribeThread = new Thread(psubscribeRunnable);
      psubscribeThread.setUncaughtExceptionHandler(eh);
      psubscribeThread.start();
    }
  }

  private void startChannelSubscriberThread(Thread.UncaughtExceptionHandler eh) {
    if (null != subscriptionChannels && !subscriptionChannels.isEmpty()) {
      final Runnable subscribeRunnable = new Runnable() {
        @Override
        public void run() {
          while (!Thread.currentThread().isInterrupted()) {
            redisClient.subscribe(redisListener, subscriptionChannels.toArray(new String[subscriptionChannels.size()]));
          }
        }
      };
      subscribeThread = new Thread(subscribeRunnable);
      subscribeThread.setUncaughtExceptionHandler(eh);
      subscribeThread.start();
    }
  }

  class RedisListener extends JedisPubSub {
    @Override
    public void onPMessage(String pattern, String channel, String message) {
      buffer.add(message);
      super.onPMessage(pattern, channel, message);
    }

    @Override
    public void onMessage(String channel, String message) {
      buffer.add(message);
      super.onMessage(channel, message);
    }
  }
}
