/**
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
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.lib.log.LogConstants;
import org.slf4j.MDC;

import java.util.List;

/**
 * Pipe Runner that wraps one source-less instance of the pipeline.
 */
public class PipeRunner {

  @FunctionalInterface
  public interface ThrowingConsumer<T> {
    public void accept(T t) throws PipelineRuntimeException, StageException;
  }

  /**
   * Runner id.
   *
   * Number from 0 to N denoting "instance number" of this runner.
   */
  private final int runnerId;

  /**
   * Pipe instances for thi srunner.
   */
  private final List<Pipe> pipes;

  public PipeRunner(int runnerId, List<Pipe> pipes) {
    this.runnerId = runnerId;
    this.pipes = ImmutableList.copyOf(pipes);
  }

  public Pipe get(int i) {
    return pipes.get(i);
  }

  public int size() {
    return pipes.size();
  }

  public List<Pipe> getPipes() {
    return pipes;
  }

  /**
   * Execute given consumer for each pipe.
   *
   * This will also set the logger appropriately.
   */
  public void forEach(ThrowingConsumer<Pipe> consumer) throws PipelineRuntimeException, StageException {
    MDC.put(LogConstants.RUNNER, String.valueOf(runnerId));
    try {
      for(Pipe p : pipes) {
        consumer.accept(p);
      }
    } finally {
      MDC.put(LogConstants.RUNNER, "");
    }
  }

  /**
   * Execute given consumer for each pipe, rethrowing usual exceptions as RuntimeException.
   *
   * Suitable for consumer that is not suppose to throw PipelineException and StageException.
   */
  public void forEachNoException(ThrowingConsumer<Pipe> consumer) {
    try {
      forEach(consumer);
    } catch (PipelineException|StageException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieve OffsetCommitTrigger pipe.
   *
   * If it exists, null otherwise.
   */
  public OffsetCommitTrigger getOffsetCommitTrigger() {
    for (Pipe pipe : pipes) {
      Stage stage = pipe.getStage().getStage();
      if (stage instanceof Target && stage instanceof OffsetCommitTrigger) {
        return (OffsetCommitTrigger) stage;
      }
    }
    return null;
  }

  /**
   * Return true if at least one stage is configured with STOP_PIPELINE for OnRecordError policy.
   */
  public boolean onRecordErrorStopPipeline() {
    for(Pipe pipe : pipes) {
      StageContext stageContext = pipe.getStage().getContext();
      if(stageContext.getOnErrorRecord() == OnRecordError.STOP_PIPELINE) {
        return true;
      }
    }

    return false;
  }
}
