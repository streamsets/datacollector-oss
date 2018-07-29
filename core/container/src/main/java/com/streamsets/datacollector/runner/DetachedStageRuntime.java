/*
 * Copyright 2018 StreamSets Inc.
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

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.creation.StageBean;
import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.FieldBatch;
import com.streamsets.pipeline.api.FieldProcessor;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;

import java.util.List;
import java.util.Set;

/**
 * Runtime wrapper for stage that is not directly part of a pipeline - to allow it to be executed independently and
 * inside it's own class loader.
 *
 * Lifecycle methods like init() and destroy() are not allowed to be called by any user as they need to be called
 * only by the framework. Thus they will throw IllegateStateException.
 */
public abstract class DetachedStageRuntime<T> {

  // Static list with all supported Stage implementation and sub-types
  private static Set<Class> SUPPORTED_TYPES = ImmutableSet.of(
    FieldProcessor.class,
    Processor.class,
    Target.class,
    Executor.class
  );

  /**
   * Return true if and only given type is supported by this runtime.
   *
   * @param type Service interface
   * @return True if and only if given interface is supported by this runtime
   */
  public static boolean supports(Class type) {
    return SUPPORTED_TYPES.contains(type);
  }

  protected final StageBean stageBean;
  protected final Stage.Info info;
  protected final Stage.Context context;
  protected final ClassLoader cl;

  /**
   * Create new instance of detached stage runtime.
   */
  public static<T> DetachedStageRuntime<? extends T> create(
    StageBean bean,
    Stage.Info info,
    Stage.Context context,
    Class<T> klass
  ) {
    switch (bean.getDefinition().getType()) {
      case PROCESSOR:
        return new DetachedStageRuntime.DetachedProcessor(bean, info, context);
      case TARGET:
      case EXECUTOR:
        return new DetachedStageRuntime.DetachedTarget(bean, info, context);
      default:
        throw new RuntimeException("Unsupported stage type: " + bean.getDefinition().getType());
    }
  }

  private DetachedStageRuntime(
    StageBean bean,
    Stage.Info info,
    Stage.Context context
  ) {
    this.stageBean = bean;
    this.cl = bean.getDefinition().getStageClassLoader();
    this.info = info;
    this.context = context;
  }


  public List<Stage.ConfigIssue> runInit() {
   return LambdaUtil.privilegedWithClassLoader(
      cl,
      () -> stageBean.getStage().init(info, context)
    );
  }

  public void runDestroy() {
   LambdaUtil.privilegedWithClassLoader(
      cl,
      () -> { stageBean.getStage().destroy(); return null; }
    );
  }

  /**
   * Subclass for processor(s).
   */
  public static class DetachedProcessor extends DetachedStageRuntime implements Processor, FieldProcessor {

    private DetachedProcessor(StageBean bean, Info info, Stage.Context context) {
      super(bean, info, context);
    }

    @Override
    public void process(FieldBatch batch) throws StageException {
     LambdaUtil.privilegedWithClassLoader(
        cl,
        () -> { ((FieldProcessor)stageBean.getStage()).process(batch); return null; }
      );
    }

    @Override
    public void process(Batch batch, BatchMaker batchMaker) throws StageException {
     LambdaUtil.privilegedWithClassLoader(
        cl,
        () -> { ((Processor)stageBean.getStage()).process(batch, batchMaker); return null; }
      );
    }

    @Override
    public List<ConfigIssue> init(Info info, Processor.Context context) {
      throw new IllegalStateException("Calling init() directly on the delegated stage is not allowed.");
    }

    @Override
    public void destroy() {
      throw new IllegalStateException("Calling destroy() directly on the delegated stage is not allowed.");
    }
  }

  /**
   * Subclass for destination(s).
   */
  public static class DetachedTarget extends DetachedStageRuntime implements Target, Executor {

    private DetachedTarget(StageBean bean, Info info, Stage.Context context) {
      super(bean, info, context);
    }

    @Override
    public void write(Batch batch) throws StageException {
      LambdaUtil.privilegedWithClassLoader(
        cl,
        () -> { ((Target)stageBean.getStage()).write(batch); return null; }
      );
    }

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      throw new IllegalStateException("Calling init() directly on the delegated stage is not allowed.");
    }

    @Override
    public void destroy() {
      throw new IllegalStateException("Calling destroy() directly on the delegated stage is not allowed.");
    }
  }
}
