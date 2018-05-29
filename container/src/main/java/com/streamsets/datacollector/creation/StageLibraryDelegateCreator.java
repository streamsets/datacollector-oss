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
package com.streamsets.datacollector.creation;

import com.streamsets.datacollector.config.StageLibraryDelegateDefinitition;
import com.streamsets.datacollector.runner.StageLibraryDelegateContext;
import com.streamsets.datacollector.runner.StageLibraryDelegateRuntime;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.delegate.StageLibraryDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StageLibraryDelegateCreator {
  private static final Logger LOG = LoggerFactory.getLogger(StageLibraryDelegateCreator.class);

  private static final StageLibraryDelegateCreator CREATOR = new StageLibraryDelegateCreator() {};
  public static StageLibraryDelegateCreator get() {
    return CREATOR;
  }

  /**
   * Create and initialize new delegate.
   *
   * We usually have this functionality in a builder somewhere else (like a Pipeline.Builder), but
   * we put it here for now for simplicity. Since this is container-only class, we will be able to
   * move this logic somewhere else at a later time without difficulties.
   *
   * @param stageLib Stage library task instance.
   * @param configuration SDC Configuration
   * @param stageLibraryName Name of the stage library from which the delegator should be loaded.
   * @param exportedInterface Interface describing which delegator is needed
   * @param <R>  Interface describing which delegator is needed
   * @return Wrapped new instance of the delegator or null on any issue
   */
  public <R> R createAndInitialize(
    StageLibraryTask stageLib,
    Configuration configuration,
    String stageLibraryName,
    Class<R> exportedInterface
  ) {
    StageLibraryDelegate instance = create(
      stageLib,
      stageLibraryName,
      exportedInterface
    );
    if(instance == null) {
      return null;
    }

    // Create & set context
    StageLibraryDelegateContext context = new StageLibraryDelegateContext(
      configuration
    );
    instance.setContext(context);

    return (R)new StageLibraryDelegateRuntime(
      instance.getClass().getClassLoader(),
      instance
    );
  }

  /**
   * Create new instance of the delegator from given stage library.
   *
   * @param stageLib Stage library task instance.
   * @param stageLibraryName Name of the stage library from which the delegator should be loaded.
   * @param exportedInterface Interface describing which delegator is needed
   * @return New instance of the delegator or null on any issue
   */
  public StageLibraryDelegate create(
    StageLibraryTask stageLib,
    String stageLibraryName,
    Class exportedInterface
  ) {
    StageLibraryDelegateDefinitition def = stageLib.getStageLibraryDelegateDefinition(stageLibraryName, exportedInterface);
    if(def == null) {
      return null;
    }

    return createInstance(def);
  }

  /**
   * Create actual instance of delegator.
   *
   * @param def Delegator definition
   * @return New instance or null on any issue
   */
  private StageLibraryDelegate createInstance(StageLibraryDelegateDefinitition def) {
    StageLibraryDelegate instance  = null;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    try {
      Thread.currentThread().setContextClassLoader(def.getClassLoader());
      instance = def.getKlass().newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      LOG.error("Can't create instance of delegator: " + ex.toString(), ex);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }

    return instance;
  }


}
