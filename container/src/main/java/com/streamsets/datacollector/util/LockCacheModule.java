/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.util;

import javax.inject.Singleton;
import dagger.Module;
import dagger.Provides;

@Module(injects = LockCache.class, library = true)
public class LockCacheModule {

  @Provides
  @Singleton
  public LockCache<String> provideLockCache() {
    return new LockCache<String>();
  }

}
