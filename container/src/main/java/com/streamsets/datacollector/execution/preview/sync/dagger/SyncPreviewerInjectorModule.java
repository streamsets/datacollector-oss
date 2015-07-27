/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.preview.sync.dagger;

import com.streamsets.datacollector.execution.preview.sync.SyncPreviewer;

import dagger.Module;

@Module(injects = SyncPreviewer.class, library = true, complete = false)
public class SyncPreviewerInjectorModule {
}