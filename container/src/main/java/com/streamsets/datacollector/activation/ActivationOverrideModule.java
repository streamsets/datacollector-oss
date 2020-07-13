/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.activation;

import com.streamsets.datacollector.activation.Activation;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Singleton;

@Module(
        injects = {Activation.class}
)
public class ActivationOverrideModule {
    private final ObjectGraph graph;

    /**
     * Loads an activation object from a given other ObjectGraph. Helpful to prevent extra copies of Activation between
     * graphs / subgraphs (created by {@link ObjectGraph#plus(Object...)}).
     */
    public ActivationOverrideModule(ObjectGraph graph) {
        this.graph = graph;
    }

    @Provides
    @Singleton
    public Activation provideActivation() {
        return graph.get(Activation.class);
    }
}