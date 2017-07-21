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

package com.streamsets.pipeline.lib.parser.net.netflow.v9;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Objects;

/**
 * <p>The key that represents a unique template ID from a given source (Netflow exporter).</p>
 *
 * From RFC 3954,
 *
 * <blockquote>NetFlow Collectors SHOULD use the combination of the source IP address and the Source ID field
 * to separate different export streams originating from the same Exporter.</blockquote>
 *
 * These fields, along with the templateId, should provide a unique key for templates that still supports template
 * reuse across multiple packets from the same exporter.
 */
public class FlowSetTemplateCacheKey {
  private final FlowKind templateKind;
  private final byte[] sourceId;
  private final InetSocketAddress sourceAddress;
  private final int templateId;

  public FlowSetTemplateCacheKey(
      FlowKind templateKind,
      byte[] sourceId,
      InetSocketAddress sourceAddress,
      int templateId
  ) {
    this.templateKind = templateKind;
    this.sourceId = sourceId;
    this.sourceAddress = sourceAddress;
    this.templateId = templateId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FlowSetTemplateCacheKey that = (FlowSetTemplateCacheKey) o;
    return templateId == that.templateId && templateKind == that.templateKind && Arrays.equals(sourceId,
        that.sourceId
    ) && Objects.equals(sourceAddress, that.sourceAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateKind, Arrays.hashCode(sourceId), sourceAddress, templateId);
  }

  @Override
  public String toString() {
    return String.format(
        "FlowSetTemplateCacheKey{templateKind=%s, sourceId=%s, sourceAddress=%s, templateId=%d}",
        templateKind.name(),
        Arrays.toString(sourceId),
        sourceAddress,
        templateId
    );
  }
}
