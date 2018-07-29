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
package com.streamsets.pipeline.lib.parser.udp;

import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.util.List;

public abstract class AbstractParser {
  protected final ProtoConfigurableEntity.Context context;

  public AbstractParser(ProtoConfigurableEntity.Context context) {
    this.context = context;
  }

  public abstract List<Record> parse(ByteBuf buf, InetSocketAddress recipient, InetSocketAddress sender)
    throws OnRecordErrorException;
}
