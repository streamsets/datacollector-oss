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
package com.streamsets.pipeline.stage.origin.opcua.server;

import org.eclipse.milo.opcua.sdk.server.api.nodes.VariableNode;
import org.eclipse.milo.opcua.sdk.server.nodes.AttributeContext;
import org.eclipse.milo.opcua.sdk.server.nodes.delegates.AttributeDelegate;
import org.eclipse.milo.opcua.sdk.server.nodes.delegates.DelegatingAttributeDelegate;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class ValueLoggingDelegate extends DelegatingAttributeDelegate {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public ValueLoggingDelegate() {}

    public ValueLoggingDelegate(@Nullable AttributeDelegate parent) {
        super(parent);
    }

    @Override
    public DataValue getValue(AttributeContext context, VariableNode node) throws UaException {
        DataValue value = super.getValue(context, node);

        // only log external reads
        if (context.getSession().isPresent()) {
            logger.info(
                "getValue() nodeId={} value={}",
                node.getNodeId(), value);
        }

        return value;
    }

    @Override
    public void setValue(AttributeContext context, VariableNode node, DataValue value) throws UaException {
        // only log external writes
        if (context.getSession().isPresent()) {
            logger.info(
                "setValue() nodeId={} value={}",
                node.getNodeId(), value);
        }

        super.setValue(context, node, value);
    }

}
