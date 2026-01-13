/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.table.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class StarRocksSink<InputT>
        implements SupportsWriterState<InputT, StarRocksWriterState>, SupportsCommitter< StarRocksCommittable>,
        Sink<InputT> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSink.class);

    private final StarRocksSinkOptions sinkOptions;
    private final RecordSerializationSchema<InputT> serializationSchema;
    private final StreamLoadProperties streamLoadProperties;

    public StarRocksSink(
            StarRocksSinkOptions sinkOptions,
            RecordSerializationSchema<InputT> serializationSchema,
            StreamLoadProperties streamLoadProperties) {
        this.sinkOptions = sinkOptions;
        this.serializationSchema = serializationSchema;
        this.streamLoadProperties = streamLoadProperties;
    }


    @Override
    public StatefulSinkWriter<InputT, StarRocksWriterState> restoreWriter(WriterInitContext writerInitContext,
                                                                          Collection<StarRocksWriterState> collection)
            throws IOException {
        try {
            return new StarRocksWriter<>(
                    sinkOptions,
                    writerInitContext,
                    writerInitContext.asSerializationSchemaInitializationContext(),
                    serializationSchema,
                    streamLoadProperties,
                    Collections.emptyList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create writer.", e);
        }
    }

    @Override
    public SimpleVersionedSerializer<StarRocksWriterState> getWriterStateSerializer() {
        return new StarRocksWriterStateSerializer();
    }

    @Override
    public Committer<StarRocksCommittable> createCommitter(CommitterInitContext committerInitContext)
            throws IOException {
        return new StarRocksCommitter(sinkOptions, streamLoadProperties);
    }

    @Override
    public SimpleVersionedSerializer<StarRocksCommittable> getCommittableSerializer() {
        return new StarRocksCommittableSerializer();
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext writerInitContext) throws IOException {
        return restoreWriter(writerInitContext, Collections.emptyList());
    }
}
