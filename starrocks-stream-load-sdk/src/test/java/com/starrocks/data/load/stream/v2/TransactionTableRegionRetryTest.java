/*
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

package com.starrocks.data.load.stream.v2;

import com.starrocks.data.load.stream.LabelGenerator;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransactionTableRegionRetryTest {

    private StreamLoadTableProperties tableProperties;
    private LabelGenerator labelGenerator;

    @Before
    public void setUp() {
        tableProperties = StreamLoadTableProperties.builder()
                .database("db")
                .table("tbl")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100)
                .build();
        labelGenerator = mock(LabelGenerator.class);
        when(labelGenerator.next()).thenReturn("label-1");
    }

    private TransactionTableRegion buildRegion(StreamLoadManager manager, StreamLoader loader, int maxRetries) {
        return new TransactionTableRegion("db.tbl", "db", "tbl",
                manager, tableProperties, loader, labelGenerator, maxRetries, 0);
    }

    @Test
    public void testRetryPendingSetOnRetryAndClearedOnComplete() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        // A loader that immediately completes the region on send
        StreamLoader loader = mock(StreamLoader.class);
        StreamLoadManager manager = mock(StreamLoadManager.class);
        TransactionTableRegion region = buildRegion(manager, loader, 2);

        // Pre-load a row so there's an inactive chunk to flush
        region.write("{\"id\":1}".getBytes());
        region.flush(FlushReason.FORCE);

        Assert.assertFalse("retryPending should be false initially", region.isRetryPending());

        // Simulate the retry branch: a retryable exception
        RuntimeException retryableEx = new RuntimeException("network error");
        // isRetryable(RuntimeException) returns true (it's not a StreamLoadFailException)

        region.fail(retryableEx);

        Assert.assertTrue("retryPending must be true after fail() retry branch", region.isRetryPending());

        // Now simulate a successful complete — retryPending must clear
        StreamLoadResponse response = new StreamLoadResponse();
        region.complete(response);

        Assert.assertFalse("retryPending must be false after complete()", region.isRetryPending());

        executor.shutdown();
    }

    @Test
    public void testRetryPendingClearedOnTerminalFail() {
        StreamLoader loader = mock(StreamLoader.class);
        StreamLoadManager manager = mock(StreamLoadManager.class);
        // maxRetries = 0 so first fail is terminal
        TransactionTableRegion region = buildRegion(manager, loader, 0);

        region.write("{\"id\":2}".getBytes());
        region.flush(FlushReason.FORCE);

        Assert.assertFalse(region.isRetryPending());

        // With maxRetries=0 the non-retryable branch fires immediately
        RuntimeException terminalEx = new RuntimeException("terminal");
        region.fail(terminalEx);

        Assert.assertFalse("retryPending must be false after terminal fail()", region.isRetryPending());
    }

    @Test
    public void testRetryPendingClearedOnUnretryableException() {
        StreamLoader loader = mock(StreamLoader.class);
        StreamLoadManager manager = mock(StreamLoadManager.class);
        TransactionTableRegion region = buildRegion(manager, loader, 5);

        region.write("{\"id\":3}".getBytes());
        region.flush(FlushReason.FORCE);

        // StreamLoadFailException with "too many filtered rows" is NOT retryable
        StreamLoadResponse.StreamLoadResponseBody body = new StreamLoadResponse.StreamLoadResponseBody();
        body.setStatus("Fail");
        body.setMessage("too many filtered rows");
        StreamLoadFailException unretryable = new StreamLoadFailException("too many filtered rows", body);

        region.fail(unretryable);

        Assert.assertFalse("retryPending must be false for non-retryable exception", region.isRetryPending());
    }
}