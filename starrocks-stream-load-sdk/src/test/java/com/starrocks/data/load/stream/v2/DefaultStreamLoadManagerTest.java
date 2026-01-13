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

import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DefaultStreamLoadManagerTest {

    private StreamLoadProperties properties;

    @Before
    public void setUp() {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("db")
                .table("tbl")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();
        properties = StreamLoadProperties.builder()
                .loadUrls("http://localhost:18030")
                .username("root")
                .password("")
                .version("3.0.0")
                .labelPrefix("test-")
                .defaultTableProperties(tableProps)
                .build();
    }

    private DefaultStreamLoadManager buildManager() {
        return new DefaultStreamLoadManager(properties, true);
    }

    @Test
    public void testCallbackThrowableNotifiesListener() {
        DefaultStreamLoadManager manager = buildManager();
        List<StreamLoadResponse> captured = new ArrayList<>();
        manager.setStreamLoadListener(captured::add);

        manager.callback(new RuntimeException("boom"));

        Assert.assertEquals(1, captured.size());
        Assert.assertNotNull(captured.get(0).getException());
        Assert.assertEquals("boom", captured.get(0).getException().getMessage());
        Assert.assertNull(captured.get(0).getBody());
    }

    @Test
    public void testCallbackStreamLoadFailExceptionAttachesBody() {
        DefaultStreamLoadManager manager = buildManager();
        List<StreamLoadResponse> captured = new ArrayList<>();
        manager.setStreamLoadListener(captured::add);

        StreamLoadResponse.StreamLoadResponseBody body = new StreamLoadResponse.StreamLoadResponseBody();
        body.setNumberFilteredRows(42L);
        body.setStatus("Fail");
        StreamLoadFailException ex = new StreamLoadFailException("fail", body);

        manager.callback(ex);

        Assert.assertEquals(1, captured.size());
        Assert.assertNotNull(captured.get(0).getException());
        Assert.assertNotNull(captured.get(0).getBody());
        Assert.assertEquals(Long.valueOf(42L), captured.get(0).getBody().getNumberFilteredRows());
    }

    @Test
    public void testCallbackListenerExceptionDoesNotEscape() {
        DefaultStreamLoadManager manager = buildManager();
        manager.setStreamLoadListener(response -> {
            throw new RuntimeException("listener blew up");
        });

        RuntimeException original = new RuntimeException("original");
        // must not throw
        manager.callback(original);

        Assert.assertSame(original, manager.getException());
    }

    @Test
    public void testCallbackThrowableWrapsNonException() {
        DefaultStreamLoadManager manager = buildManager();
        List<StreamLoadResponse> captured = new ArrayList<>();
        manager.setStreamLoadListener(captured::add);

        Throwable error = new OutOfMemoryError("oom");
        manager.callback(error);

        Assert.assertEquals(1, captured.size());
        Assert.assertNotNull(captured.get(0).getException());
        Assert.assertTrue(captured.get(0).getException() instanceof RuntimeException);
        Assert.assertSame(error, captured.get(0).getException().getCause());
    }
}