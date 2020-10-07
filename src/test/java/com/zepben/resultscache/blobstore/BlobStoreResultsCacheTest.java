/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.resultscache.blobstore;

import com.zepben.blobstore.BlobReader;
import com.zepben.blobstore.BlobStore;
import com.zepben.blobstore.BlobWriter;
import com.zepben.resultscache.ResultsCache;
import com.zepben.resultscache.ResultsCacheException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.zepben.testutils.exception.ExpectException.expect;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.IsNot.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class BlobStoreResultsCacheTest {

    private static final int NUM_VALUES_TO_ADD = 1000;

    private BlobStore blobStore = mock(BlobStore.class);
    private BlobReader blobReader = mock(BlobReader.class);
    private BlobWriter blobWriter = mock(BlobWriter.class);

    private ResultsCache cache;

    @BeforeEach
    public void before() {
        when(blobStore.reader()).thenReturn(blobReader);
        when(blobStore.writer()).thenReturn(blobWriter);
        cache = new BlobStoreResultsCache(blobStore);
    }

    @Test
    public void works() throws Exception {
        Map<String, byte[]> values = new HashMap<>();
        final int[] putCount = {0};

        when(blobReader.get(ArgumentMatchers.<String>any(), any()))
            .then(invocation -> values.get(invocation.getArgument(0).toString()));
        when(blobWriter.write(any(), any(), any()))
            .then(invocation -> {
                values.put(invocation.getArgument(0).toString(), invocation.getArgument(2));
                ++putCount[0];
                return true;
            });

        Random random = new Random();
        for (int i = 0; i < NUM_VALUES_TO_ADD; ++i) {
            byte[] value = new BigInteger(160, random).toString(32).getBytes();
            assertThat(value, notNullValue());
            assertThat(value.length, greaterThan(0));

            String key = cache.put(value);
            assertThat(key, notNullValue());
            assertThat(key, not(equalTo("")));
            assertThat(cache.get(key), equalTo(value));
        }
        assertThat(values.size(), equalTo(NUM_VALUES_TO_ADD));
        verify(blobWriter, times(putCount[0])).commit();

        cache.close();
        expect(() -> cache.get("key")).toThrow(ResultsCacheException.class).withMessage("Results cache has been closed");
        expect(() -> cache.put("value".getBytes())).toThrow(ResultsCacheException.class).withMessage("Results cache has been closed");
        expect(() -> cache.addTimeToLive("key")).toThrow(ResultsCacheException.class).withMessage("Results cache has been closed");
        expect(() -> cache.updateTimeToLive("key")).toThrow(ResultsCacheException.class).withMessage("Results cache has been closed");
        expect(() -> cache.processTimeToLive(Duration.ZERO)).toThrow(ResultsCacheException.class).withMessage("Results cache has been closed");
        cache.close();
    }

    @Test
    public void addTimeToLive() throws Exception {
        String key = "key";
        cache.addTimeToLive(key);

        ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(blobWriter).write(eq(key), eq(BlobStoreResultsCache.TTL_ATTR), bytesCaptor.capture());
        verify(blobWriter).commit();

        assertInstantIsCloseToNow(decodeInstant(bytesCaptor.getValue()));
    }

    @Test
    public void updateTimeToLive() throws Exception {
        String key = "key";
        cache.updateTimeToLive(key);

        ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(blobWriter).update(eq(key), eq(BlobStoreResultsCache.TTL_ATTR), bytesCaptor.capture());
        verify(blobWriter).commit();

        assertInstantIsCloseToNow(decodeInstant(bytesCaptor.getValue()));
    }

    @Test
    public void processesTtl() throws Exception {
        String keepId = "keep";
        String deleteId = "delete";

        Instant now = Instant.now();
        Map<String, byte[]> values = new HashMap<>();
        values.put(keepId, now.plusSeconds(5).toString().getBytes(BlobStoreResultsCache.BYTE_ENCODING));
        values.put(deleteId, now.minusSeconds(5).toString().getBytes(BlobStoreResultsCache.BYTE_ENCODING));

        doReturn(values).when(blobReader).getAll(BlobStoreResultsCache.TTL_ATTR);
        cache.processTimeToLive(Duration.ofSeconds(1));

        verify(blobReader).getAll(BlobStoreResultsCache.TTL_ATTR);
        verify(blobWriter, never()).delete(keepId);
        verify(blobWriter).delete(deleteId);
        verify(blobWriter).commit();
    }

    private Instant decodeInstant(byte[] instantBytes) {
        String timeStr = new String(instantBytes, BlobStoreResultsCache.BYTE_ENCODING);
        return Instant.parse(timeStr);
    }

    private void assertInstantIsCloseToNow(Instant instant) {
        long secondsAgo = Instant.now().getEpochSecond() - instant.getEpochSecond();
        assertThat(secondsAgo, lessThanOrEqualTo(1L));
    }

}
