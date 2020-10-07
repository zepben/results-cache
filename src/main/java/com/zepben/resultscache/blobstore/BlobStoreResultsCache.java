/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.resultscache.blobstore;

import com.zepben.annotations.EverythingIsNonnullByDefault;
import com.zepben.blobstore.BlobStore;
import com.zepben.blobstore.BlobStoreException;
import com.zepben.resultscache.ResultsCache;
import com.zepben.resultscache.ResultsCacheException;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Results cache backed by a blob store.
 */
@SuppressWarnings("WeakerAccess")
@EverythingIsNonnullByDefault
public class BlobStoreResultsCache implements ResultsCache {

    static final Charset BYTE_ENCODING = StandardCharsets.UTF_8;

    private static final String RESULTS_ATTR = "results";
    static final String TTL_ATTR = "ttl";

    public static final Set<String> REQUIRED_ATTRS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(RESULTS_ATTR, TTL_ATTR)));

    @Nullable
    private BlobStore blobStore;

    public BlobStoreResultsCache(BlobStore blobStore) {
        this.blobStore = blobStore;
    }

    @Nullable
    @Override
    public byte[] get(String key) throws ResultsCacheException {
        if (blobStore == null)
            throw new ResultsCacheException("Results cache has been closed", null);

        try {
            return blobStore.reader().get(key, RESULTS_ATTR);
        } catch (BlobStoreException e) {
            throw new ResultsCacheException(e.getMessage(), e);
        }
    }

    @Override
    public String put(byte[] result) throws ResultsCacheException {
        if (blobStore == null)
            throw new ResultsCacheException("Results cache has been closed", null);

        try {

            Set<String> ids = blobStore.reader().ids(RESULTS_ATTR);
            String key;
            do {
                key = new String(Base64.getUrlEncoder().encode(UUID.randomUUID().toString().getBytes()));
            } while (ids.contains(key));

            if (blobStore.writer().write(key, RESULTS_ATTR, result)) {
                blobStore.writer().commit();
                return key;
            } else
                return "";
        } catch (BlobStoreException e) {
            throw new ResultsCacheException(e.getMessage(), e);
        }
    }

    @Override
    public void addTimeToLive(String key) throws ResultsCacheException {
        if (blobStore == null)
            throw new ResultsCacheException("Results cache has been closed", null);

        try {
            blobStore.writer().write(key, TTL_ATTR, buildTtlValue());
            blobStore.writer().commit();
        } catch (BlobStoreException e) {
            throw new ResultsCacheException(e.getMessage(), e);
        }
    }

    @Override
    public void updateTimeToLive(String key) throws ResultsCacheException {
        if (blobStore == null)
            throw new ResultsCacheException("Results cache has been closed", null);

        try {
            blobStore.writer().update(key, TTL_ATTR, buildTtlValue());
            blobStore.writer().commit();
        } catch (BlobStoreException e) {
            throw new ResultsCacheException(e.getMessage(), e);
        }
    }

    @Override
    public void processTimeToLive(Duration duration) throws ResultsCacheException {
        if (blobStore == null)
            throw new ResultsCacheException("Results cache has been closed", null);

        try {
            Instant now = Instant.now();
            for (Map.Entry<String, byte[]> entry : blobStore.reader().getAll(TTL_ATTR).entrySet()) {
                String id = entry.getKey();
                byte[] value = entry.getValue();

                if (value != null) {
                    Instant expireTime = Instant.parse(new String(value, BYTE_ENCODING)).plus(duration);

                    if (now.isAfter(expireTime)) {
                        blobStore.writer().delete(id);
                        blobStore.writer().commit();
                    }
                }
            }
        } catch (BlobStoreException e) {
            throw new ResultsCacheException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        if (blobStore != null) {
            try {
                blobStore.close();
            } catch (Exception e) {
                throw new ResultsCacheException(e.getMessage(), e);
            }
            blobStore = null;
        }
    }

    private byte[] buildTtlValue() {
        return Instant.now().toString().getBytes(BYTE_ENCODING);
    }

}
