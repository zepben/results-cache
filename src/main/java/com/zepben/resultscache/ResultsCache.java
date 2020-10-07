/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.resultscache;

import com.zepben.annotations.EverythingIsNonnullByDefault;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * An interface that represents a cache of results.
 */
@EverythingIsNonnullByDefault
public interface ResultsCache extends AutoCloseable {

    /**
     * Retrieve a previously stored result.
     *
     * @param key the id previously returned by the cache for the result you are looking for.
     * @return if the id is found, the result, otherwise null.
     * @throws ResultsCacheException if there is an error with the cache.
     */
    @Nullable
    byte[] get(String key) throws ResultsCacheException;

    /**
     * Store a result for later use.
     *
     * @param result the result to store.
     * @return a key that can be used to retrieve the result at a later time, or an empty string if it failed to save.
     * @throws ResultsCacheException if there is an error with the cache.
     */
    String put(byte[] result) throws ResultsCacheException;

    /**
     * Add time to live processing to a result.
     *
     * @param key the id previously returned by the cache for the result.
     * @throws ResultsCacheException if there is an error with the cache.
     */
    void addTimeToLive(String key) throws ResultsCacheException;

    /**
     * Update/replace the time to live on a result.
     *
     * @param key the id previously returned by the cache for the result.
     * @throws ResultsCacheException if there is an error with the cache.
     */
    void updateTimeToLive(String key) throws ResultsCacheException;

    /**
     * Perform the time to live processing on all entries in the cache. Any results that have expired will be removed.
     *
     * @param duration the amount of time the result should live past it's ttl value.
     * @throws ResultsCacheException if there is an error with the cache.
     */
    void processTimeToLive(Duration duration) throws ResultsCacheException;

}
