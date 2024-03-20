/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.resultscache

import java.time.Duration

/**
 * An interface that represents a cache of results.
 */
interface ResultsCache : AutoCloseable {

    /**
     * Retrieve a previously stored result.
     *
     * @param key the id previously returned by the cache for the result you are looking for.
     * @return if the id is found, the result, otherwise null.
     * @throws ResultsCacheException if there is an error with the cache.
     */
    @Throws(ResultsCacheException::class)
    operator fun get(key: String): ByteArray?

    /**
     * Store a result for later use.
     *
     * @param result the result to store.
     * @return a key that can be used to retrieve the result at a later time, or an empty string if it failed to save.
     * @throws ResultsCacheException if there is an error with the cache.
     */
    @Throws(ResultsCacheException::class)
    fun put(result: ByteArray): String

    /**
     * Add time to live processing to a result.
     *
     * @param key the id previously returned by the cache for the result.
     * @throws ResultsCacheException if there is an error with the cache.
     */
    @Throws(ResultsCacheException::class)
    fun addTimeToLive(key: String)

    /**
     * Update/replace the time to live on a result.
     *
     * @param key the id previously returned by the cache for the result.
     * @throws ResultsCacheException if there is an error with the cache.
     */
    @Throws(ResultsCacheException::class)
    fun updateTimeToLive(key: String)

    /**
     * Perform the time to live processing on all entries in the cache. Any results that have expired will be removed.
     *
     * @param duration the amount of time the result should live past it's ttl value.
     * @throws ResultsCacheException if there is an error with the cache.
     */
    @Throws(ResultsCacheException::class)
    fun processTimeToLive(duration: Duration)

}
