/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.resultscache.blobstore

import com.zepben.blobstore.BlobStore
import com.zepben.blobstore.BlobStoreException
import com.zepben.resultscache.ResultsCache
import com.zepben.resultscache.ResultsCacheException
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.util.*

/**
 * Results cache backed by a blob store.
 */
class BlobStoreResultsCache(
    private val blobStore: BlobStore
) : ResultsCache {

    private var closed = false

    @Throws(ResultsCacheException::class)
    override operator fun get(key: String): ByteArray? {
        if (closed)
            throw ResultsCacheException("Results cache has been closed", null)

        try {
            return blobStore.reader[key, RESULTS_ATTR]
        } catch (e: BlobStoreException) {
            throw ResultsCacheException(e.message, e)
        }
    }

    @Throws(ResultsCacheException::class)
    override fun put(result: ByteArray): String {
        if (closed)
            throw ResultsCacheException("Results cache has been closed", null)

        try {
            val ids = blobStore.reader.ids(RESULTS_ATTR)
            var key: String
            do {
                key = String(Base64.getUrlEncoder().encode(UUID.randomUUID().toString().toByteArray()))
            } while (ids.contains(key))

            if (blobStore.writer.write(key, RESULTS_ATTR, result)) {
                blobStore.writer.commit()
                return key
            } else return ""
        } catch (e: BlobStoreException) {
            throw ResultsCacheException(e.message, e)
        }
    }

    @Throws(ResultsCacheException::class)
    override fun addTimeToLive(key: String) {
        if (closed)
            throw ResultsCacheException("Results cache has been closed", null)

        try {
            blobStore.writer.write(key, TTL_ATTR, buildTtlValue())
            blobStore.writer.commit()
        } catch (e: BlobStoreException) {
            throw ResultsCacheException(e.message, e)
        }
    }

    @Throws(ResultsCacheException::class)
    override fun updateTimeToLive(key: String) {
        if (closed)
            throw ResultsCacheException("Results cache has been closed", null)

        try {
            blobStore.writer.update(key, TTL_ATTR, buildTtlValue())
            blobStore.writer.commit()
        } catch (e: BlobStoreException) {
            throw ResultsCacheException(e.message, e)
        }
    }

    @Throws(ResultsCacheException::class)
    override fun processTimeToLive(duration: Duration) {
        if (closed)
            throw ResultsCacheException("Results cache has been closed", null)

        try {
            val now = Instant.now()
            blobStore.reader.getAll(TTL_ATTR).forEach { (id, value) ->
                if (value != null) {
                    val expireTime = Instant.parse(String(value, BYTE_ENCODING)).plus(duration)

                    if (now.isAfter(expireTime)) {
                        blobStore.writer.delete(id)
                        blobStore.writer.commit()
                    }
                }
            }
        } catch (e: BlobStoreException) {
            throw ResultsCacheException(e.message, e)
        }
    }

    @Throws(Exception::class)
    override fun close() {
        if (!closed) {
            try {
                blobStore.close()
            } catch (e: Exception) {
                throw ResultsCacheException(e.message, e)
            }
            closed = true
        }
    }

    private fun buildTtlValue(): ByteArray =
        Instant.now().toString().toByteArray(BYTE_ENCODING)

    companion object {

        val BYTE_ENCODING: Charset = StandardCharsets.UTF_8

        const val RESULTS_ATTR: String = "results"
        const val TTL_ATTR: String = "ttl"

        @Suppress("unused")
        val REQUIRED_ATTRS: Set<String> = setOf(RESULTS_ATTR, TTL_ATTR)

    }

}
