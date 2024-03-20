/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.resultscache.blobstore

import com.zepben.blobstore.BlobReader
import com.zepben.blobstore.BlobStore
import com.zepben.blobstore.BlobWriter
import com.zepben.resultscache.ResultsCacheException
import com.zepben.resultscache.blobstore.BlobStoreResultsCache.Companion.BYTE_ENCODING
import com.zepben.testutils.exception.ExpectException.Companion.expect
import io.mockk.*
import org.hamcrest.MatcherAssert.*
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import java.util.*

class BlobStoreResultsCacheTest {

    private val blobReader = mockk<BlobReader>(relaxed = true)
    private val blobWriter = mockk<BlobWriter>(relaxed = true)

    private val blobStore = mockk<BlobStore> {
        every { reader() } returns blobReader
        every { writer() } returns blobWriter
        justRun { close() }
    }

    private val cache = BlobStoreResultsCache(blobStore)

    @Test
    fun works() {
        val values = mutableMapOf<String, ByteArray>()
        var putCount = 0

        every { blobReader[any<String>(), any()] } answers { values[firstArg()] }
        every { blobWriter.write(any(), any(), any()) } answers {
            values[firstArg()] = thirdArg()
            ++putCount
            true
        }

        val random = Random()
        for (i in 0 until NUM_VALUES_TO_ADD) {
            val value = BigInteger(160, random).toString(32).toByteArray(BYTE_ENCODING)
            assertThat(value, notNullValue())
            assertThat(value.size, greaterThan(0))

            val key = cache.put(value)
            assertThat(key, notNullValue())
            assertThat(key, not(equalTo("")))
            assertThat(cache[key], equalTo(value))
        }

        assertThat(values.size, equalTo(NUM_VALUES_TO_ADD))

        verify(exactly = putCount) { blobWriter.commit() }

        cache.close()

        expect { cache["key"] }.toThrow<ResultsCacheException>().withMessage("Results cache has been closed")
        expect { cache.put("value".toByteArray(BYTE_ENCODING)) }.toThrow<ResultsCacheException>().withMessage("Results cache has been closed")
        expect { cache.addTimeToLive("key") }.toThrow<ResultsCacheException>().withMessage("Results cache has been closed")
        expect { cache.updateTimeToLive("key") }.toThrow<ResultsCacheException>().withMessage("Results cache has been closed")
        expect { cache.processTimeToLive(Duration.ZERO) }.toThrow<ResultsCacheException>().withMessage("Results cache has been closed")

        // Make sure we can call close again without an exception.
        cache.close()

        // Make sure we only closed the store once.
        verify(exactly = 1) { blobStore.close() }
    }

    @Test
    @Throws(Exception::class)
    fun addTimeToLive() {
        val key = "key"
        cache.addTimeToLive(key)

        val bytesCaptor = slot<ByteArray>()
        verify { blobWriter.write(eq(key), eq(BlobStoreResultsCache.TTL_ATTR), capture(bytesCaptor)) }
        verify { blobWriter.commit() }

        assertInstantIsCloseToNow(decodeInstant(bytesCaptor.captured))
    }

    @Test
    @Throws(Exception::class)
    fun updateTimeToLive() {
        val key = "key"
        cache.updateTimeToLive(key)

        val bytesCaptor = slot<ByteArray>()
        verify { blobWriter.update(eq(key), eq(BlobStoreResultsCache.TTL_ATTR), capture(bytesCaptor)) }
        verify { blobWriter.commit() }

        assertInstantIsCloseToNow(decodeInstant(bytesCaptor.captured))
    }

    @Test
    @Throws(Exception::class)
    fun processesTtl() {
        val keepId = "keep"
        val deleteId = "delete"

        val now = Instant.now()
        val values: MutableMap<String, ByteArray> = HashMap()
        values[keepId] = now.plusSeconds(5).toString().toByteArray(BYTE_ENCODING)
        values[deleteId] = now.minusSeconds(5).toString().toByteArray(BYTE_ENCODING)

        every { blobReader.getAll(BlobStoreResultsCache.TTL_ATTR) } returns values
        cache.processTimeToLive(Duration.ofSeconds(1))

        verify { blobReader.getAll(BlobStoreResultsCache.TTL_ATTR) }
        verify(exactly = 0) { blobWriter.delete(keepId) }
        verify { blobWriter.delete(deleteId) }
        verify { blobWriter.commit() }
    }

    private fun decodeInstant(instantBytes: ByteArray): Instant {
        val timeStr = String(instantBytes, BYTE_ENCODING)
        return Instant.parse(timeStr)
    }

    private fun assertInstantIsCloseToNow(instant: Instant) {
        val secondsAgo = Instant.now().epochSecond - instant.epochSecond
        assertThat(secondsAgo, lessThanOrEqualTo(1L))
    }

    companion object {

        private const val NUM_VALUES_TO_ADD = 1000

    }

}
