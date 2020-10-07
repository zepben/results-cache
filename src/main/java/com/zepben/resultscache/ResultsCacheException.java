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

/**
 * Exception that can be thrown when things fail in the results cache.
 */
@EverythingIsNonnullByDefault
public class ResultsCacheException extends Exception {

    public ResultsCacheException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

}
