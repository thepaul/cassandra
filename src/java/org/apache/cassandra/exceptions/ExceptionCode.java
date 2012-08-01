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
package org.apache.cassandra.exceptions;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.transport.ProtocolException;

/**
 * Exceptions code, as defined by the binary protocol.
 */
public enum ExceptionCode
{
    SERVER_ERROR      (0),
    PROTOCOL_ERROR   (10),

    // 1xx: problem during request execution
    UNAVAILABLE     (100),
    OVERLOADED      (101),
    IS_BOOTSTRAPPING(102),
    TRUNCATE_ERROR  (103),
    WRITE_TIMEOUT   (110),
    READ_TIMEOUT    (120),

    // 2xx: problem validating the request
    SYNTAX_ERROR    (200),
    UNAUTHORIZED    (210),
    INVALID         (220),
    CONFIG_ERROR    (230),
    ALREADY_EXISTS  (240);

    public final int value;
    private static final Map<Integer, ExceptionCode> valueToCode = new HashMap<Integer, ExceptionCode>(ExceptionCode.values().length);
    static
    {
        for (ExceptionCode code : ExceptionCode.values())
            valueToCode.put(code.value, code);
    }

    private ExceptionCode(int value)
    {
        this.value = value;
    }

    public static ExceptionCode fromValue(int value)
    {
        ExceptionCode code = valueToCode.get(value);
        if (code == null)
            throw new ProtocolException(String.format("Unknown error code %d", value));
        return code;
    }
}
