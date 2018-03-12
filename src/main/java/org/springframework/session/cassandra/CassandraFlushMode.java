package org.springframework.session.cassandra;

import org.springframework.session.SessionRepository;

public enum CassandraFlushMode {
    /**
     * Only writes to Hazelcast when
     * {@link SessionRepository#save(org.springframework.session.Session)} is invoked. In
     * a web environment this is typically done as soon as the HTTP response is committed.
     */
    ON_SAVE,

    /**
     * Writes to Hazelcast as soon as possible. For example
     * {@link SessionRepository#createSession()} will write the session to Hazelcast.
     * Another example is that setting an attribute on the session will also write to
     * Hazelcast immediately.
     */
    IMMEDIATE
}
