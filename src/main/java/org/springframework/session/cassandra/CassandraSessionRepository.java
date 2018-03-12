package org.springframework.session.cassandra;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Repository
public class CassandraSessionRepository implements
        FindByIndexNameSessionRepository<CassandraSessionRepository.CassandraSession> {

    /**
     * The default name of database table used by Spring Session to store sessions.
     */
    public static final String DEFAULT_TABLE_NAME = "cassandra_sessions";

    private static final String SPRING_SECURITY_CONTEXT = "SPRING_SECURITY_CONTEXT";

    private static final Log logger = LogFactory
            .getLog(CassandraSessionRepository.class);

    private static final PrincipalNameResolver PRINCIPAL_NAME_RESOLVER = new PrincipalNameResolver();

    private CassandraTemplate cassandraTemplate;

    private CassandraFlushMode cassandraFlushMode = CassandraFlushMode.ON_SAVE;

    /**
     * The name of database table used by Spring Session to store sessions.
     */
    private String tableName = DEFAULT_TABLE_NAME;

    /**
     * If non-null, this value is used to override the default value for
     * {@link CassandraSession#setMaxInactiveInterval(Duration)}.
     */
    private Integer defaultMaxInactiveInterval;

    /**
     * Set the name of database table used to store sessions.
     * @param tableName the database table name
     */
    public void setTableName(String tableName) {
        Assert.hasText(tableName, "Table name must not be empty");
        this.tableName = tableName.trim();
    }

    @Override
    public CassandraSession createSession() {
        CassandraSession session = new CassandraSession();
        if (this.defaultMaxInactiveInterval != null) {
            session.setMaxInactiveInterval(Duration.ofSeconds(this.defaultMaxInactiveInterval));
        }
        return session;
    }

    @Override
    public void save(final CassandraSession session) {
        if (session.isNew()) {
            cassandraTemplate.insert(session);
        } else {
            cassandraTemplate.update(session);
        }
        session.clearChangeFlags();
    }

    @Override
    public CassandraSession findById(final String id) {
        Select findById = QueryBuilder.select().from(tableName).where(QueryBuilder.eq("id", id)).limit(1);
        final CassandraSession session = cassandraTemplate.selectOne(findById, CassandraSession.class);

        if (session != null) {
            if (session.isExpired()) {
                deleteById(id);
            }
            else {
                return session;
            }
        }
        return null;
    }

    @Override
    public void deleteById(final String id) {
        cassandraTemplate.deleteById(id, CassandraSession.class);
    }

    @Override
    public Map<String, CassandraSession> findByIndexNameAndIndexValue(String indexName,
                                                                 final String indexValue) {
        if (!PRINCIPAL_NAME_INDEX_NAME.equals(indexName)) {
            return Collections.emptyMap();
        }

        Select findByIndexName = QueryBuilder.select().from(tableName).where(QueryBuilder.eq(indexName, indexValue)).orderBy(QueryBuilder.asc("id"));

        List<CassandraSession> sessions = cassandraTemplate.select(findByIndexName, CassandraSession.class);

        Map<String, CassandraSession> sessionMap = new HashMap<>(
                sessions.size());

        for (CassandraSession session : sessions) {
            sessionMap.put(session.getId(), session);
        }

        return sessionMap;
    }

    public void cleanUpExpiredSessions() {
        Select deleteExpiredSessions = QueryBuilder.select().from(tableName).where(QueryBuilder.lt("expiry_time", System.currentTimeMillis())).orderBy(QueryBuilder.asc("id"));
        List<CassandraSession> sessions = cassandraTemplate.select(deleteExpiredSessions, CassandraSession.class);
        cassandraTemplate.delete(sessions);

        if (logger.isDebugEnabled()) {
            logger.debug("Cleaned up expired sessions");
        }
    }

    /**
     * Serialize object as base64 encoded String
     *
     * @param _object The object to serialize
     * @return The serialized object
     */
    private String serialize(Object _object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(_object);
            oos.close();
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Deserialize base64 encoded object
     *
     * @param _serialized The object serialized
     * @return The deserialized object
     */
    private Object deserialize(String _serialized) {
        if (_serialized == null) {
            return null;
        }
        try {
            return new ObjectInputStream(
                    new ByteArrayInputStream(
                            Base64.getDecoder().decode(_serialized)
                    )
            ).readObject();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    final class CassandraSession implements Session {

        private final Session delegate;

        private final String primaryKey;

        private boolean isNew;

        private boolean changed;

        private Map<String, Object> delta = new HashMap<>();

        CassandraSession() {
            this.delegate = new MapSession();
            this.isNew = true;
            this.primaryKey = UUID.randomUUID().toString();
            flushImmediateIfNecessary();
        }

        CassandraSession(String primaryKey, Session delegate) {
            Assert.notNull(primaryKey, "primaryKey cannot be null");
            Assert.notNull(delegate, "Session cannot be null");
            this.primaryKey = primaryKey;
            this.delegate = delegate;
            flushImmediateIfNecessary();
        }

        boolean isNew() {
            return this.isNew;
        }

        boolean isChanged() {
            return this.changed;
        }

        Map<String, Object> getDelta() {
            return this.delta;
        }

        void clearChangeFlags() {
            this.isNew = false;
            this.changed = false;
            this.delta.clear();
            flushImmediateIfNecessary();
        }

        String getPrincipalName() {
            return PRINCIPAL_NAME_RESOLVER.resolvePrincipal(this);
        }

        Instant getExpiryTime() {
            return getLastAccessedTime().plus(getMaxInactiveInterval());
        }

        @Override
        public String getId() {
            return this.delegate.getId();
        }

        @Override
        public String changeSessionId() {
            this.changed = true;
            return this.delegate.changeSessionId();
        }

        @Override
        public <T> T getAttribute(String attributeName) {
            return this.delegate.getAttribute(attributeName);
        }

        @Override
        public Set<String> getAttributeNames() {
            return this.delegate.getAttributeNames();
        }

        @Override
        public void setAttribute(String attributeName, Object attributeValue) {
            this.delegate.setAttribute(attributeName, attributeValue);
            this.delta.put(attributeName, attributeValue);
            if (PRINCIPAL_NAME_INDEX_NAME.equals(attributeName) ||
                    SPRING_SECURITY_CONTEXT.equals(attributeName)) {
                this.changed = true;
            }
            flushImmediateIfNecessary();
        }

        @Override
        public void removeAttribute(String attributeName) {
            this.delegate.removeAttribute(attributeName);
            this.delta.put(attributeName, null);
            flushImmediateIfNecessary();
        }

        @Override
        public Instant getCreationTime() {
            return this.delegate.getCreationTime();
        }

        @Override
        public void setLastAccessedTime(Instant lastAccessedTime) {
            this.delegate.setLastAccessedTime(lastAccessedTime);
            this.changed = true;
            flushImmediateIfNecessary();
        }

        @Override
        public Instant getLastAccessedTime() {
            return this.delegate.getLastAccessedTime();
        }

        @Override
        public void setMaxInactiveInterval(Duration interval) {
            this.delegate.setMaxInactiveInterval(interval);
            this.changed = true;
            flushImmediateIfNecessary();
        }

        @Override
        public Duration getMaxInactiveInterval() {
            return this.delegate.getMaxInactiveInterval();
        }

        @Override
        public boolean isExpired() {
            return this.delegate.isExpired();
        }

        private void flushImmediateIfNecessary() {
            if (CassandraSessionRepository.this.cassandraFlushMode == CassandraFlushMode.IMMEDIATE) {
                CassandraSessionRepository.this.save(this);
            }
        }

    }

    /**
     * Resolves the Spring Security principal name.
     *
     * @author Vedran Pavic
     */
    static class PrincipalNameResolver {

        private SpelExpressionParser parser = new SpelExpressionParser();

        public String resolvePrincipal(Session session) {
            String principalName = session.getAttribute(PRINCIPAL_NAME_INDEX_NAME);
            if (principalName != null) {
                return principalName;
            }
            Object authentication = session.getAttribute(SPRING_SECURITY_CONTEXT);
            if (authentication != null) {
                Expression expression = this.parser
                        .parseExpression("authentication?.name");
                return expression.getValue(authentication, String.class);
            }
            return null;
        }

    }

    public CassandraTemplate getCassandraTemplate() {
        return cassandraTemplate;
    }

    @Autowired
    public void setCassandraTemplate(CassandraTemplate cassandraTemplate) {
        this.cassandraTemplate = cassandraTemplate;
    }

    public Integer getDefaultMaxInactiveInterval() {
        return defaultMaxInactiveInterval;
    }

    public void setDefaultMaxInactiveInterval(Integer defaultMaxInactiveInterval) {
        this.defaultMaxInactiveInterval = defaultMaxInactiveInterval;
    }

    public CassandraFlushMode getCassandraFlushMode() {
        return cassandraFlushMode;
    }

    public void setCassandraFlushMode(CassandraFlushMode cassandraFlushMode) {
        this.cassandraFlushMode = cassandraFlushMode;
    }

}
