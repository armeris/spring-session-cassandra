package org.springframework.session.cassandra;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.session.ExpiringSession;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.Session;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

    private CassandraTemplate cassandraTemplate;

    private CassandraFlushMode cassandraFlushMode;

    /**
     * The name of database table used by Spring Session to store sessions.
     */
    private String tableName = DEFAULT_TABLE_NAME;

    public String getCleanupCron() {
        return cleanupCron;
    }

    public void setCleanupCron(String cleanupCron) {
        this.cleanupCron = cleanupCron;
    }

    private String cleanupCron;

    /**
     * If non-null, this value is used to override the default value for
     * {@link CassandraSession#setMaxInactiveIntervalInSeconds(int)}.
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

    public CassandraSessionRepository(CassandraTemplate cassandraTemplate){
        this.cassandraTemplate = cassandraTemplate;
        this.cassandraFlushMode = CassandraFlushMode.ON_SAVE;
    }

    @Override
    public CassandraSession createSession() {
        CassandraSession session = new CassandraSession();
        if (this.defaultMaxInactiveInterval != null) {
            session.setMaxInactiveIntervalInSeconds(this.defaultMaxInactiveInterval);
        }
        return session;
    }

    @Override
    public void save(final CassandraSession session) {
        if (session.isNew()) {
            cassandraTemplate.execute(CassandraTemplate.createInsertQuery(tableName, session, null, cassandraTemplate.getConverter()));
        } else {
            cassandraTemplate.execute(CassandraTemplate.createUpdateQuery(tableName, session, null, cassandraTemplate.getConverter()));
        }
        session.clearChangeFlags();
    }

    @Override
    public CassandraSession getSession(String id) {
        Select findById = QueryBuilder.select().from(tableName).where(QueryBuilder.eq("id", id)).limit(1);
        final CassandraSession session = cassandraTemplate.selectOne(findById, CassandraSession.class);

        if (session != null) {
            if (session.isExpired()) {
                delete(id);
            }
            else {
                return session;
            }
        }
        return null;
    }

    @Override
    public void delete(String id) {
        cassandraTemplate.deleteById(CassandraSession.class, id);
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
        Select deleteExpiredSessions = QueryBuilder.select("id").from(tableName).where(QueryBuilder.lt("expiryTime", System.currentTimeMillis())).orderBy(QueryBuilder.desc("lastTimeAccessed")).allowFiltering();
        List<CassandraSession> sessions = cassandraTemplate.select(deleteExpiredSessions, CassandraSession.class);
        cassandraTemplate.delete(CassandraTemplate.createDeleteQuery(tableName, sessions, null, cassandraTemplate.getConverter()));

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

    final class CassandraSession implements ExpiringSession {

        private final String id;

        private boolean isNew;

        private boolean changed;

        private Long creationTime;

        private Long lastAccessedTime;

        private int maxIntervalInSeconds;

        private Map<String, String> data = new HashMap<>();


        CassandraSession() {
            this.isNew = true;
            this.creationTime = System.currentTimeMillis();
            this.id = UUID.randomUUID().toString();
            this.changed = false;
            flushImmediateIfNecessary();
        }

        CassandraSession(String id) {
            Assert.notNull(id, "id cannot be null");
            this.creationTime = System.currentTimeMillis();
            this.id = id;
            this.changed = false;
            flushImmediateIfNecessary();
        }

        boolean isNew() {
            return this.isNew;
        }

        boolean isChanged() {
            return this.changed;
        }

        void clearChangeFlags() {
            this.isNew = false;
            this.changed = false;
        }

        @Override
        public String getId() {
            return null;
        }

        @Override
        public <T> T getAttribute(String attributeName) {
            return (T) deserialize(this.data.get(attributeName));
        }

        @Override
        public Set<String> getAttributeNames() {
            Set<String> result = new HashSet<>();

            for(String key : this.data.keySet()){
                result.add(key);
            }

            return result;
        }

        @Override
        public void setAttribute(String attributeName, Object attributeValue) {
            if (PRINCIPAL_NAME_INDEX_NAME.equals(attributeName) ||
                    SPRING_SECURITY_CONTEXT.equals(attributeName)) {
                this.changed = true;
            }
            data.put(attributeName, serialize(attributeValue));
            flushImmediateIfNecessary();
        }

        @Override
        public void removeAttribute(String attributeName) {
            this.data.remove(attributeName);
        }

        @Override
        public long getCreationTime() {
            return creationTime;
        }

        @Override
        public void setLastAccessedTime(long lastAccessedTime) {
            this.lastAccessedTime = lastAccessedTime;
        }

        @Override
        public long getLastAccessedTime() {
            return lastAccessedTime;
        }

        @Override
        public void setMaxInactiveIntervalInSeconds(int interval) {
            this.maxIntervalInSeconds = interval;
        }

        @Override
        public int getMaxInactiveIntervalInSeconds() {
            return maxIntervalInSeconds;
        }

        @Override
        public boolean isExpired() {
            return false;
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
