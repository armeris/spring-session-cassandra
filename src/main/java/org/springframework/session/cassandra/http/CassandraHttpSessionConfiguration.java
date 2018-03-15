package org.springframework.session.cassandra.http;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.DefaultManagedTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.session.MapSession;
import org.springframework.session.cassandra.CassandraFlushMode;
import org.springframework.session.cassandra.CassandraSessionRepository;
import org.springframework.session.config.annotation.web.http.SpringHttpSessionConfiguration;
import org.springframework.session.web.http.SessionRepositoryFilter;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Spring {@code @Configuration} class used to configure and initialize a JDBC based
 * {@code HttpSession} provider implementation in Spring Session.
 * <p>
 * Exposes the {@link SessionRepositoryFilter} as a bean named
 * {@code springSessionRepositoryFilter}. In order to use this a single {@link DataSource}
 * must be exposed as a Bean.
 *
 * @author Vedran Pavic
 * @author Eddú Meléndez
 * @since 1.2.0
 * @see EnableCassandraHttpSession
 */
@Configuration
@EnableScheduling
public class CassandraHttpSessionConfiguration extends SpringHttpSessionConfiguration
		implements EmbeddedValueResolverAware, ImportAware,
		SchedulingConfigurer {

	static final String DEFAULT_CLEANUP_CRON = "0 * * * * *";

	private Integer maxInactiveIntervalInSeconds = MapSession.DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS;

	private String tableName = CassandraSessionRepository.DEFAULT_TABLE_NAME;

	private String cleanupCron = DEFAULT_CLEANUP_CRON;

	private StringValueResolver embeddedValueResolver;

	private CassandraSessionRepository sessionRepository;

	public void setMaxInactiveIntervalInSeconds(Integer maxInactiveIntervalInSeconds) {
		this.maxInactiveIntervalInSeconds = maxInactiveIntervalInSeconds;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setCleanupCron(String cleanupCron) {
		this.cleanupCron = cleanupCron;
	}

	@Override
	public void setEmbeddedValueResolver(StringValueResolver resolver) {
		this.embeddedValueResolver = resolver;
	}

	@Override
	public void setImportMetadata(AnnotationMetadata importMetadata) {
		Map<String, Object> attributeMap = importMetadata
				.getAnnotationAttributes(EnableCassandraHttpSession.class.getName());
		AnnotationAttributes attributes = AnnotationAttributes.fromMap(attributeMap);
		if(attributes.getNumber("maxInactiveIntervalInSeconds") != null) {
			sessionRepository.setDefaultMaxInactiveInterval(attributes
					.getNumber("maxInactiveIntervalInSeconds"));
		}else{
			sessionRepository.setDefaultMaxInactiveInterval(maxInactiveIntervalInSeconds);
		}
		String tableNameValue = attributes.getString("tableName");
		if (StringUtils.hasText(tableNameValue)) {
			sessionRepository.setTableName(this.embeddedValueResolver
					.resolveStringValue(tableNameValue));
		}else{
			sessionRepository.setTableName(this.tableName);
		}
		String cleanupCron = attributes.getString("cleanupCron");
		if (StringUtils.hasText(cleanupCron)) {
			sessionRepository.setCleanupCron(cleanupCron);
		}else{
			sessionRepository.setCleanupCron(this.cleanupCron);
		}
		CassandraFlushMode cassandraFlushMode = attributes.getEnum("cassandraFlushMode");
		if (cassandraFlushMode != null) {
			sessionRepository.setCassandraFlushMode(cassandraFlushMode);
		}else{
			sessionRepository.setCassandraFlushMode(CassandraFlushMode.ON_SAVE);
		}
	}

	public TaskScheduler getTaskScheduler(){
	    return new DefaultManagedTaskScheduler();
    }

	@Bean
	public ScheduledTaskRegistrar getScheduledTaskRegistrar(){
	    return new ScheduledTaskRegistrar();
    }

    @Bean
    public CassandraSessionRepository getSessionRepository(CassandraTemplate cassandraTemplate) {
        return new CassandraSessionRepository(cassandraTemplate);
    }

	@Override
	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
		taskRegistrar.addCronTask(() -> sessionRepository.cleanUpExpiredSessions(),
				this.cleanupCron);
	}

	@Autowired
	public void setSessionRepository(CassandraSessionRepository sessionRepository){
	    this.sessionRepository = sessionRepository;
    }
}
