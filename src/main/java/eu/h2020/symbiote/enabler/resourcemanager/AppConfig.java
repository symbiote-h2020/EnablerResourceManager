package eu.h2020.symbiote.enabler.resourcemanager;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by vasgl on 7/18/2017.
 */
@Configuration
@EnableMongoRepositories
class AppConfig extends AbstractMongoConfiguration {

    @Value("${symbiote.enabler.rm.database}")
    private String databaseName;

    @Value("${spring.data.mongodb.host:localhost}")
    private String mongoHost;

    @Override
    protected String getDatabaseName() {
        return databaseName;
    }

    @Override
    public Mongo mongo() { return new MongoClient(); }

    @Override
    protected Collection<String> getMappingBasePackages() { return Arrays.asList("com.oreilly.springdata.mongodb"); }

    @Bean
    @Override
    public MongoTemplate mongoTemplate() {
        return new MongoTemplate(new MongoClient(mongoHost), getDatabaseName());
    }
}