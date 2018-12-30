package com.searchhandler.config;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.Getter;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.StringTokenizer;

/**
 * MyBatis Paging
 *
 * @author Owen.Yuan 2017/6/15.
 */
@Configuration
@EnableTransactionManagement
@MapperScan(basePackages = { "com.tigerobo.searchhandler.mapper" })
public class MybatisConfig {

    @Getter
    @Value("${spring.datasource.type}")
    private Class<? extends DataSource> dataSourceType;
    @Value("${mybatis.config-location}")
    private String configLocation;
    @Value("${mybatis.mapper-locations}")
    private String mapperLocations;
    @Value("${mybatis.type-aliases-package}")
    private String typeAliasesPackage;

    @Getter
    @Value("${spring.datasource.url}")
    private String url;
    @Value("${spring.datasource.username}")
    private String username;
    @Value("${spring.datasource.password}")
    private String password;
    @Value("${spring.datasource.driver-class-name}")
    private String driverClassName;
    @Value("${spring.datasource.druid.filters}")
    private String druidFilters;
    @Value("${spring.datasource.druid.max-active}")
    private int druidMaxActive;
    @Value("${spring.datasource.druid.initial-size}")
    private int druidInitialSize;
    @Value("${spring.datasource.druid.max-wait}")
    private int druidMaxWait;
    @Value("${spring.datasource.druid.min-idle}")
    private int druidMinIdle;
    @Value("${spring.datasource.druid.time-between-eviction-runs-millis}")
    private int druidTimeBetweenEvictionRunsMillis;
    @Value("${spring.datasource.druid.min-evictable-idle-time-millis}")
    private int druidMinEvictableIdleTimeMillis;
    @Value("${spring.datasource.druid.validation-query}")
    private String druidValidationQuery;

    @Value("${spring.datasource.druid.test-while-idle}")
    private boolean druidTestWhileIdle;
    @Value("${spring.datasource.druid.test-on-borrow}")
    private boolean druidTestOnBorrow;
    @Value("${spring.datasource.druid.test-on-return}")
    private boolean druidTestOnReturn;
    @Value("${spring.datasource.druid.pool-prepared-statements}")
    private boolean druidPoolPreparedStatements;
    @Value("${spring.datasource.druid.max-open-prepared-statements}")
    private int druidMaxOpenPreparedStatements;
    @Value("${spring.datasource.druid.connection-properties}")
    private String druidConnectionProperties;


    @Bean(name="dataSource")
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource dataSource() {
        DruidDataSource datasource = new DruidDataSource();
        datasource.setUrl(url);
        datasource.setUsername(username);
        datasource.setPassword(password);
        datasource.setDriverClassName(driverClassName);
        datasource.setInitialSize(druidInitialSize);
        datasource.setMinIdle(druidMinIdle);
        datasource.setMaxActive(druidMaxActive);
        datasource.setMaxWait(druidMaxWait);
        datasource.setTimeBetweenEvictionRunsMillis(druidTimeBetweenEvictionRunsMillis);
        datasource.setMinEvictableIdleTimeMillis(druidMinEvictableIdleTimeMillis);
        datasource.setValidationQuery(druidValidationQuery);
        datasource.setTestWhileIdle(druidTestWhileIdle);
        datasource.setTestOnBorrow(druidTestOnBorrow);
        datasource.setTestOnReturn(druidTestOnReturn);
        datasource.setPoolPreparedStatements(druidPoolPreparedStatements);
        datasource.setMaxOpenPreparedStatements(druidMaxOpenPreparedStatements);
        String connectionInitSqls = "SET NAMES utf8mb4";
        StringTokenizer tokenizer = new StringTokenizer(connectionInitSqls, ";");
        datasource.setConnectionInitSqls(Collections.list(tokenizer));
        //datasource.setMaxPoolPreparedStatementPerConnectionSize(druidMaxOpenPreparedStatements);
        try {
            datasource.setFilters(druidFilters);
        } catch (SQLException e) {
            System.err.println("druid configuration initialization filter: " + e);
        }
        datasource.setConnectionProperties(druidConnectionProperties);
        return datasource;
        //return DataSourceBuilder.create().type(dataSourceType).build();
    }

    @Bean(name = "sqlSessionFactory")
    public SqlSessionFactory sqlSessionFactoryBean() {
        try {
            SqlSessionFactoryBean sessionFactoryBean = new SqlSessionFactoryBean();
            sessionFactoryBean.setDataSource(dataSource());
            sessionFactoryBean.setTypeAliasesPackage(typeAliasesPackage);
            ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            sessionFactoryBean.setMapperLocations(resolver.getResources(mapperLocations));
            sessionFactoryBean.setConfigLocation(resolver.getResource(configLocation));
            return sessionFactoryBean.getObject();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    public SqlSessionTemplate sqlSessionTemplate(SqlSessionFactory sqlSessionFactory) {
        return new SqlSessionTemplate(sqlSessionFactory);
    }

    @Bean
    @Primary
    public PlatformTransactionManager transactionManager(@Qualifier("dataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}
