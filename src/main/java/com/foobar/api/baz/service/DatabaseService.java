package com.foobar.api.baz.service;

import com.amazonaws.lambda.thirdparty.com.google.gson.Gson;
import com.amazonaws.lambda.thirdparty.com.google.gson.GsonBuilder;
import com.foobar.api.baz.cofigurations.SecretManagerConfigurations;
import com.foobar.api.baz.model.SecretManagerDbCredentials;
import com.foobar.api.baz.util.ResourceLoaderUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.util.Properties;

public class DatabaseService {

    private static final Properties config = ResourceLoaderUtil.getProperties();
    private static String url = "";
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static HikariDataSource dataSource = null;
    private static SecretManagerDbCredentials secretManagerDbCredentials = null;

    public static Connection getConnection() throws Exception {
        if (dataSource == null)
            initDatasource();
        return dataSource.getConnection();
    }

    private static void initDatasource() throws Exception {

        String systemAWSRegion = System.getenv("AWS_REGION");



        SecretManagerConfigurations scm = SecretManagerConfigurations.builder()
                                          .awsRegion(systemAWSRegion != null ? systemAWSRegion : "ca-central-1")
                                          .profileName(null)
                                          .secretId(config.getProperty("secret.id"))
                                          .build();

        String secretValue = scm.getSecretValue();

        secretManagerDbCredentials = GSON.fromJson(secretValue, SecretManagerDbCredentials.class);
        url = getUrl();
        HikariConfig hconf = getHikariConfig();
        dataSource = new HikariDataSource(hconf);
    }

    private static String getUrl() {
        return String.format("jdbc:postgresql://%s:%s/%s", config.getProperty("db.host"), config.getProperty("db.port"),
                config.getProperty("db.catalog"));
    }

    private static HikariConfig getHikariConfig() {
        HikariConfig hconf = new HikariConfig();
        hconf.setUsername(secretManagerDbCredentials.getUsername());
        hconf.setPassword(secretManagerDbCredentials.getPassword());
        hconf.setJdbcUrl(url);
        hconf.setMinimumIdle(0);
        hconf.setMaximumPoolSize(100);
        hconf.setConnectionTimeout(30000);
        hconf.setIdleTimeout(1000);
        hconf.setLeakDetectionThreshold(10000);
        return hconf;
    }
}
