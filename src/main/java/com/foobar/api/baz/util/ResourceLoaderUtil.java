package com.foobar.api.baz.util;

import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

public class ResourceLoaderUtil {

    @Getter
    private static final Properties properties = new Properties();
    private static final String PROPERTIES_FILE = "/api.properties";
    static Logger logger = Logger.getLogger(ResourceLoaderUtil.class.getName());

    static {
        try (InputStream inputStream = ResourceLoaderUtil.class.getResourceAsStream(PROPERTIES_FILE)) {
            if (inputStream == null) {
                logger.info("api.properties not found on the classpath");
            }
            properties.load(inputStream);
        } catch (IOException e) {
            logger.info("Failed to load api.properties");
        }
    }

}
