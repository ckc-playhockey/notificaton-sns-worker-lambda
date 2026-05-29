package com.foobar.api.baz;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ResourceLoaderUtil {

    private static final Properties properties = new Properties();
    private static final String PROPERTIES_FILE = "/api.properties";

    static {
        try (InputStream inputStream = ResourceLoaderUtil.class.getResourceAsStream(PROPERTIES_FILE)) {
            if (inputStream == null) {
                throw new RuntimeException("api.properties not found on the classpath");
            }
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load api.properties", e);
        }
    }

    public static Properties getProperties() {
        return properties;
    }
}
