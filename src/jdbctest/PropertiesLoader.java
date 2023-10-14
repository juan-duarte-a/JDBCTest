package jdbctest;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertiesLoader {
    public static Properties loadProperties(String propertiesFile) throws IOException {
        Properties dbmsProperties = new Properties();

        try {
            dbmsProperties.loadFromXML(new FileInputStream(propertiesFile));
        } catch (IOException e) {
            System.err.println("Error reading properties file.");
            throw e;
        }

        return dbmsProperties;
    }

}
