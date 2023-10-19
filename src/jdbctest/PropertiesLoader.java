package jdbctest;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertiesLoader {
    
    public static enum TYPE {XML, PROPERTIES};
    
    public static Properties loadProperties(String propertiesFile, TYPE type) throws IOException {
        Properties dbmsProperties = new Properties();

        try (FileInputStream inputStream = new FileInputStream(propertiesFile)) {
            switch (type) {
                case XML -> dbmsProperties.loadFromXML(inputStream);
                case PROPERTIES -> dbmsProperties.load(inputStream);
            }
        } catch (IOException e) {
            System.err.println("Error reading properties file.");
            throw e;
        }

        return dbmsProperties;
    }

}
