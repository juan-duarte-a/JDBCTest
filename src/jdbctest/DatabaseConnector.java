package jdbctest;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConnector {
    private final Properties dbmsProperties;
    private MysqlConnectionPoolDataSource poolDataSource;

    public DatabaseConnector(String propertiesFile) throws IOException {
        dbmsProperties = PropertiesLoader.loadProperties(propertiesFile);
    }

    public Connection getConnection() throws SQLException {
        String connectionUrl = "jdbc:"
                + dbmsProperties.getProperty("dbms") + "://"
                + dbmsProperties.getProperty("server") + ":"
                + dbmsProperties.getProperty("port") + "/";
        if (dbmsProperties.get("connection-type").equals("standard")) {
            return standardConnection(connectionUrl);
        } else if (dbmsProperties.get("connection-type").equals("pooled")){
            return pooledConnection(connectionUrl);
        }

        return null;
    }

    public Connection standardConnection(String connectionUrl) throws SQLException {
        Connection connection;
        try {
            connection = DriverManager.getConnection(connectionUrl, dbmsProperties);
            System.out.println("Connection to " + dbmsProperties.getProperty("dbms") + " successful.");
        } catch (SQLException e) {
            System.err.println("Error connecting to database.");
            throw e;
        }

        return connection;
    }

    public Connection pooledConnection(String connectionUrl) throws SQLException {
        if (poolDataSource == null) {
            poolDataSource = new MysqlConnectionPoolDataSource();
            poolDataSource.setURL(connectionUrl);
            poolDataSource.setUser(dbmsProperties.getProperty("user"));
            poolDataSource.setPassword(dbmsProperties.getProperty("password"));
        }

        Connection connection;
        try {
            connection = poolDataSource.getConnection();
            System.out.println("Pooled connection to " + dbmsProperties.getProperty("dbms") + " successful.");
        } catch (SQLException e) {
            System.err.println("Error connecting to database.");
            throw e;
        }

        return connection;
    }

}
