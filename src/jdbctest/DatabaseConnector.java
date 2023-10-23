package jdbctest;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConnector {
    private final Properties dbmsProperties;
    private MysqlConnectionPoolDataSource poolDataSource;

    public DatabaseConnector(String propertiesFile, PropertiesLoader.TYPE type) throws IOException {
        dbmsProperties = PropertiesLoader.loadProperties(propertiesFile, type);
    }

    public Connection getConnection(boolean showMetadata) throws SQLException {
        var connection = connect();
        
        if (showMetadata) {
            showConnectionMetadata(connection);
        }
        
        connection.setCatalog(dbmsProperties.getProperty("database"));
        return connection;
    }
    
    public Connection connect() throws SQLException {
        Connection connection = null;
        
        String connectionUrl = "jdbc:"
                + dbmsProperties.getProperty("dbms") + "://"
                + dbmsProperties.getProperty("server") + ":"
                + dbmsProperties.getProperty("port") + "/";
        if (dbmsProperties.get("connection-type").equals("standard")) {
            connection = standardConnection(connectionUrl);
        } else if (dbmsProperties.get("connection-type").equals("pooled")){
            connection = pooledConnection(connectionUrl);
        }
        
        return connection;
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
    
    public void showConnectionMetadata(Connection connection) throws SQLException {
        DatabaseMetaData dbMetaData = connection.getMetaData();
        
        System.out.println();
        System.out.println("DBMS supports TYPE_FORWARD_ONLY ResulSet: "
                + dbMetaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
        System.out.println("DBMS supports TYPE_SCROLL_INSENSITIVE ResulSet: "
                + dbMetaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
        System.out.println("DBMS supports TYPE_SCROLL_SENSITIVE ResulSet: "
                + dbMetaData.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
        System.out.println("DBMS supports CONCUR_READ_ONLY in TYPE_SCROLL_INSENSITIVE ResulSet: "
                + dbMetaData.supportsResultSetConcurrency(
                        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
        System.out.println("DBMS supports CONCUR_UPDATABLE in TYPE_SCROLL_INSENSITIVE ResulSet: "
                + dbMetaData.supportsResultSetConcurrency(
                        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE));
        System.out.println();
    }
    
    public boolean databaseExists() throws SQLException {
        try (var connection = connect()) {
            connection.setCatalog(dbmsProperties.getProperty("database"));
        } catch (SQLException e) {
            if (e.getErrorCode() == 1049) {
                return false;
            } else {
                throw e;
            }
        }
        
        return true;
    }

}
