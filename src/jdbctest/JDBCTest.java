package jdbctest;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.sql.PooledConnection;

public class JDBCTest {

    private static final String PROPERTIES_FILE = "./src/properties/dbms-properties.xml";
    private static final String SQL_SCRIPT = "./src/sql_scripts/tienda.sql";
    public static final String DATABASE_NAME = "tienda";
    
    private Properties dbmsProperties;
    private PooledConnection pooledConnection;
    
    public static void main(String[] args) {
        JDBCTest jdbct = new JDBCTest();
        
        try {
            jdbct.loadProperties(PROPERTIES_FILE);
            jdbct.createDatabase();
            
            if (jdbct.pooledConnection != null) {
                jdbct.pooledConnection.close();
            }
        } catch (IOException | SQLException e) {
            if (e instanceof SQLException) {
                printSQLException((SQLException) e);
            } else {
                System.err.println(e.getMessage());
            }
        }
    }
    
    public void loadProperties(String properties_file) throws IOException {
        dbmsProperties = new Properties();
        
        try {
            dbmsProperties.loadFromXML(new FileInputStream(properties_file));            
        } catch (IOException e) {
            System.err.println("Error reading properties file.");
            throw e;
        }
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
        MysqlConnectionPoolDataSource poolDataSource = new MysqlConnectionPoolDataSource();
        
        poolDataSource.setURL(connectionUrl);
        poolDataSource.setUser(dbmsProperties.getProperty("user"));
        poolDataSource.setPassword(dbmsProperties.getProperty("password"));
        
        if (pooledConnection == null) {
            try {
                pooledConnection = poolDataSource.getPooledConnection();
                System.out.println("Pooled connection to " + dbmsProperties.getProperty("dbms") + " successful.");
            } catch (SQLException e) {
                System.err.println("Error connecting to database.");
                throw e;
            }
        }
        
        return pooledConnection.getConnection();
    }
    
    public void createDatabase() throws FileNotFoundException, IOException, SQLException {
        StringBuilder sqlScript = new StringBuilder();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(SQL_SCRIPT))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sqlScript.append(line).append("\n");
            }
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + SQL_SCRIPT);
            throw e;
        } catch (IOException e) {
            System.err.println("Error reading create database SQL Script.");
            throw e;
        }
        
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        String[] queries = sqlScript.toString().split(";");
        
        for (String query : queries) {
            if (!query.isBlank()) {
                statement.addBatch(query);
            }
        }
        
        int[] results = statement.executeBatch();
        int numberOfStatements = results.length;
        
        for (int i = 0; i < numberOfStatements; i++) {
            if (results[i] == Statement.EXECUTE_FAILED) {
                System.err.println("Statement execution failed in statement number " + i);
            }
        }
        
        System.out.println("Database creation successful.");
    }
    
    private static void printSQLException(SQLException e) {
        System.err.println(e.getMessage());
        System.err.println("SQL State: " + e.getSQLState());
        System.err.println("Error code: " + e.getErrorCode());
        
        Throwable t = e.getCause();
        while (t != null) {
            System.err.println("Cause: " + t);
            t = t.getCause();
        }
    }
    
}
