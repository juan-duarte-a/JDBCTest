package jdbctest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCTest {

    private static final String PROPERTIES_FILE = "./src/properties/dbms-properties.xml";
    private static final String SQL_SCRIPT = "./src/sql_scripts/tienda.sql";
    private final DatabaseConnector databaseConnector;

    public static void main(String[] args) {
        try {
            JDBCTest jdbct = new JDBCTest();
            Connection connection = jdbct.getConnection(true);
            jdbct.createDatabase(connection);
            SQLQueryManager.getAllProducts(connection);
            SQLQueryManager.getProductsFromVendorCode(2, connection);
        } catch (IOException | SQLException e) {
            if (e instanceof SQLException sqlException) {
                printSQLException(sqlException);
            } else {
                System.err.println(e.getMessage());
            }
        } 
    }
    
    public JDBCTest() throws IOException {
         databaseConnector = new DatabaseConnector(PROPERTIES_FILE);
    }
    
    public Connection getConnection(boolean showMetadata) throws SQLException {
        return databaseConnector.getConnection(showMetadata);
    }

    public void createDatabase(Connection connection) throws IOException, SQLException {
        Statement statement = connection.createStatement();
        String[] queries = SQLScriptParser.parseSQL(SQL_SCRIPT);
        
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
