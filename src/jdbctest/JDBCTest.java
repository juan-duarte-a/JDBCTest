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
            jdbct.createDatabase();
        } catch (IOException | SQLException e) {
            if (e instanceof SQLException) {
                printSQLException((SQLException) e);
            } else {
                System.err.println(e.getMessage());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public JDBCTest() throws IOException {
         databaseConnector = new DatabaseConnector(PROPERTIES_FILE);
    }

    public void createDatabase() throws IOException, SQLException {

        Connection connection = databaseConnector.getConnection();
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
