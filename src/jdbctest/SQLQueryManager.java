package jdbctest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLQueryManager {
    
    private static void executeSelect(String query, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        
        try (ResultSet resultSet = statement.executeQuery(query)) {
            printResultSet(query, resultSet, 26);
        }
    }
    
    public static void getAllManufacturers(Connection connection) throws SQLException {
        executeSelect("SELECT * FROM fabricante", connection);
    }

    public static void getAllProducts(Connection connection) throws SQLException {
        executeSelect("SELECT * FROM producto", connection);
    }
    
    public static void getProductsFromManufacturer(
            int vendor, Connection connection) throws SQLException {
        String query = "SELECT * FROM producto WHERE codigo_fabricante = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, vendor);
        
        try (ResultSet resultSet = statement.executeQuery()) {
            printResultSet(query, resultSet, 26);
        }
    }

    public static void printResultSet(
            String query, ResultSet resultSet, int columnLength) 
            throws SQLException {
        int columns = resultSet.getMetaData().getColumnCount();
        int lineLength = (columnLength + 3) * columns;
        
        System.out.println();
        System.out.println(query);
        for (int i = 0; i < lineLength + 1; i++) {
            System.out.print("-");
        }
        System.out.println();
        
        System.out.print("| ");
        for (int i = 1; i <= columns; i++) {
            String columnName = resultSet.getMetaData().getColumnName(i);
            columnName = adjustTextLength(columnName, columnLength);
            System.out.print(columnName + " | ");
        }
        
        System.out.println();
        for (int i = 0; i < lineLength + 1; i++) {
            System.out.print("-");
        }
        System.out.println();

        while (resultSet.next()) {
            System.out.print("| ");
            for (int i = 1; i <= columns; i++) {
                String value = resultSet.getString(i);
                value = adjustTextLength(value, columnLength);
                System.out.print(value + " | ");
            }
            System.out.println();
        }

        for (int i = 0; i < lineLength + 1; i++) {
            System.out.print("-");
        }
        System.out.println();
    }

    private static String adjustTextLength(String text, int length) {
        if (text.length() < length) {
            int spaces = length - text.length();
            for (int j = 0; j < spaces; j++) {
                text += " ";
            }
        } else {
            text = text.substring(0, length);
        }

        return text;
    }

}
