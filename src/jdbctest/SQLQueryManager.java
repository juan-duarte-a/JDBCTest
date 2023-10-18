package jdbctest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLQueryManager {

    public static void getAllProducts(Connection connection) throws SQLException {
        String query = "SELECT * FROM producto";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        printResultSet(resultSet, 26);
    }
    
    public static void getProductsFromVendorCode(int vendor, Connection connection) throws SQLException {
        String query = "SELECT * FROM producto WHERE codigo_fabricante = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, vendor);
        ResultSet resultSet = statement.executeQuery();
        printResultSet(resultSet, 26);
    }

    public static void printResultSet(ResultSet resultSet, int columnLength) throws SQLException {
        int columns = resultSet.getMetaData().getColumnCount();
        int lineLength = (columnLength + 3) * columns;
        
        System.out.println();
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
