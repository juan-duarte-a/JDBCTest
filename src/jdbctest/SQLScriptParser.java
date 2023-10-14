package jdbctest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class SQLScriptParser {

    public static String[] parseSQL(String sqlScriptFile) throws IOException {
        StringBuilder sqlScript = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(sqlScriptFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sqlScript.append(line).append("\n");
            }
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + sqlScriptFile);
            throw e;
        } catch (IOException e) {
            System.err.println("Error reading create database SQL Script.");
            throw e;
        }

        return sqlScript.toString().split(";");
    }

}
