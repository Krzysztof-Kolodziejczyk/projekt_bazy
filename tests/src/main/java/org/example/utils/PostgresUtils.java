package org.example.utils;

import lombok.var;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostgresUtils {

    public Connection getConnection(String propertiesFile) throws SQLException {
        var config = getFromProperties(propertiesFile);
        return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
        // remember to close connection after uasage
    }

    public PostgresConfig getFromProperties(String filename) {
        try {
            Properties props = new Properties();
            props.load(getClass().getClassLoader().getResourceAsStream(filename));
            return PostgresConfig.builder()
                    .url(props.getProperty("db.url"))
                    .username(props.getProperty("db.user"))
                    .password(props.getProperty("db.password"))
                    .build();
        } catch (IOException | NullPointerException ex) {
            throw new RuntimeException(String.format("Invalid %s format", filename));
        }
    }

    public double measureQueryTimeInMilliseconds(Connection connection, String query) throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(String.format("EXPLAIN ANALYZE %s", query));
        String lastRow = "";
        while (rs.next()) {
            lastRow = rs.getString(1);
        }
        return extractExecutionTime(lastRow);
    }

    private static double extractExecutionTime(String text) {
        Pattern pattern = Pattern.compile("Execution Time: (\\d+\\.\\d+) ms");
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            String timeStr = matcher.group(1);
            return Double.parseDouble(timeStr);
        } else {
            throw new IllegalArgumentException("Could not match regexp.");
        }
    }
}
