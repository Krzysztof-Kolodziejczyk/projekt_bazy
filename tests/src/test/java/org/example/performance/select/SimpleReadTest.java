package org.example.performance.select;

import lombok.var;
import org.bson.Document;
import org.example.utls.TestBaseConfig;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class SimpleReadTest extends TestBaseConfig {

    @Test
    public void testSelectByNameFromClients() throws SQLException {
        var postgresQuery = "SELECT * FROM clients WHERE first_name='Jan';";
        var mongoSimpleQuery = new Document("first_name", "Jan");
        var mongoEmbeddedQuery = new Document("first_name", "Jan");

        measureQueries(postgresQuery, mongoSimpleQuery, "clients", mongoEmbeddedQuery, "clients_orders", "select/simple_read/1.csv");
    }

    @Test
    public void testSelectByLastNameFromClients() throws SQLException {
        var postgresQuery = "SELECT * FROM clients WHERE last_name='Kowalski';";
        var mongoSimpleQuery = new Document("last_name", "Kowalski");
        var mongoEmbeddedQuery = new Document("last_name", "Kowalski");

        measureQueries(postgresQuery, mongoSimpleQuery, "clients", mongoEmbeddedQuery, "clients_orders", "select/simple_read/2.csv");
    }

    @Test
    public void testSelectByEmailFromClients() throws SQLException {
        var postgresQuery = "SELECT * FROM clients WHERE email='jan@example.com';";
        var mongoSimpleQuery = new Document("email", "jan@example.com");
        var mongoEmbeddedQuery = new Document("email", "jan@example.com");

        measureQueries(postgresQuery, mongoSimpleQuery, "clients", mongoEmbeddedQuery, "clients_orders", "select/simple_read/3.csv");
    }

    @Test
    public void testSelectByPriceFromProducts() throws SQLException {
        var postgresQuery = "SELECT * FROM products WHERE price > 1000;";
        var mongoSimpleQuery = new Document("price", new Document("$gt", 1000));
        var mongoEmbeddedQuery = new Document("price", new Document("$gt", 1000));

        measureQueries(postgresQuery, mongoSimpleQuery, "products", mongoEmbeddedQuery, "products", "select/simple_read/4.csv");
    }

}
