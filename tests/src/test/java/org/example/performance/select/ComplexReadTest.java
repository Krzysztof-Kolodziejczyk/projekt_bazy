package org.example.performance.select;

import lombok.var;
import org.bson.Document;
import org.example.utls.TestBaseConfig;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;

public class ComplexReadTest extends TestBaseConfig {

    @Test
    public void testComplexClientOrderQuery() throws SQLException {
        var postgresQuery = "SELECT c.first_name, c.last_name, COUNT(o.order_id) AS total_orders, AVG(p.price) AS average_price " +
                "FROM clients c " +
                "JOIN orders o ON c.client_id = o.client_id " +
                "JOIN orders_products op ON o.order_id = op.order_id " +
                "JOIN products p ON op.product_id = p.product_id " +
                "GROUP BY c.first_name, c.last_name;";

        var mongoSimpleQuery = Arrays.asList(
                new Document("$lookup", new Document("from", "orders")
                        .append("localField", "client_id")
                        .append("foreignField", "client_id")
                        .append("as", "client_orders"))
                );

        var mongoEmbeddedQuery = Arrays.asList(
                new Document("$unwind", "$orders"),
                new Document("$unwind", "$orders.products"),
                new Document("$group", new Document("_id", new Document("first_name", "$first_name").append("last_name", "$last_name"))
                        .append("total_orders", new Document("$sum", 1))
                        .append("average_price", new Document("$avg", "$orders.products.price"))
                ));

        measureQueries(postgresQuery, mongoSimpleQuery, "clients", mongoEmbeddedQuery, "clients_orders", "select/complex_read/1.csv");
    }
}

