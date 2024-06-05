package org.example.performance.delete;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.example.utls.TestBaseConfig;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class ComplexDeleteTest extends TestBaseConfig {

//    @Test
//    public void deleteInactiveClientsFromPostgresAndMongo() throws SQLException {
//        // SQL do usunięcia klientów, którzy nie złożyli zamówienia w ciągu ostatniego roku
//        String postgresQuery = "DELETE FROM clients WHERE client_id NOT IN (" +
//                "SELECT client_id FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '1 year'" +
//                ");";
//
//        // MongoDB query - usuń klientów, którzy nie złożyli zamówienia w ostatnim roku
//        Bson mongoFilter = new Document("last_order_date", new Document("$lt", new java.util.Date(System.currentTimeMillis() - 365L * 24 * 3600 * 1000)));
//
//        measureDeletes(postgresQuery, mongoFilter, "clients", mongoFilter, "clients_nested", "deletes/inactive_clients.csv");
//    }
}
