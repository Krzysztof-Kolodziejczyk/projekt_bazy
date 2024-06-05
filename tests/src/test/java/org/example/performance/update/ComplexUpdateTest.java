package org.example.performance.update;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.example.utls.TestBaseConfig;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

public class ComplexUpdateTest extends TestBaseConfig {

    @Test
    public void complexUpdateClientsInPostgresAndMongo() throws SQLException {
        String postgresQuery = "UPDATE clients SET email = CONCAT(first_name, '.', last_name, '@example.com') WHERE client_id > 5;";

        Bson mongoSimpleQuery = Filters.gt("client_id", 5);
        Bson mongoSimpleUpdate = Updates.set("email", new Document("$concat", List.of("$first_name", ".", "$last_name", "@example.com")));

        Bson mongoNestedQuery = Filters.gt("client_id", 5);
        Bson mongoNestedUpdate = Updates.set("email", new Document("$concat", List.of("$first_name", ".", "$last_name", "@example.com")));

        measureUpdates(postgresQuery, mongoSimpleQuery, mongoSimpleUpdate, "clients", mongoNestedQuery, mongoNestedUpdate, "clients_nested", "updates/complex_update.csv");
    }
}
