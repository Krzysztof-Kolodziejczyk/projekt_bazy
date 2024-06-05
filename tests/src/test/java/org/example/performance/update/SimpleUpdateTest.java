package org.example.performance.update;

import lombok.var;
import org.bson.Document;
import org.example.utls.TestBaseConfig;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

public class SimpleUpdateTest extends TestBaseConfig {

    @Test
    public void updateClientInPostgresAndMongo() throws SQLException {
        // Zapytanie SQL dla PostgreSQL
        var postgresQuery = "UPDATE clients SET email = 'updated@example.com' WHERE client_id = 1;";

        // Dokument dla MongoDB (pierwsza instancja)
        var mongoSimpleQuery = new Document("client_id", 1);
        var mongoSimpleUpdate = new Document("$set", new Document("email", "updated@example.com"));

        // Dokument dla MongoDB (druga instancja, zagnieżdżona)
        var mongoNestedQuery = new Document("client_id", 1);
        var mongoNestedUpdate = new Document("$set", new Document("email", "updated@example.com"));

        // Wywołanie funkcji pomiarowej dla obu baz danych
        measureUpdates(postgresQuery, mongoSimpleQuery, mongoSimpleUpdate, "clients", mongoNestedQuery, mongoNestedUpdate, "clients_nested", "updates/single_update.csv");
    }

    @Test
    public void updateMultipleClientsInPostgresAndMongo() throws SQLException {
        // Aktualizacja wielu klientów w PostgreSQL
        var postgresQuery = "UPDATE clients SET address = 'New Address' WHERE client_id IN (2, 3);";

        // Aktualizacja dla MongoDB (pierwsza instancja)
        var mongoSimpleQuery = new Document("client_id", new Document("$in", List.of(2, 3)));
        var mongoSimpleUpdate = new Document("$set", new Document("address", "New Address"));

        // Aktualizacja dla MongoDB (druga instancja, zagnieżdżona)
        var mongoNestedQuery = new Document("client_id", new Document("$in", List.of(2, 3)));
        var mongoNestedUpdate = new Document("$set", new Document("address", "New Address"));

        // Wywołanie funkcji pomiarowej dla obu baz danych
        measureUpdates(postgresQuery, mongoSimpleQuery, mongoSimpleUpdate, "clients", mongoNestedQuery, mongoNestedUpdate, "clients_nested", "updates/multiple_updates.csv");
    }


}
