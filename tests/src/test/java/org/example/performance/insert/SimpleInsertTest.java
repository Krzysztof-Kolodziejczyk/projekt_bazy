package org.example.performance.insert;

import lombok.var;
import org.bson.Document;
import org.example.utls.TestBaseConfig;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

public class SimpleInsertTest extends TestBaseConfig {

    @Test
    public void insertSingleClientIntoPostgresAndMongo() throws SQLException {
        // Zapytanie SQL dla PostgreSQL
        var postgresQuery = "INSERT INTO clients (first_name, last_name, email, address, phone) VALUES ('Jan', 'Nowak', 'jan@example.com', 'Warszawa, ul. Mokotowska 17', '500600700');";

        // Dokument dla pierwszej instancji MongoDB
        var mongoDocument = new Document("first_name", "Jan")
                .append("last_name", "Nowak")
                .append("email", "jan@example.com")
                .append("address", "Warszawa, ul. Mokotowska 17")
                .append("phone", "500600700");

        // Dokument dla drugiej instancji MongoDB (zagnieżdżone struktury)
        var mongoNestedDocument = new Document("first_name", "Jan")
                .append("last_name", "Nowak")
                .append("email", "jan@example.com")
                .append("address", "Warszawa, ul. Mokotowska 17")
                .append("phone", "500600700")
                .append("orders", List.of()); // Przykład zagnieżdżenia, dodaj rzeczywiste dane w zależności od struktury

        // Wywołanie funkcji pomiarowej dla obu baz danych i obu instancji MongoDB
        measureBatchInserts(postgresQuery, List.of(mongoDocument), List.of(mongoNestedDocument), "clients", "clients_nested", "inserts/single_insert.csv");
    }

    @Test
    public void insertMultipleClientsIntoPostgresAndMongo() throws SQLException {
        // Wstawianie wielu klientów do PostgreSQL
        var postgresQuery = "INSERT INTO clients (first_name, last_name, email, address, phone) VALUES " +
                "('Anna', 'Kowalska', 'anna@example.com', 'Kraków, ul. Wielicka 44', '600700800'), " +
                "('Piotr', 'Wiśniewski', 'piotr@example.com', 'Gdańsk, ul. Długa 5', '700800900');";

        // Dokumenty dla pierwszej instancji MongoDB
        var mongoDocuments = List.of(
                new Document("first_name", "Anna")
                        .append("last_name", "Kowalska")
                        .append("email", "anna@example.com")
                        .append("address", "Kraków, ul. Wielicka 44")
                        .append("phone", "600700800"),
                new Document("first_name", "Piotr")
                        .append("last_name", "Wiśniewski")
                        .append("email", "piotr@example.com")
                        .append("address", "Gdańsk, ul. Długa 5")
                        .append("phone", "700800900")
        );

        // Dokumenty dla drugiej instancji MongoDB (zagnieżdżone struktury)
        var mongoNestedDocuments = List.of(
                new Document("first_name", "Anna")
                        .append("last_name", "Kowalska")
                        .append("email", "anna@example.com")
                        .append("address", "Kraków, ul. Wielicka 44")
                        .append("phone", "600700800")
                        .append("orders", List.of()),
                new Document("first_name", "Piotr")
                        .append("last_name", "Wiśniewski")
                        .append("email", "piotr@example.com")
                        .append("address", "Gdańsk, ul. Długa 5")
                        .append("phone", "700800900")
                        .append("orders", List.of())
        );

        // Wywołanie funkcji pomiarowej dla obu baz danych i obu instancji MongoDB
        measureBatchInserts(postgresQuery, mongoDocuments, mongoNestedDocuments, "clients", "clients_nested", "inserts/multiple_inserts.csv");
    }
}
