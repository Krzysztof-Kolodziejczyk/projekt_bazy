package org.example.performance.insert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.bson.Document;
import org.example.utls.TestBaseConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ComplexInsertTest extends TestBaseConfig {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void insertClientsIntoDatabases() throws Exception {
        // Wczytanie danych z pliku JSON
        List<Document> clients = mapper.readValue(Paths.get("src/test/resources/mongo/clients.json").toFile(),
                new TypeReference<List<Document>>() {});

        String postgresQuery = generateInsertQuery("clients", clients);

        // Załóżmy, że istnieje metoda do generowania zapytań dla PostgresSQL na podstawie dokumentów MongoDB
        measureBatchInserts(postgresQuery, clients, clients, "clients", "clients_nested", "inserts/clients_inserts.csv");
    }

    // Metoda pomocnicza do generowania zapytania SQL INSERT na podstawie dokumentów MongoDB
    private String generateInsertQuery(String tableName, List<Document> documents) {
        StringBuilder sb = new StringBuilder("INSERT INTO " + tableName + " (");
        if (!documents.isEmpty()) {
            Document firstDoc = documents.get(0);
            // Usuwamy client_id z listy kolumn, jeśli istnieje
            List<String> keys = new ArrayList<>(firstDoc.keySet());
            keys.remove("client_id");  // Usuń `client_id` z kluczy, aby zapobiec ręcznemu wstawianiu

            String columns = String.join(", ", keys);
            sb.append(columns).append(") VALUES ");
            documents.forEach(doc -> {
                String values = keys.stream()
                        .map(key -> formatValueForSQL(doc.get(key)))
                        .collect(Collectors.joining(", "));
                sb.append("(").append(values).append("), ");
            });
            sb.setLength(sb.length() - 2); // Usunięcie przecinka i spacji na końcu
        }
        sb.append(";");
        return sb.toString();
    }

    private String formatValueForSQL(Object value) {
        if (value == null) return "NULL";
        return "'" + value.toString().replace("'", "''") + "'";
    }

}
