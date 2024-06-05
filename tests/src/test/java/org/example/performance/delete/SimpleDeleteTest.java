package org.example.performance.delete;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.example.utls.TestBaseConfig;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class SimpleDeleteTest extends TestBaseConfig {

    @Test
    public void deleteClientsFromPostgresAndMongo() throws SQLException {
        String postgresQuery = "DELETE FROM clients WHERE client_id = 1;";

        Document mongoSimpleQuery = new Document("client_id", 1);

        Document mongoEmbeddedQuery = new Document("client_id", 1);

        measureQueries(postgresQuery, mongoSimpleQuery, "clients", mongoEmbeddedQuery, "clients_nested", "deletes/single_delete.csv");
    }

    @Test
    public void deleteMultipleClientsFromPostgresAndMongo() throws SQLException {
        String postgresQuery = "DELETE FROM clients WHERE client_id IN (2, 3, 4);";

        Bson mongoSimpleFilter = Filters.in("client_id", 2, 3, 4);

        Bson mongoEmbeddedFilter = Filters.in("client_id", 2, 3, 4);

        measureDeletes(postgresQuery, mongoSimpleFilter, "clients", mongoEmbeddedFilter, "clients_nested", "deletes/multiple_deletes.csv");
    }

}
