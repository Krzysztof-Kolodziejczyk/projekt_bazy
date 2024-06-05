package org.example.utls;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import lombok.var;
import org.junit.jupiter.api.Test;

public class DatabaseConnectionTest extends TestBaseConfig {

    @Test
    public void testPostgreSQLConnection() throws Exception {
        var connection = postgresUtils.getConnection("postgresql.properties");
        assert connection != null;
        connection.close();
    }

    @Test
    public void testMongoEmbeddedConnection() {
        MongoDatabase db = mongoUtils.getMongoDatabase("mongo_embedded.properties");
        MongoIterable<String> collections = db.listCollectionNames();
        collections.first();
    }

    @Test
    public void testMonsgoEmbeddedConnection() {
        MongoDatabase db = mongoUtils.getMongoDatabase("mongo_embedded.properties");
        MongoIterable<String> collections = db.listCollectionNames();
        collections.first();
    }

    @Test
    public void testMongoSimpleConnection() {
        MongoDatabase db = mongoUtils.getMongoDatabase("mongo_simple.properties");
        MongoIterable<String> collections = db.listCollectionNames();
        collections.first();
    }
}
