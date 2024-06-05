package org.example.utils;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.result.DeleteResult;
import lombok.var;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class MongoUtils {
    private static final String SYSTEM_PROFILE_COLLECTION = "system.profile";

    public MongoDatabase getMongoDatabase(String propertiesFile) {
        var config = getFromProperties(propertiesFile);
        var mongoClient = MongoClients.create(
                String.format("mongodb://%s:%s@%s:%d/mongo?authsource=admin", config.getUsername(),
                        config.getPassword(), config.getHost(), config.getPort()));
        return mongoClient.getDatabase(config.getDatabaseName());
    }

    public MongoConfig getFromProperties(String filename) {
        try {
            Properties props = new Properties();
            props.load(getClass().getClassLoader().getResourceAsStream(filename));
            return MongoConfig.builder()
                    .host(props.getProperty("db.host"))
                    .port(Integer.parseInt(props.getProperty("db.port")))
                    .username(props.getProperty("db.user"))
                    .password(props.getProperty("db.password"))
                    .databaseName("mongo")
                    .build();
        } catch (IOException | NullPointerException ex) {
            throw new RuntimeException(String.format("Invalid %s format", filename));
        }
    }

    private void enableProfiling(MongoDatabase mongoDatabase) {
        mongoDatabase.runCommand(new Document("profile", 2).append("slowms", 0));
    }

    private double getLastOperationTime(MongoDatabase mongoDatabase, String collectionName) {
        MongoCollection<Document> profileCollection = mongoDatabase.getCollection(SYSTEM_PROFILE_COLLECTION);
        Document lastProfileEntry = profileCollection.find(new Document("op", "query")
                        .append("ns", mongoDatabase.getName() + "." + collectionName))
                .sort(new Document("ts", -1))
                .first();
        if (lastProfileEntry == null) {
            throw new IllegalStateException("No profile entry found. Is profiling enabled?");
        }
        return lastProfileEntry.getInteger("millis");
    }

    public double measureQueryTimeInMillisecondsForFind(MongoDatabase mongoDatabase, Document query, String collectionName) {
        enableProfiling(mongoDatabase);

        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        collection.find(query).first();

        return getLastOperationTime(mongoDatabase, collectionName);
    }

    public double measureQueryTimeInMillisecondsForAggregateForDocument(MongoDatabase mongoDatabase, List<Document> aggregation, String collectionName) {
        enableProfiling(mongoDatabase);

        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        long startTime = System.nanoTime(); // Początek pomiaru czasu
        collection.aggregate(aggregation).first(); // Wykonanie agregacji
        long endTime = System.nanoTime(); // Koniec pomiaru czasu

        disableProfiling(mongoDatabase); // Wyłączenie profilowania po zakończeniu operacji

        return (endTime - startTime) / 1_000_000.0; // Konwersja nanosekund na milisekundy i zwrócenie wyniku
    }

    private void disableProfiling(MongoDatabase mongoDatabase) {
        Document command = new Document("profile", 0); // Wyłączenie profilowania
        mongoDatabase.runCommand(command);
    }

    public double measureDeleteTime(MongoDatabase mongoDatabase, Bson deleteFilter, String collectionName) {
        enableProfiling(mongoDatabase);

        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        long startTime = System.nanoTime(); // Start timing
        DeleteResult result = collection.deleteMany(deleteFilter, new DeleteOptions()); // Perform the delete operation
        long endTime = System.nanoTime(); // End timing

        disableProfiling(mongoDatabase); // Disable profiling after operation

        if (result.wasAcknowledged()) {
            System.out.println("Deleted count: " + result.getDeletedCount());
        }

        return (endTime - startTime) / 1_000_000.0; // Convert from nanoseconds to milliseconds
    }
}
