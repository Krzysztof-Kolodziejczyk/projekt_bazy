package org.example.utls;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.var;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.example.utils.CsvWriter;
import org.example.utils.MongoUtils;
import org.example.utils.PostgresUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class TestBaseConfig {

    public static final String POSTGRES_PROPERTIES = "postgresql.properties";
    public static final String MONGO_SIMPLE_PROPERTIES = "mongo_embedded.properties";
    public static final String MONGO_EMBEDDED_PROPERTIES = "mongo_simple.properties";

    protected static final MongoUtils mongoUtils = new MongoUtils();
    protected static final PostgresUtils postgresUtils = new PostgresUtils();

    protected void measureQueries(String postgresQuery, Document mongoSimpleQuery, String mongoSimpleCollection,
                                  Document mongoEmbeddedQuery, String mongoEmbeddedCollection, String outputFile) throws SQLException {
        var connection = postgresUtils.getConnection(POSTGRES_PROPERTIES);
        var queryTime = String.valueOf(postgresUtils.measureQueryTimeInMilliseconds(connection, postgresQuery));

        var mongoSimpleDatabase = mongoUtils.getMongoDatabase(MONGO_SIMPLE_PROPERTIES);
        var mongoSimpleQueryTime = String.valueOf(mongoUtils.measureQueryTimeInMillisecondsForFind(mongoSimpleDatabase, mongoSimpleQuery, mongoSimpleCollection));

        var mongoEmbeddedDatabase = mongoUtils.getMongoDatabase(MONGO_EMBEDDED_PROPERTIES);
        var mongoEmbeddedQueryTime = String.valueOf(mongoUtils.measureQueryTimeInMillisecondsForFind(mongoEmbeddedDatabase, mongoEmbeddedQuery, mongoEmbeddedCollection));

        CsvWriter.writeToCsv("src/test/resources/results", outputFile, List.of(queryTime, mongoSimpleQueryTime, mongoEmbeddedQueryTime));
    }

    protected void measureQueries(String postgresQuery, List<Document> mongoSimpleQuery, String mongoSimpleCollection,
                                  List<Document> mongoEmbeddedQuery, String mongoEmbeddedCollection, String outputFile) throws SQLException {
        var connection = postgresUtils.getConnection(POSTGRES_PROPERTIES);
        var queryTime = String.valueOf(postgresUtils.measureQueryTimeInMilliseconds(connection, postgresQuery));

        var mongoSimpleDatabase = mongoUtils.getMongoDatabase(MONGO_SIMPLE_PROPERTIES);
        var mongoSimpleQueryTime = String.valueOf(mongoUtils.measureQueryTimeInMillisecondsForAggregateForDocument(mongoSimpleDatabase, mongoSimpleQuery, mongoSimpleCollection));

        var mongoEmbeddedDatabase = mongoUtils.getMongoDatabase(MONGO_EMBEDDED_PROPERTIES);
        var mongoEmbeddedQueryTime = String.valueOf(mongoUtils.measureQueryTimeInMillisecondsForAggregateForDocument(mongoEmbeddedDatabase, mongoEmbeddedQuery, mongoEmbeddedCollection));

        CsvWriter.writeToCsv("src/test/resources/results", outputFile, List.of(queryTime, mongoSimpleQueryTime, mongoEmbeddedQueryTime));
    }

    public void measureBatchInserts(String postgresQuery, List<Document> mongoDocuments, List<Document> mongoNestedDocuments,
                                    String mongoCollectionName, String mongoNestedCollectionName, String outputFile) throws SQLException {
        // Pomiar dla PostgreSQL
        var postgresConnection = postgresUtils.getConnection(POSTGRES_PROPERTIES);
        long startTimePostgres = System.currentTimeMillis();
        try (Statement stmt = postgresConnection.createStatement()) {
            stmt.executeUpdate(postgresQuery);
        }
        long endTimePostgres = System.currentTimeMillis();

        // Pomiar dla pierwszej instancji MongoDB (proste dokumenty)
        MongoDatabase mongoDatabaseSimple = mongoUtils.getMongoDatabase(MONGO_SIMPLE_PROPERTIES);
        MongoCollection<Document> collectionSimple = mongoDatabaseSimple.getCollection(mongoCollectionName);
        long startTimeMongoSimple = System.currentTimeMillis();
        collectionSimple.insertMany(mongoDocuments);
        long endTimeMongoSimple = System.currentTimeMillis();

        // Pomiar dla drugiej instancji MongoDB (zagnieżdżone dokumenty)
        MongoDatabase mongoDatabaseNested = mongoUtils.getMongoDatabase(MONGO_EMBEDDED_PROPERTIES);
        MongoCollection<Document> collectionNested = mongoDatabaseNested.getCollection(mongoNestedCollectionName);
        long startTimeMongoNested = System.currentTimeMillis();
        collectionNested.insertMany(mongoNestedDocuments);
        long endTimeMongoNested = System.currentTimeMillis();

        // Obliczenie czasów wykonania
        double postgresTime = (endTimePostgres - startTimePostgres) / 1000.0; // Sekundy
        double mongoSimpleTime = (endTimeMongoSimple - startTimeMongoSimple) / 1000.0; // Sekundy
        double mongoNestedTime = (endTimeMongoNested - startTimeMongoNested) / 1000.0; // Sekundy

        // Zapis do CSV
        CsvWriter.writeToCsv("src/test/resources/results", outputFile, List.of(
                String.valueOf(postgresTime),
                String.valueOf(mongoSimpleTime),
                String.valueOf(mongoNestedTime)
        ));
    }

    protected void measureUpdates(String postgresQuery, Bson mongoSimpleQuery, Bson mongoSimpleUpdate, String mongoSimpleCollection,
                                Bson mongoNestedQuery, Bson mongoNestedUpdate, String mongoNestedCollection, String outputFile) throws SQLException {
        long startTime, endTime;
        double postgresTime, mongoSimpleTime, mongoNestedTime;

        // Aktualizacja w PostgreSQL
        try (Connection conn = postgresUtils.getConnection(POSTGRES_PROPERTIES); Statement stmt = conn.createStatement()) {
            startTime = System.currentTimeMillis();
            stmt.executeUpdate(postgresQuery);
            endTime = System.currentTimeMillis();
            postgresTime = (endTime - startTime) / 1000.0;
        }

        // Aktualizacja w pierwszej instancji MongoDB
        MongoDatabase mongoSimpleDatabase = mongoUtils.getMongoDatabase(MONGO_SIMPLE_PROPERTIES);
        MongoCollection<Document> collectionSimple = mongoSimpleDatabase.getCollection(mongoSimpleCollection);
        startTime = System.currentTimeMillis();
        collectionSimple.updateOne(mongoSimpleQuery, mongoSimpleUpdate);
        endTime = System.currentTimeMillis();
        mongoSimpleTime = (endTime - startTime) / 1000.0;

        // Aktualizacja w drugiej instancji MongoDB
        MongoDatabase mongoNestedDatabase = mongoUtils.getMongoDatabase(MONGO_EMBEDDED_PROPERTIES);
        MongoCollection<Document> collectionNested = mongoNestedDatabase.getCollection(mongoNestedCollection);
        startTime = System.currentTimeMillis();
        collectionNested.updateOne(mongoNestedQuery, mongoNestedUpdate);
        endTime = System.currentTimeMillis();
        mongoNestedTime = (endTime - startTime) / 1000.0;

        // Zapisywanie wyników do pliku CSV
        CsvWriter.writeToCsv("src/test/resources/results", outputFile, List.of(String.valueOf(postgresTime), String.valueOf(mongoSimpleTime), String.valueOf(mongoNestedTime)));
    }

    protected void measureDeletes(String postgresQuery, Bson mongoSimpleFilter, String mongoSimpleCollection,
                                Bson mongoEmbeddedFilter, String mongoNestedCollection, String outputFile) throws SQLException {
        var connection = postgresUtils.getConnection(POSTGRES_PROPERTIES);
        var queryTime = String.valueOf(postgresUtils.measureQueryTimeInMilliseconds(connection, postgresQuery));

        var mongoSimpleDatabase = mongoUtils.getMongoDatabase(MONGO_SIMPLE_PROPERTIES);
        var mongoSimpleQueryTime = String.valueOf(mongoUtils.measureDeleteTime(mongoSimpleDatabase, mongoSimpleFilter, mongoSimpleCollection));

        var mongoEmbeddedDatabase = mongoUtils.getMongoDatabase(MONGO_EMBEDDED_PROPERTIES);
        var mongoEmbeddedQueryTime = String.valueOf(mongoUtils.measureDeleteTime(mongoEmbeddedDatabase, mongoEmbeddedFilter, mongoNestedCollection));

        CsvWriter.writeToCsv("src/test/resources/results", outputFile, List.of(queryTime, mongoSimpleQueryTime, mongoEmbeddedQueryTime));
    }


}
