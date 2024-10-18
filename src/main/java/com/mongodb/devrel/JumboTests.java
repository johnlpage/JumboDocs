package com.mongodb.devrel;

import java.util.ArrayList;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;

public class JumboTests {

    JumboHandler handler;
    Document jumboDoc;
    MongoClient mongoClient;
    Logger logger;

    JumboTests(MongoClient mongoClient, JumboHandler jumboDocHandler) {
        logger = LoggerFactory.getLogger(JumboTests.class.getName());
        this.handler = jumboDocHandler;
        this.mongoClient = mongoClient;

        jumboDoc = new JumboTestDocument();
        // Slow Expensive way to get inaccurate value just to check we are testing with
        // big docs
        logger.info("Jumbo Doc is ~ " + jumboDoc.toJson().length() + " bytes ");

    }

    public void run() {
        try {
            logger.info("Running Test");
            smallDocSizeTest();
            largeDocSizeTest();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.getStackTrace();
        }
    }

    private void smallDocSizeTest() throws Exception {
        logger.info("Small Doc Size Test");
        Document d = new Document("name", "Harvest");
        if (handler.isJumbo(d)) {
            throw new Exception("Small document incorrectly identified as Jumbo");
        }
        logger.info("OK");
    }

    private boolean checkIfIndexExists(MongoCollection<Document> collection, String indexName) {
        // List all indexes in the collection
        for (Document index : collection.listIndexes()) {
            String name = index.getString("name");
            if (indexName.equals(name)) {
                return true; // The index exists
            }
        }
        return false; // The index does not exist
    }

    private void largeDocSizeTest() throws Exception {
        logger.info("Large Doc Size Test");

        if (handler.isJumbo(jumboDoc) == false) {
            throw new Exception("Large document incorrectly identified as Not Jumbo");
        }

        // Split it into Parts

        ArrayList<Document> parts;
        ObjectId key = new ObjectId();
        parts = handler.splitDocument(jumboDoc, key);

        // Write them into a Collection

        MongoDatabase db = mongoClient.getDatabase("JumboTest");
        MongoCollection<Document> coll = db.getCollection("jumbodocs");
        ClientSession session = mongoClient.startSession();

        // Verify we have the index

        boolean indexExists = checkIfIndexExists(coll, "__recid_1___chunkno_1");
        if (!indexExists) {
            throw new Exception("Need index on { __recid:1, __chunkno:1 }");
        }

        try {
            session.startTransaction();

            // Delete any old version with this recid - Make sure you have index on __recid
            // and __chunkno
            Bson delete_existing_query = Filters.eq("__recid", key);
            DeleteResult dr = coll.deleteMany(delete_existing_query);

            // Insert the new ones
            InsertManyResult ir = coll.insertMany(parts);
            session.commitTransaction();
        } catch (Exception e) {
            throw new Exception("Transaction Failed " + e.getMessage());
        }

        parts = null; // Garbage collect

        // Fetch it back
        Bson query = Filters.eq("__recid", key);
        Bson sortorder = Sorts.orderBy(Sorts.ascending("__recid"), Sorts.ascending("__chunkno"));

        FindIterable<Document> cursor = coll.find(query).sort(sortorder);
        Document newDoc = handler.combineChunks(cursor);

        logger.info("Reconstructed doc Doc is ~ " + jumboDoc.toJson().length() + " bytes ");

        logger.info("OK");
    }

}
