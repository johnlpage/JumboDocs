package com.mongodb.devrel;


import java.util.logging.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.Document;

import com.mongodb.client.*;

class JumboExample {

    public static void main(String[] args) {
        LogManager.getLogManager().reset();
        String URI = "mongodb://localhost";
        MongoClient mongoClient = MongoClients.create(URI);
        Logger logger = LoggerFactory.getLogger(JumboExample.class.getName());

        //Connect to MongoDB

        Document helloCommand = new Document("ping", 1);
        Document helloResponse = mongoClient.getDatabase("admin").runCommand(helloCommand);
        logger.info(helloResponse.toJson());

        JumboHandler jumboDocHandler = new JumboHandler();
        JumboTests tests = new JumboTests(mongoClient, jumboDocHandler);
        tests.run();

        return;
    }

}