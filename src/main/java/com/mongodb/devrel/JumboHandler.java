package com.mongodb.devrel;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.FindIterable;


import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.ByteBufNIO;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import org.bson.io.ByteBufferBsonInput;
import org.bson.types.Binary;

public class JumboHandler {

    Logger logger;

    SizeCheckingOutputBuffer sizeChecker;

    JumboHandler() {
        logger = LoggerFactory.getLogger(JumboHandler.class.getName());
        sizeChecker = new SizeCheckingOutputBuffer();
    }

    // This returns true if the document as BSON would be larger than X bytes
    // It uses existing Driver classes and methods to keep it's code complexity and
    // risk low
    // It allocates minimal objects/RAM and for an object >16MB stops as soon as the
    // threshold is reached

    boolean isJumbo(Document doc) {
        sizeChecker.truncateToPosition(0);
        BsonBinaryWriter writer = new BsonBinaryWriter(sizeChecker);
        DocumentCodec documentCodec = new DocumentCodec();
        try {
            documentCodec.encode(writer, doc, EncoderContext.builder().build());
        } catch (RuntimeException e) {
            return true;
        }
        return false;
    }

    // Whilst it would be good if we could stream this - as data structures grow
    // We then need to go back and update previous values, for example the length of
    // an array
    // in bytes is stored is at the start of it - and only when you finish
    // converting an array
    // Do you have that value - even a document starts with it's length, because of
    // this
    // we need to get the WHOLE document as a byte array then just split it up when
    // we read and
    // write - we cannot write it in parts unfortunately.

    ArrayList<Document> splitDocument(Document jumboDoc, Object idValue) {
        final int DOC_CHUNK_SIZE = 8 * 1024 * 1024; //
        ArrayList<Document> chunkDocs = new ArrayList<>();
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        DocumentCodec documentCodec = new DocumentCodec();

        documentCodec.encode(writer, jumboDoc, EncoderContext.builder().build());

        byte[] rawBuffer = buffer.toByteArray();
        int dataLength = rawBuffer.length;
        int nChunks = (dataLength / DOC_CHUNK_SIZE) + 1;

        for (int c = 0; c < nChunks; c++) {
            // _id is left as default
            Document d = new Document("__recid", idValue);
            d.append("__chunkno", c);

            int offset = c * DOC_CHUNK_SIZE;
            int length = DOC_CHUNK_SIZE;
            if (offset + DOC_CHUNK_SIZE >= dataLength) {
                length = dataLength - offset;
            }

            byte[] subArray = Arrays.copyOfRange(rawBuffer, offset, offset + length);

            // Create and return a MongoDB Binary object from the byte buffer
            Binary BSONchunk = new Binary(subArray);
            d.append("__bindata", BSONchunk);
            chunkDocs.add(d);
        }

        return chunkDocs;
    }

    public Document combineChunks(FindIterable<Document> cursor) {
        Document rval;
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        DocumentCodec documentCodec = new DocumentCodec();

        try {
            for (Document doc : cursor) {
                outputBuffer.write(((Binary) doc.get("__bindata")).getData());
            }
        } catch (Exception e) {
            logger.error("Error" + e.getMessage());
        }

        ByteBufferBsonInput bsonInput = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(outputBuffer.toByteArray())));

        rval = documentCodec.decode(new BsonBinaryReader(bsonInput), DecoderContext.builder().build());

        return rval;

    }
}
