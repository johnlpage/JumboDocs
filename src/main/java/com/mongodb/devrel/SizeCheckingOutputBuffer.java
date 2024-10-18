package com.mongodb.devrel;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.ByteBuf;
import org.bson.io.OutputBuffer;

//This is the most efficient and simple way to check if the document will be >16MB
//If it's not then we don't do anything. Serialize to a fake buffer that barfs when 
// too much data is written.

public class SizeCheckingOutputBuffer extends OutputBuffer {
    private static int JUMBO_SIZE = 16_000_000; //
    private int length = 0;
    Logger logger;

    SizeCheckingOutputBuffer() {
        logger = LoggerFactory.getLogger(SizeCheckingOutputBuffer.class.getName());
    }

    @Override
    public int getPosition() {
        return length;
    }

    @Override
    public void write(final byte[] b) {
        length +=  b.length;
        if (length > JUMBO_SIZE) {
            throw new RuntimeException("Too Large");
        }
    }

    @Override
    public void writeByte(int value) {
        length++;
        if (length > JUMBO_SIZE) {
            throw new RuntimeException("Too Large");
        }
    }

    @Override
    protected void write(int position, int value) {
        //Used to put lengths in further back
        if(position >= length) {
            throw new RuntimeException("Writing into future buffer");
        }
    }

    @Override
    public void truncateToPosition(int newPosition) {
        length = newPosition;
    }

    // These three not used AFAIK
    @Override
    public int pipe(OutputStream out) throws IOException {
        throw new IOException("Unimplemented method 'pipe'");
    }

    @Override
    public List<ByteBuf> getByteBuffers() {
        throw new UnsupportedOperationException("Unimplemented method 'getByteBuffers'");
    }

    @Override
    public int getSize() {

        throw new UnsupportedOperationException("Unimplemented method 'getSize'");
    }

    @Override
    public void writeBytes(byte[] bytes, int offset, int length) {

        throw new UnsupportedOperationException("Unimplemented method 'writeBytes'");
    }

}