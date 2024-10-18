package com.mongodb.devrel;

import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

import org.bson.Document;

public class JumboTestDocument extends Document {
    
     //Generate a Huge document >16MB
     JumboTestDocument() {
      
        int nThings = 1000; //Just for scale
        // Add lots of fields
        for(int x=0;x<nThings;x++ ) {
            this.append("int_"+x, x);
            this.append("double_"+x, Double.valueOf(x));
            this.append("string_"+x, new String("Something " + x));
            this.append("date_"+x,new Date());
            this.append("long_"+x, Long.valueOf(x));
        }
        // Make a large array of objects
        ArrayList<Document> elements = new ArrayList<Document>();
        for(int e=0;e < nThings; e++) {
            Document element = new Document();
            for( int x=0;x< 20;x++ ) {
                element.append("int_"+x, x);
                element.append("double_"+x, Double.valueOf(x));
                element.append("string_"+x, new String("Something " + x));
                element.append("date_"+x,new Date());
                element.append("long_"+x, Long.valueOf(x));
            }
            elements.add(element);
        }
        this.append("array", elements);

        //Add a Huge Field (20MB string)
        this.append("bigstring", generateRandomString(20*1024*1024));

        //Some nesting
        Document nest = new Document();
        for(int x=0;x<500;x++ ) {
            nest.append("int_"+x, x);
            nest.append("double_"+x, Double.valueOf(x));
            nest.append("string_"+x, new String("Something " + x));
            nest.append("date_"+x,new Date());
            nest.append("long_"+x, Long.valueOf(x));
        }
        nest.append("child", Document.parse(nest.toJson())) ; // Deep copy
        this.append("doc",nest);
    }

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789    ";
    private static final Random random = new Random();

    private String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
    }

}
