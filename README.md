Code to take a MongoDB document, determine if it's >16MB and if so store it as a binary.
Only useful for storing huge payloads as you cannot use the content inside the DB.
Similar to GridFS but goes to and from "Document" class
