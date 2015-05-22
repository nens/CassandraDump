/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.nelen_schuurmans.ddsc.cassandra;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Carsten Byrman <carsten.byrman@nelen-schuurmans.nl>
 */
public class Count {

    private static final Logger logger = LoggerFactory.getLogger(Dump.class);

    public static void main(String[] args) throws IOException {

        // Filename, e.g. `/data/ddsc.json.gz`
        String file = args.length > 0 ? args[0] : "ddsc.json.gz";

        // Read from a gzip-compressed file using a streaming JSON processor.
        InputStream input = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)));
        JsonFactory factory = new JsonFactory();
        JsonParser parser = factory.createJsonParser(input);

        Set<String> set = new HashSet<String>();
        JsonToken token = parser.nextToken(); // START_ARRAY

        while (true) {
            token = parser.nextToken();
            if (JsonToken.START_OBJECT.equals(token)) {
                parser.nextToken(); // FIELD_NAME
                parser.nextToken(); // VALUE_STRING
                String bucket = parser.getText();
                String uuid = bucket.split(":")[0].toLowerCase();
                try {
                    UUID.fromString(uuid);
                } catch (Exception e) {
                    logger.error(String.format("No UUID: {%s}", uuid));
                }
                set.add(uuid);
            } else if (token == null) {
                break;
            }
        }

        parser.close();
        System.out.println(set.size());

    }
}
