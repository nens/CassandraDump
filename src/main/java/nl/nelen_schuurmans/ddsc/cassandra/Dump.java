package nl.nelen_schuurmans.ddsc.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.Bytes;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-off code for dumping DDSC's Cassandra database to gzip-compressed JSON
 * (it will be garbage collected after migration).
 *
 * The JSON will be formatted something like this:
 *
 * [{"key":"97f16f51-c916-41ad-ae88-4ee8b1626b44:2014","columns":[
 * ["2014-01-03T02:59:17.000000Z_flag","-1",1388718646815724],
 * ["2014-01-03T02:59:17.000000Z_value","-2.8927",1388718646815724],
 * ["2014-01-05T00:00:02.000000Z_flag","-1",1388882404411913],
 * ["2014-01-05T00:00:02.000000Z_value","-2.9056",1388882404411913],...]},...]
 *
 * @author Carsten Byrman <carsten.byrman@nelen-schuurmans.nl>
 */
public class Dump {

    private static final Logger logger = LoggerFactory.getLogger(Dump.class);

    private static final String[] NODES = {
        "10.100.239.201", "10.100.239.203", // EasyNet
        "10.100.239.202", "10.100.239.204" // GlobalSwitch
    };
    private static final int PORT = 9042; // native_transport_port
    private static final String KEYSPACE = "ddsc";
    private static final String COLUMN_FAMILY = "events";
    private static final int PAGE_SIZE = 24 * 365;
    private static final String ENCODING = "UTF-8";

    public static void main(String[] args) throws IOException {

        // Filename, e.g. `/data/ddsc.json.gz`
        String file = args.length > 0 ? args[0] : "ddsc.json.gz";

        Cluster cluster = Cluster.builder().addContactPoints(NODES).withPort(PORT).build();
        Session session = cluster.connect(KEYSPACE);

        // Get the first partition key.
        String sql = String.format("SELECT key FROM %s LIMIT 1;", COLUMN_FAMILY);
        Statement stmt = new SimpleStatement(sql);
        ResultSet rs = session.execute(stmt);

        if (rs.isExhausted()) {
            logger.info("Nothing to dump.");
            System.exit(0);
        }

        Charset charset = Charset.forName(ENCODING);

        Row row = rs.one();
        String keyHex = Bytes.toHexString(row.getBytes("key"));
        String key = charset.decode(row.getBytes("key")).toString();

        // Double check it's really the very first key.
        sql = String.format("SELECT key FROM %s WHERE token(key) < token(%s) "
                + "LIMIT 1", "events", keyHex);
        stmt = new SimpleStatement(sql);
        rs = session.execute(stmt);

        if (!rs.isExhausted()) {
            logger.error("{} is not the first partion key!?", key);
            System.exit(1);
        }

        // Write to a gzip-compressed file using a streaming JSON processor.
        OutputStream output = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
        JsonFactory jf = new JsonFactory();
        JsonGenerator jg = jf.createJsonGenerator(output, JsonEncoding.UTF8);

        jg.writeStartArray();

        int i = 0;

        // Our version of Cassandra does not support automatic paging in CQL,
        // resulting in a lot of boilerplate code.
        while (true) {

            //logger.info("Fetching data for partion key #{}: {}", i++, key);
            // Get the first page for the current key.
            sql = String.format("SELECT key, column1, value, writetime(value) "
                    + "FROM %s WHERE key = %s LIMIT %d",
                    "events", keyHex, PAGE_SIZE);
            stmt = new SimpleStatement(sql);
            rs = session.execute(stmt);

            jg.writeStartObject();
            jg.writeStringField("key", key);
            jg.writeFieldName("columns");
            jg.writeStartArray();

            while (!rs.isExhausted()) {

                Iterator<Row> iter = rs.iterator();

                while (iter.hasNext()) {
                    row = iter.next();
                    String column1 = charset.decode(row.getBytes("column1")).toString();
                    String value = charset.decode(row.getBytes("value")).toString();
                    Long writetime = row.getLong("writetime(value)");
                    jg.writeStartArray();
                    jg.writeString(column1);
                    jg.writeString(value);
                    jg.writeNumber(writetime);
                    jg.writeEndArray();
                }

                // Fetch the next page for the current key.
                String column1Hex = Bytes.toHexString(row.getBytes("column1"));
                sql = String.format("SELECT key, column1, value, writetime(value) "
                        + "FROM %s WHERE key = %s and column1 > %s LIMIT %d",
                        "events", keyHex, column1Hex, PAGE_SIZE);
                stmt = new SimpleStatement(sql);
                rs = session.execute(stmt);

            }

            jg.writeEndArray();
            jg.writeEndObject();

            // Get the next key.
            sql = String.format("SELECT key FROM %s WHERE token(key) > token(%s) "
                    + "LIMIT 1", "events", keyHex);
            stmt = new SimpleStatement(sql);
            rs = session.execute(stmt);

            if (rs.isExhausted()) {
                break;
            }

            row = rs.one();
            keyHex = Bytes.toHexString(row.getBytes("key"));
            key = charset.decode(row.getBytes("key")).toString();

        }

        jg.writeEndArray();
        jg.close();
        output.close();
        session.close();
        cluster.close();
    }

}
