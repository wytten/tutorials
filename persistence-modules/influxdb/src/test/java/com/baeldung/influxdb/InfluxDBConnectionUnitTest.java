package com.baeldung.influxdb;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBResultMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBConnectionUnitTest {

	private static final String DB_NAME = "baeldung";

	private InfluxDB connection;

	@Before
	public void setUp() {;
		String pw = System.getenv("IFPW");
        connection = InfluxDBFactory.connect("http://127.0.0.1:8086", "admin", pw);
        // Create "baeldung" and check for it
        connection.createDatabase(DB_NAME);
        assertTrue(connection.databaseExists(DB_NAME));
	}
	
	@After
	public void tearDown() {
        // Drop "baeldung" and check again
        connection.deleteDatabase(DB_NAME);
        assertFalse(connection.databaseExists(DB_NAME));
        connection.deleteDatabase(DB_NAME);
        connection.close();
	}
	
    @Test
    public void whenCorrectInfoDatabaseConnects() {
        assertTrue(pingServer(connection));
    }

    private boolean pingServer(InfluxDB influxDB) {
        try {
            // Ping and check for version string
            Pong response = influxDB.ping();
            if (response.getVersion().equalsIgnoreCase("unknown")) {
                log.error("Error pinging server.");
                return false;
            } else {
                log.info("Database version: {}", response.getVersion());
                return true;
            }
        } catch (InfluxDBIOException idbo) {
            log.error("Exception while pinging database: ", idbo);
            return false;
        }
    }

    @Test
    public void whenPointsWrittenPointsExists() throws Exception {

        // Need a retention policy before we can proceed
        connection.createRetentionPolicy("defaultPolicy", DB_NAME, "30d", 1, true);

        // Since we are doing a batch thread, we need to set this as a default
        connection.setRetentionPolicy("defaultPolicy");

        // Enable batch mode
        connection.enableBatch(10, 10, TimeUnit.MILLISECONDS);

        for (int i = 0; i < 10; i++) {
            Point point = Point.measurement("memory")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("name", "server1")
                    .addField("free", 4743656L)
                    .addField("used", 1015096L)
                    .addField("buffer", 1010467L)
                    .build();

            connection.write(DB_NAME, "defaultPolicy", point);
            Thread.sleep(20);

        }

        // Unfortunately, the sleep inside the loop doesn't always add enough time to insure
        // that Influx's batch thread flushes all of the writes and this sometimes fails without
        // another brief pause.
        Thread.sleep(100);
        List<com.baeldung.influxdb.MemoryPoint> memoryPointList = getPoints(connection, "Select * from memory", DB_NAME);

        assertEquals(10, memoryPointList.size());

        // Turn off batch and clean up
        connection.disableBatch();
    }

    private List<MemoryPoint> getPoints(InfluxDB connection, String query, String databaseName) {

        // Run the query
        Query queryObject = new Query(query, databaseName);
        QueryResult queryResult = connection.query(queryObject);

        // Map it
        InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
        return resultMapper.toPOJO(queryResult, MemoryPoint.class);
    }



    @Test
    public void whenBatchWrittenBatchExists() {

        // Need a retention policy before we can proceed
        // Since we are doing batches, we need not set it
        connection.createRetentionPolicy("defaultPolicy", DB_NAME, "30d", 1, true);


        BatchPoints batchPoints = BatchPoints
                .database(DB_NAME)
                .retentionPolicy("defaultPolicy")
                .build();
        Point point1 = Point.measurement("memory")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("free", 4743656L)
                .addField("used", 1015096L)
                .addField("buffer", 1010467L)
                .build();
        Point point2 = Point.measurement("memory")
                .time(System.currentTimeMillis() - 100, TimeUnit.MILLISECONDS)
                .addField("free", 4743696L)
                .addField("used", 1016096L)
                .addField("buffer", 1008467L)
                .build();
        batchPoints.point(point1);
        batchPoints.point(point2);
        connection.write(batchPoints);

        List<MemoryPoint> memoryPointList = getPoints(connection, "Select * from memory", DB_NAME);

        assertEquals(2, memoryPointList.size());
        assertTrue(4743696L == memoryPointList.get(0).getFree());


        memoryPointList = getPoints(connection, "Select * from memory order by time desc", DB_NAME);

        assertEquals(2, memoryPointList.size());
        assertTrue(4743656L == memoryPointList.get(0).getFree());
    }

}
