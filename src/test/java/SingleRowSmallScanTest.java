import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.*;
import org.junit.*;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SingleRowSmallScanTest {
    private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static final byte[] TEST_TABLE = "test".getBytes();
    private static final byte[] TEST_CF = "cf".getBytes();
    private static final byte[] TEST_ROW = "row".getBytes();
    private static final String TEST_COL = "col:%04d";
    private static final byte[] TEST_VALUE = new byte[] { 0x01 };
    private static final int NUM_COLUMNS = 10;
    private static final int INFINITE_LOOP = 10000;
    private static HConnection connection;

    @BeforeClass
    public static void prepareTestEnvironment() throws Exception {
        TEST_UTIL.startMiniCluster();
        Configuration conf = TEST_UTIL.getConfiguration();
        connection = HConnectionManager.createConnection(conf);
        populateTestSet();
    }

    @AfterClass
    public static void cleanupTestEnvironment() throws Exception {
        connection.close();
        TEST_UTIL.shutdownMiniCluster();
    }

    private static void populateTestSet() throws IOException {
        HTable hTable = TEST_UTIL.createTable(TEST_TABLE, TEST_CF);
        try {
            Put put = new Put(TEST_ROW);
            for (int colId = 0; colId < NUM_COLUMNS; colId++) {
                put.add(TEST_CF, String.format(TEST_COL, colId).getBytes(), TEST_VALUE);
            }
            hTable.put(put);
        } finally {
            hTable.close();
        }
    }

    private int smallScan(int numCaching, int batchSize) throws IOException {
        return executeScan(true, numCaching, batchSize);
    }

    private int bigScan(int numCaching, int batchSize) throws IOException {
        return executeScan(false, numCaching, batchSize);
    }

    private int executeScan(boolean isSmallScan, int numCaching, int batchSize) throws IOException {
        HTableInterface htable = connection.getTable(TEST_TABLE);
        int numCells = 0;
        try {
            Scan scan = new Scan(TEST_ROW, TEST_ROW);
            if (numCaching > 0) {
                scan.setCaching(numCaching);
            }
            if (batchSize > 0) {
                scan.setBatch(batchSize);
            }
            scan.setSmall(isSmallScan);

            ResultScanner rs = htable.getScanner(scan);
            for (Result r = rs.next(); (r != null && numCells < INFINITE_LOOP); r = rs.next()) {
                numCells += r.size();
            }
        } finally {
            htable.close();
        }

        return numCells;
    }

    @Test
    public void testNormalCases() throws IOException {
        // OK
        assertEquals(NUM_COLUMNS, smallScan(0, 0));
        assertEquals(NUM_COLUMNS, bigScan(0, 0));
    }

    @Test
    public void testScan_caching1_batch0() throws IOException {
        // OK
        assertEquals(NUM_COLUMNS, smallScan(1, 0));
        assertEquals(NUM_COLUMNS, bigScan(1, 0));
    }

    @Test
    public void testScan_caching1_batch1() throws IOException {
        // FAIL, infinite looping
        assertEquals(INFINITE_LOOP, smallScan(1, 1));
        assertEquals(NUM_COLUMNS, bigScan(1, 1));
    }

    @Test
    public void testScan_caching1_batch2() throws IOException {
        // FAIL, infinite looping
        assertEquals(INFINITE_LOOP, smallScan(1, 2));
        assertEquals(NUM_COLUMNS, bigScan(1, 2));
    }

    @Test
    public void testScan_caching2_batch1() throws IOException {
        // FAIL, infinite looping
        assertEquals(INFINITE_LOOP, smallScan(2, 1));
        assertEquals(NUM_COLUMNS, bigScan(2, 1));
    }

    @Test
    public void testScan_caching4_batch2() throws IOException {
        // FAIL, infinite looping
        assertEquals(INFINITE_LOOP, smallScan(4, 2));
        assertEquals(NUM_COLUMNS, bigScan(4, 2));
    }

    @Test
    public void testScan_caching4_batch3() throws IOException {
        // FAIL, duplicated scan
        assertEquals(NUM_COLUMNS + NUM_COLUMNS - 3, smallScan(4, 3));
        assertEquals(NUM_COLUMNS, bigScan(4, 3));
    }

    @Test
    public void testScan_caching5_batch3() throws IOException {
        // OK
        assertEquals(NUM_COLUMNS, smallScan(5, 3));
        assertEquals(NUM_COLUMNS, bigScan(5, 3));
    }
}