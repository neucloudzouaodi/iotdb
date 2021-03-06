package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.sql.*;

import static cn.edu.tsinghua.iotdb.service.TestUtils.*;
import static org.junit.Assert.*;


public class LargeDataTest {

    private static final String TIMESTAMP_STR = "Time";
    private final String d0s0 = "root.vehicle.d0.s0";
    private final String d0s1 = "root.vehicle.d0.s1";
    private final String d0s2 = "root.vehicle.d0.s2";
    private final String d0s3 = "root.vehicle.d0.s3";
    private final String d0s4 = "root.vehicle.d0.s4";
    private final String d0s5 = "root.vehicle.d0.s5";

    private final String d1s0 = "root.vehicle.d1.s0";
    private final String d1s1 = "root.vehicle.d1.s1";

    private static String[] stringValue = new String[]{"A", "B", "C", "D", "E"};
    private static String[] booleanValue = new String[]{"true", "false"};

    private static String[] create_sql = new String[]{
            "SET STORAGE GROUP TO root.vehicle",

            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s5 WITH DATATYPE=DOUBLE, ENCODING=RLE",

            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE",
    };

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;
    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    private int maxNumberOfPointsInPage;
    private int pageSizeInByte;
    private int groupSizeInByte;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();

            // use small page setting
            // origin value
            maxNumberOfPointsInPage = tsFileConfig.maxNumberOfPointsInPage;
            pageSizeInByte = tsFileConfig.pageSizeInByte;
            groupSizeInByte = tsFileConfig.groupSizeInByte;
            // new value
            tsFileConfig.maxNumberOfPointsInPage = 1000;
            tsFileConfig.pageSizeInByte = 1024 * 1024 * 150;
            tsFileConfig.groupSizeInByte = 1024 * 1024 * 1000;

            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(5000);
            //recovery value
            tsFileConfig.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
            tsFileConfig.pageSizeInByte = pageSizeInByte;
            tsFileConfig.groupSizeInByte = groupSizeInByte;
            EnvironmentUtils.cleanEnv();
        }
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException, FileNotFoundException {
        //PrintStream ps = new PrintStream(new FileOutputStream("src/test/resources/ha.txt"));
        //System.setOut(ps);

        if (testFlag) {
            Thread.sleep(5000);
            insertSQL();

            Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

            // select test
            selectAllTest();
            selectOneSeriesWithValueFilterTest();
            seriesTimeDigestReadTest();
            crossSeriesReadUpdateTest();

            // aggregation test
            aggregationWithoutFilterTest();
            aggregationTest();
            aggregationWithFilterOptimizationTest();
            allNullSeriesAggregationTest();
            negativeValueAggTest();

            // group by test
            groupByTest();
            allNullSeriesGroupByTest();
            fixBigGroupByClassFormNumberTest();

            // fill test
            previousFillTest();
            linearFillTest();

            // verify the rightness of overflow insert and after merge operation
            newInsertAggTest();

            connection.close();
        }
    }

    private void selectAllTest() throws ClassNotFoundException, SQLException {
        String selectSql = "select * from root";

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(selectSql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                        + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3);
                //System.out.println("===" + ans);
                cnt++;
            }
            //System.out.println("cnt ::" + cnt);
            assertEquals(23400, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void selectOneSeriesWithValueFilterTest() throws ClassNotFoundException, SQLException {

        String selectSql = "select s0 from root.vehicle.d0 where s0 >= 20";

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(selectSql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0);
                //System.out.println("===" + ans);
                cnt++;
            }
            assertEquals(16440, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    // https://github.com/thulab/iotdb/issues/192
    private void seriesTimeDigestReadTest() throws ClassNotFoundException, SQLException {

        // [3000, 13599] , [13700,23999]

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        boolean hasResultSet;
        Statement statement;

        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s0 from root.vehicle.d0 where time > 22987");
            assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0);
                //System.out.println(ans);
                //assertEquals(retArray[cnt], ans);
                cnt++;
            }
            //System.out.println(cnt);
            assertEquals(3012, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void crossSeriesReadUpdateTest() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        boolean hasResultSet;
        Statement statement;

        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s1 from root.vehicle.d0 where s0 < 111");
            assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                long time = Long.valueOf(resultSet.getString(TIMESTAMP_STR));
                String value = resultSet.getString(d0s1);
                if (time > 23000 && time < 100100) {
                    //System.out.println("~~~~" + time + "," + value);
                    assertEquals("11111111", value);
                }
                //String ans = resultSet.getString(d0s1);
                //System.out.println(ans);
                cnt++;
            }
            assertEquals(22600, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void aggregationWithoutFilterTest() throws ClassNotFoundException, SQLException {

        String sql = "select count(s0),mean(s0),first(s0),sum(s0)," +
                "count(s1),mean(s1),first(s1),sum(s1) from root.vehicle.d0";
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(sum(d0s0)) + "," + resultSet.getString(mean(d0s0))
                        + "," + resultSet.getString(first(d0s0)) + "," + resultSet.getString(count(d0s1))
                        + "," + resultSet.getString(sum(d0s1)) + "," + resultSet.getString(mean(d0s1))
                        + "," + resultSet.getString(first(d0s1));
                // 0,23400,2672550.0,114.21153846153847,2000,23200,1.2213278715E10,526434.4273706897,4
                assertEquals("0,23400,2672550.0,114.21153846153847,2000,22700,1.2212153465E10,537980.3288546256,4", ans);
                cnt++;
            }
            assertEquals(1, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void aggregationTest() throws ClassNotFoundException, SQLException {

        String sql = "select count(s0),mean(s0),first(s0),sum(s0),last(s0) from root.vehicle.d0 where s0 >= 20";

        //String sql = "select count(s0),mean(s0),first(s0),sum(s0) from root.vehicle.d0 where s0 >= 20";

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(mean(d0s0)) + "," + resultSet.getString(first(d0s0))
                        + "," + resultSet.getString(sum(d0s0)) + "," + resultSet.getString(last(d0s0));
                assertEquals("0,16440,159.58211678832117,2000,2623530.0,6666", ans);
                cnt++;
            }
            assertEquals(1, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void aggregationWithFilterOptimizationTest() throws ClassNotFoundException, SQLException {

        // all the page data of d0.s0 are eligible for the time filter
        String sql = "select count(s0),mean(s0),first(s0),sum(s0),last(s0) from root.vehicle.d0 where time < 1600000";
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(sum(d0s0)) + "," + resultSet.getString(mean(d0s0))
                         + "," + resultSet.getString(first(d0s0)) + "," + resultSet.getString(last(d0s0));
                //System.out.println("===" + ans);
                assertEquals("0,23400,2672550.0,114.21153846153847,2000,6666", ans);
                cnt++;
            }
            assertEquals(1, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void allNullSeriesAggregationTest() throws ClassNotFoundException, SQLException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            String sql;
            boolean hasResultSet;
            ResultSet resultSet;
            int cnt;
            String[] retArray = new String[]{};

            // (1). aggregation test : there is no value in series d1.s0 and no filter
            sql = "select count(s0),max_value(s0),min_value(s0),max_time(s0),min_time(s0),mean(s0),first(s0),sum(s0),last(s0) " +
                    "from root.vehicle.d1";
            hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d1s0))
                        + "," + resultSet.getString(max_value(d1s0)) + "," + resultSet.getString(min_value(d1s0))
                        + "," + resultSet.getString(max_time(d1s0)) + "," + resultSet.getString(min_time(d1s0))
                        + "," + resultSet.getString(mean(d1s0)) + "," + resultSet.getString(first(d1s0))
                        + "," + resultSet.getString(sum(d1s0)) + "," + resultSet.getString(last(d1s0));
                assertEquals("0,0,null,null,null,null,null,null,null,null", ans);
                //System.out.println("============ " + ans);
                cnt++;
            }
            assertEquals(1, cnt);
            statement.close();

            // (2). aggregation test : there is no value in series d1.s0 and have filter
            sql = "select count(s0),max_value(s0),min_value(s0),max_time(s0),min_time(s0),mean(s0),first(s0),sum(s0),last(s0) " +
                    "from root.vehicle.d1 where s0 > 1000000";
            statement = connection.createStatement();
            hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d1s0))
                        + "," + resultSet.getString(max_value(d1s0)) + "," + resultSet.getString(min_value(d1s0))
                        + "," + resultSet.getString(max_time(d1s0)) + "," + resultSet.getString(min_time(d1s0))
                        + "," + resultSet.getString(mean(d1s0)) + "," + resultSet.getString(first(d1s0))
                        + "," + resultSet.getString(sum(d1s0)) + "," + resultSet.getString(last(d1s0));
                assertEquals("0,0,null,null,null,null,null,null,null,null", ans);
                //System.out.println("0,0,null,null,null,null" + ans);
                cnt++;
            }
            assertEquals(1, cnt);
            statement.close();

            // (3). aggregation test : there is no value in series d1.s0 in the given time range
            sql = "select max_value(s0),min_value(s0)" +
                    "from root.vehicle.d0 where time > 13601 and time < 13602";
            statement = connection.createStatement();
            hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR)
                        + "," + resultSet.getString(max_value(d0s0)) + "," + resultSet.getString(min_value(d0s0));
                assertEquals("0,null,null", ans);
                //System.out.println(".." + ans);
                cnt++;
            }
            assertEquals(1, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void negativeValueAggTest() throws ClassNotFoundException, SQLException {

        String sql = "select count(s0),sum(s0),mean(s0),first(s0) from root.vehicle.d0 where s0 < 0";
        String selectSql = "select s0 from root.vehicle.d0 where s0 < 0";

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            double sum = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(mean(d0s0)) + "," + resultSet.getString(first(d0s0))
                        + "," + resultSet.getString(sum(d0s0));
                //System.out.println("====" + ans);
                Assert.assertEquals("0,855,-10.0,-1,-8550.0", ans);
                //sum += Double.valueOf(resultSet.getString(d0s0));
                cnt++;
            }
            assertEquals(1, cnt);
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void groupByTest() throws ClassNotFoundException, SQLException, FileNotFoundException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            // (1). group by only with time filter
            String[] retArray = new String[]{
                    "2000,null,null,null,null,null",
                    "2100,null,null,null,null,null",
                    "2200,null,null,null,null,null",
                    "2300,49,2375.0,2351,116375.0,2399",
                    "2400,100,2449.5,2400,244950.0,2499",
                    "2500,null,null,null,null,null",
                    "3000,100,49.5,0,4950.0,99",
                    "3100,100,49.5,0,4950.0,99",
                    "3200,100,49.5,0,4950.0,99",
                    "3300,100,49.5,0,4950.0,99",
                    "3400,null,null,null,null,null",
                    "3500,null,null,null,null,null",
                    "3600,null,null,null,null,null",
                    "3700,null,null,null,null,null",
                    "3800,null,null,null,null,null",
                    "3900,null,null,null,null,null",
                    "4000,null,null,null,null,null",
            };
            String countSql = "select count(s0),mean(s0),first(s0),sum(s0),last(s0) from root.vehicle.d0 where time < 3400 and time > 2350 " +
                    "group by (100ms, 2000, [2000,2500], [3000, 4000])";
            boolean hasResultSet = statement.execute(countSql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(mean(d0s0)) + "," + resultSet.getString(first(d0s0))
                        + "," + resultSet.getString(sum(d0s0)) + "," + resultSet.getString(last(d0s0));
                assertEquals(retArray[cnt], ans);
                //System.out.println("============ " + ans);
                cnt++;
            }
            assertEquals(17, cnt);
            statement.close();

            // (2). group by only with time filter and value filter
            retArray = new String[]{
                    "2000,null,null,null,null,null",
                    "2100,null,null,null,null,null",
                    "2200,null,null,null,null,null",
                    "2300,49,2375.0,2351,116375.0,2399",
                    "2400,100,2449.5,2400,244950.0,2499",
                    "2500,null,null,null,null,null",
                    "3000,28,51.214285714285715,8,1434.0,99",
                    "3100,26,49.42307692307692,0,1285.0,89",
                    "3200,30,52.5,6,1575.0,99",
                    "3300,24,51.5,16,1236.0,87",
                    "3400,null,null,null,null,null",
                    "3500,null,null,null,null,null",
                    "3600,null,null,null,null,null",
                    "3700,null,null,null,null,null",
                    "3800,null,null,null,null,null",
                    "3900,null,null,null,null,null",
                    "4000,null,null,null,null,null",
            };
            statement = connection.createStatement();
            countSql = "select count(s0),mean(s0),first(s0),sum(s0),last(s0) from root.vehicle.d0 where time < 3400 and time > 2350 and s2 > 15 " +
                    "group by (100ms, 2000, [2000,2500], [3000, 4000])";
            hasResultSet = statement.execute(countSql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(mean(d0s0)) + "," + resultSet.getString(first(d0s0))
                        + "," + resultSet.getString(sum(d0s0)) + "," + resultSet.getString(last(d0s0));
                assertEquals(retArray[cnt], ans);
                //System.out.println("============ " + ans);
                cnt++;
            }
            assertEquals(17, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void fixBigGroupByClassFormNumberTest() throws ClassNotFoundException, SQLException {

        // remove formNumber in GroupByEngineNoFilter and GroupByEngineWithFilter

        String[] retArray = new String[]{
                "3000,100,99,0,3099,3000,49.5,0,4950.0",
                "3100,100,99,0,3199,3100,49.5,0,4950.0",
                "3200,100,99,0,3299,3200,49.5,0,4950.0",
                "3300,100,99,0,3399,3300,49.5,0,4950.0",
                "3400,100,99,0,3499,3400,49.5,0,4950.0",
                "3500,100,99,0,3599,3500,49.5,0,4950.0",
                "3600,100,99,0,3699,3600,49.5,0,4950.0",
                "3700,100,99,0,3799,3700,49.5,0,4950.0",
                "3800,100,99,0,3899,3800,49.5,0,4950.0",
                "3900,100,99,0,3999,3900,49.5,0,4950.0",
                "4000,1,0,0,4000,4000,0.0,0,0.0"
        };
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        ;
        boolean hasResultSet;
        Statement statement;

        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            statement = connection.createStatement();
            hasResultSet = statement.execute("select min_value(s0),max_value(s0),max_time(s0),min_time(s0),count(s0),mean(s0),first(s0),sum(s0)"
                    + "from root.vehicle.d0 group by(100ms, 0, [3000,4000])");
            assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(max_value(d0s0)) + "," + resultSet.getString(min_value(d0s0))
                        + "," + resultSet.getString(max_time(d0s0)) + "," + resultSet.getString(min_time(d0s0))
                        + "," + resultSet.getString(mean(d0s0)) + "," + resultSet.getString(first(d0s0))
                        + "," + resultSet.getString(sum(d0s0));
                // System.out.println(ans);
                assertEquals(retArray[cnt], ans);
                cnt++;
            }
            assertEquals(11, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void allNullSeriesGroupByTest() throws ClassNotFoundException, SQLException, FileNotFoundException {

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            String sql;
            boolean hasResultSet;
            ResultSet resultSet;
            int cnt;
            String[] retArray = new String[]{};

            // (1). group by test : the result is all null value
            sql = "select count(s0),max_value(s1),mean(s0),first(s1),sum(s0) from root.vehicle.d0 where s2 > 1000000 " +
                    "group by (1000ms, 2000, [3500, 25000])";
            hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0)) + "," + resultSet.getString(max_value(d0s1))
                        + "," + resultSet.getString(mean(d0s0)) + "," + resultSet.getString(first(d0s1)) + "," + resultSet.getString(sum(d0s0));
                Assert.assertEquals(resultSet.getString(TIMESTAMP_STR) + ",null,null,null,null,null", ans);
                // System.out.println("============ " + ans);
                cnt++;
            }
            Assert.assertEquals(23, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void previousFillTest() throws SQLException, ClassNotFoundException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet;
            ResultSet resultSet;
            int cnt;

            // null test
            hasResultSet = statement.execute("select s0,s1,s2,s3,s4 from root.vehicle.d0 where time = 199 fill(int32[previous, 5m])");
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                        + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3) + "," + resultSet.getString(d0s4);
                assertEquals("199,null,null,null,null,null", ans);
                cnt ++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();

            // has value in queryTime
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s0,s1,s2,s3,s4 from root.vehicle.d0 where time = 12000 fill(int32[previous, 5m])");
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                        + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3) + "," + resultSet.getString(d0s4);
                assertEquals("12000,0,15,10.0,A,true", ans);
                //System.out.println("=====" + ans);
                cnt ++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();

            // test series,page index and update data
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s0,s1,s2,s3,s4,s5 from root.vehicle.d0 where time = 99999 " +
                    "fill(int32[previous, 90000ms], int64[previous, 90000ms], float[previous, 90000ms], double[previous, 90000ms], " +
                    "boolean[previous,90000ms], text[previous, 90000ms])");
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                        + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3) + "," + resultSet.getString(d0s4)
                        + "," + resultSet.getString(d0s5);
                assertEquals("99999,59,11111111,14.0,E,false,13599.0", ans);
                cnt ++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();

            // overflow insert
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s0,s1,s2,s3,s4,s5 from root.vehicle.d0 where time = 202000 " +
                    "fill(int32[previous, 90000ms], int64[previous, 90000ms], float[previous, 90000ms], double[previous, 90000ms], " +
                    "boolean[previous,90000ms], text[previous, 90000ms])");
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                        + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3) + "," + resultSet.getString(d0s4)
                        + "," + resultSet.getString(d0s5);
                assertEquals("202000,6666,7777,8888.0,goodman,false,9999.0", ans);
                //System.out.println("====" + ans);
                cnt ++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void linearFillTest() throws SQLException, ClassNotFoundException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet;
            ResultSet resultSet;
            int cnt;

            // doesn't have value in queryTime, but have value in [beforeTime, afterTime]
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s0,s1,s2,s3,s4 from root.vehicle.d0 " +
                    "where time = 24001 fill(int32[previous, 5m], int64[linear, 100ms, 100ms]," +
                    "float[linear, 100ms, 10m], double[previous, 10m])");
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                        + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3) + "," + resultSet.getString(d0s4);
                assertEquals("24001,59,null,14.001053,null,null", ans);
                //System.out.println("=====" + ans);
                cnt ++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();

            // overflow insert
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s0,s1,s2,s3,s4 from root.vehicle.d0 " +
                    "where time = 2300 fill(int32[linear, 5m, 5m], int64[linear, 100ms, 100ms]," +
                    "float[linear, 100ms, 10m], double[linear])");
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                        + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3) + "," + resultSet.getString(d0s4);
                assertEquals("2300,2300,null,2302.0,A,null", ans);
                //System.out.println("=====" + ans);
                cnt ++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();

            // overflow insert, file data added into result firstly
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s0,s1,s2,s3,s4 from root.vehicle.d0 " +
                    "where time = 2600 fill(int32[linear], int64[linear]," +
                    "float[linear], double[linear])");
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                        + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3) + "," + resultSet.getString(d0s4);
                assertEquals("2600,1996,null,1998.4192,null,null", ans);
                //System.out.println("=====" + ans);
                cnt ++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();

            // overflow insert, file data added into result firstly
            statement = connection.createStatement();
            hasResultSet = statement.execute("select s0,s1,s2,s3,s4 from root.vehicle.d0 " +
                    "where time = 50000 fill(int32[linear], int64[linear]," +
                    "float[linear], double[linear])");
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + "," + resultSet.getString(d0s1)
                        + "," + resultSet.getString(d0s2) + "," + resultSet.getString(d0s3) + "," + resultSet.getString(d0s4);
                assertEquals("50000,39,11111111,27.684555,null,null", ans);
                //System.out.println("=====" + ans);
                cnt ++;
            }
            Assert.assertEquals(1, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void insertSQL() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            for (String sql : create_sql) {
                statement.execute(sql);
            }

            // insert large amount of data    time range : 3000 ~ 13600
            for (int time = 3000; time < 13600; time++) {
                //System.out.println("===" + time);
                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s4) values(%s, %s)", time, booleanValue[time % 2]);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, time);
                statement.execute(sql);
            }

            // statement.execute("flush");

            // insert large amount of data time range : 13700 ~ 24000
            for (int time = 13700; time < 24000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
                statement.execute(sql);
            }

            statement.execute("merge");

            Thread.sleep(5000);

            // buffwrite data, unsealed file
            for (int time = 100000; time < 101000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
                statement.execute(sql);
            }

            statement.execute("flush");

            // bufferwrite data, memory data
            for (int time = 200000; time < 201000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, -time % 20);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, -time % 30);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, -time % 77);
                statement.execute(sql);
            }

            // overflow insert, time < 3000
            for (int time = 2000; time < 2500; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
                statement.execute(sql);
            }

            // overflow insert, time > 200000
            for (int time = 200900; time < 201000; time++) {

                String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 6666);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, 7777);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, 8888);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, "goodman");
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s4) values(%s, %s)", time, booleanValue[time % 2]);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, 9999);
                statement.execute(sql);
            }

            // overflow delete
            statement.execute("DELETE FROM root.vehicle.d0.s1 WHERE time < 3200");

            // overflow update
            statement.execute("UPDATE root.vehicle SET d0.s1 = 11111111 WHERE time > 23000 and time < 100100");

            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public void newInsertAggTest() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;

        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            String sql;
            boolean hasResultSet;
            ResultSet resultSet;
            int cnt;
            String[] retArray = new String[]{};
            sql = "select count(s0),count(s1),count(s2),count(s3),count(s4)" +
                    "from root.vehicle.d0";
            hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2))
                        + "," + resultSet.getString(count(d0s3)) + "," + resultSet.getString(count(d0s4));
                //assertEquals("0,0,null,null,null,null,null,null,null,null", ans);
                //System.out.println("(1) ============ " + ans);
                cnt++;
            }
            statement.close();

            statement = connection.createStatement();
            for (int time = 10000; time < 60000; time++) {
                sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
                statement.execute(sql);
                sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
                statement.execute(sql);
            }
            statement.close();

            sql = "select count(s0),count(s1),count(s2),count(s3),count(s4)" +
                    "from root.vehicle.d0";
            statement = connection.createStatement();
            hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2))
                        + "," + resultSet.getString(count(d0s3)) + "," + resultSet.getString(count(d0s4));
                //assertEquals("0,0,null,null,null,null,null,null,null,null", ans);
                System.out.println("(2) ============ " + ans);
                cnt++;
            }
            statement.close();

            statement = connection.createStatement();
            statement.execute("merge");
            statement.close();

            statement = connection.createStatement();
            hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            resultSet = statement.getResultSet();
            cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
                        + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2))
                        + "," + resultSet.getString(count(d0s3)) + "," + resultSet.getString(count(d0s4));
                //assertEquals("0,0,null,null,null,null,null,null,null,null", ans);
                System.out.println("(3) ============ " + ans);
                cnt++;
            }
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
