package cn.edu.tsinghua.zad;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by zouaodi on 2019/4/24.
 */
public class IotdbHelloWorld {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Connection connection = null;
        Statement statement = null;
        try {

            // 1. load JDBC driver of IoTDB
            Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
            // 2. DriverManager connect to IoTDB
            connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
            // 3. Create statement
            statement = connection.createStatement();
            // 4. Set storage group
//            statement.execute("set storage group to root.vehicle.sensor");
            // 5. Create timeseries
//            statement.execute("CREATE TIMESERIES root.vehicle.sensor.sensor0 WITH DATATYPE=DOUBLE, ENCODING=PLAIN");
//            statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
            // 6. Insert data to IoTDB
//            statement.execute("INSERT INTO root.vehicle.sensor(timestamp, sensor0) VALUES (2018/10/24 19:33:00, 142)");
            // 7. Query data
            String sql = "select * from root.ln.wf01";
            String path = "root.ln.wf01.wt01";
            boolean hasResultSet = statement.execute(sql);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            System.out.println(databaseMetaData.toString());
            if (hasResultSet) {
//                ResultSet resultSet = databaseMetaData.getColumns(null,null,"root.*",null);
                ResultSet res = statement.getResultSet();
                System.out.println("                    Time" + "|" + path);
                while (res.next()) {
                    long time = Long.parseLong(res.getString("Time"));
                    String dateTime = dateFormat.format(new Date(time));
                    System.out.println(dateTime + " | " + res.getString(path));
                }
                res.close();
            }
        }
        finally {
            // 8. Close
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }
}
