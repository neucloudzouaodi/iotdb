package cn.edu.tsinghua.zad;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by zouaodi on 2019/4/24.
 */
public class insert {
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
//             statement.execute("CREATE TIMESERIES root.ln.wf01.wt01 WITH DATATYPE=BOOLEAN,ENCODING=PLAIN");
//          statement.execute("CREATE TIMESERIES root.ln.wf01.wt02.temperature WITH DATATYPE=FLOAT , ENCODING=PLAIN");
//            PreparedStatement preparedStatement = connection.prepareStatement("insert into root.ln.wf01.wt02(timestamp,status,temperature) values(?,?,?)");
//            preparedStatement.setLong(1, 1509465600000L);
//            preparedStatement.setBoolean(2, true);
//            preparedStatement.setFloat(3, 25.957603f);
//            preparedStatement.execute();
//            preparedStatement.clearParameters();
//            preparedStatement.setLong(1, 1509465660000L);
//            preparedStatement.setBoolean(2, true);
//            preparedStatement.setFloat(3, 24.359503f);
//            preparedStatement.execute();
//            preparedStatement.clearParameters();
//            preparedStatement.setLong(1, 1509465720000L);
//            preparedStatement.setBoolean(2, false);
//            preparedStatement.setFloat(3, 20.092794f);
//            preparedStatement.execute();
//            preparedStatement.clearParameters();
//            preparedStatement.setTimestamp(1, Timestamp.valueOf("2017-11-01 00:03:00"));
//            preparedStatement.setBoolean(2, false);
//            preparedStatement.setFloat(3, 20.092794f);
//            preparedStatement.execute();
//            preparedStatement.clearParameters();
//            preparedStatement.close();
            ResultSet resultSet = statement.executeQuery("select * from root");
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            System.out.println(databaseMetaData.toString());
            while(resultSet.next()){
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= resultSetMetaData.getColumnCount();i++) {
                    builder.append(resultSet.getString(i)).append(",");
                }
                System.out.println(builder);
            }
            statement.close();
        }
        finally {
            // 8. Close
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }
}
