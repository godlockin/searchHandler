package searchhandler.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

@Slf4j
@Component
public class DataBaseUtils {

    public static void export() throws Exception {

        Class.forName("com.mysql.jdbc.Driver");
        String url = "jdbc:mysql://127.0.0.1:3306/test";
        String username = "root";
        String password = "Aa123456";
        Connection conn = DriverManager.getConnection(url, username, password);
        Statement statement = conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
        statement.setFetchSize(Integer.MIN_VALUE);
    }
}
