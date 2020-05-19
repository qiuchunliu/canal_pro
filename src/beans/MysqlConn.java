package beans;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MysqlConn {

    private static Connection conn = null;
    private static Statement stmt = null;

    public MysqlConn(String ip, String port, String user, String pwd, String database){
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            String url = String.format(
                    "jdbc:mysql://%s:%s/%s?useSSL=false&serverTimezone=UTC"
                    ,ip,port,database
                    );
            conn = DriverManager.getConnection(url, user, pwd);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            assert conn != null;
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public Statement getStmt() {
        return stmt;
    }

    public void close(){
        try {
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
