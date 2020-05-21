package beans;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MysqlConn {

    private static Connection conn = null;
    private static Statement stmt = null;
    private static Logger log = Logger.getLogger(MysqlConn.class);

    public MysqlConn(String ip, String port, String user, String pwd, String database){
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("classNotFound ", e);
        }
        try {
            String url = String.format(
                    "jdbc:mysql://%s:%s/%s?useSSL=false&serverTimezone=UTC"
                    ,ip,port,database
                    );
            conn = DriverManager.getConnection(url, user, pwd);
        } catch (SQLException e) {
            log.error("get connection failed", e);
        }
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            log.error("get statement failed", e);
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
