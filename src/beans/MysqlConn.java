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

    public MysqlConn(String ip, String port, String user, String pwd, String database) throws ClassNotFoundException, SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("GET_MYSQLCONN FAILED ->driverClassNotFound", e);
            throw new ClassNotFoundException();
        }
        try {
            String url = String.format(
                    "jdbc:mysql://%s:%s/%s?useSSL=false&serverTimezone=UTC"
                    ,ip,port,database
                    );
            conn = DriverManager.getConnection(url, user, pwd);
        } catch (SQLException e) {
            log.error("GET_MYSQLCONN FAILED ->get connection failed", e);
            throw new SQLException();
        }
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            log.error("GET_MYSQLCONN FAILED ->get statement failed", e);
            throw new SQLException();
        }
    }


    public Statement getStmt() {
        return stmt;
    }

    public void close() throws SQLException {
        try {
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            log.error("GET_MYSQLCONN FAILED ->connection close failed", e);
            throw new SQLException();
        }
    }
}
