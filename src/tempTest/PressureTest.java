package tempTest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PressureTest {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://192.168.0.159:30115/frombase?useSSL=false&serverTimezone=UTC&characterEncoding=utf-8";
    private static final String USER = "5v_user";
    private static final String PW = "dec44ad";

    public static void main(String[] args) {
        getValues();

    }

    /**
     * 插入
     */
    private static void insert(String sql) throws ClassNotFoundException, SQLException {
        Connection conn;
        Statement stmt;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL, USER, PW);
        conn.setAutoCommit(false);    //1,开启事务		/true是自动提交，一个语句一个语句执行，设置为false不自动提交。
        stmt = conn.createStatement();
        stmt.execute(sql);
        conn.commit();

        stmt.close();
        conn.close();

    }

    /**
     * 读文件
     */
    private static void getValues() {
        File file = new File("D:\\programs\\canal_pro\\canalLog\\asset_set_extra_charge.sql");
        if (!file.exists()) {
            System.out.println("file missed");
            return;
        }
        try {
            LineIterator iterator = FileUtils.lineIterator(file, "UTf-8");
            long l = 0L;
            int cnt = 1;

            StringBuilder sb = new StringBuilder("INSERT INTO test_canon_preloan.asset_set_extra_charge (`id`, `asset_set_extra_charge_uuid`, `asset_set_uuid`, `version`, `create_time`, `last_modify_time`, `first_account_name`, `first_account_uuid`, `second_account_name`, `second_account_uuid`, `third_account_name`, `third_account_uuid`, `account_amount`) VALUES");
            while (iterator.hasNext()) {
                if (l > 1000L){
                    l = 0L;
                    String sql = sb.toString();
                    if (sql.endsWith(",")){
                        sql = sql.replace("),", ");");
                    }
                    sql = sql.replace(";(", ",(");
                    System.out.println("sql == " + sql);
                    insert(sql);
                    System.out.println("insert " + cnt++ + " times");
//                    if (cnt == 40) break;
                    Thread.sleep(100);
                    sb = new StringBuilder("INSERT INTO test_canon_preloan.asset_set_extra_charge (`id`, `asset_set_extra_charge_uuid`, `asset_set_uuid`, `version`, `create_time`, `last_modify_time`, `first_account_name`, `first_account_uuid`, `second_account_name`, `second_account_uuid`, `third_account_name`, `third_account_uuid`, `account_amount`) VALUES");
                }
                String line = iterator.nextLine();
                if (!line.startsWith("(")) continue;
                sb.append(line);
                l++;
//                if (l > 100) break;
            }
        } catch (IOException | ClassNotFoundException | SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
