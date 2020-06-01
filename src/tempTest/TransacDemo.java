package tempTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TransacDemo {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    //    private static final String DB_URL = "jdbc:mysql://192.168.24.101:3306/tobase1?useSSL=false&serverTimezone=UTC";
//    private static final String DB_URL = "jdbc:mysql://192.168.69.178:3306/?useSSL=false&serverTimezone=UTC";
//    private static final String DB_URL = "jdbc:mysql://111.231.66.20:3306/fromsdfbase?useSSL=false&serverTimezone=UTC";
    private static final String DB_URL = "jdbc:mysql://192.168.0.159:30115/frombase?useSSL=false&serverTimezone=UTC";
    private static final String USER = "5v_user";
//    private static final String USER = "mycanal";
    private static final String PW = "dec44ad";
//    private static final String PW = "1111";

    public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {

        int cnt = 0;
        while (cnt++ < 50) {
            Connection conn;
            Statement stmt;
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, USER, PW);
            conn.setAutoCommit(false);    //1,开启事务		/true是自动提交，一个语句一个语句执行，设置为false不自动提交。

            stmt = conn.createStatement();

            System.out.println("insert " + cnt + " row");
            Thread.sleep(1);
            //2，打开窗口
            stmt.addBatch("INSERT INTO frombase.asset_total (\n" +
                    "  `asset_uuid`,\n" +
                    "  `asset_principal_value`,\n" +
                    "  `asset_interest_value`,\n" +
                    "  `asset_recycle_date`,\n" +
                    "  `create_time`,\n" +
                    "  `last_modified_time`,\n" +
                    "  `current_period`,\n" +
                    "  `version_no`,\n" +
                    "  `old_version`,\n" +
                    "  `contract_uuid`,\n" +
                    "  `active_finger_print`,\n" +
                    "  `flag`,\n" +
                    "  `old_ctime`,\n" +
                    "  `time_flag`,\n" +
                    "  `single_loan_contract_no`,\n" +
                    "  `outer_repayment_plan_no`\n" +
                    "  ) VALUES (uuid(),\n" +
                    "  158.46,\n" +
                    "  30.08,\n" +
                    "  \"2019-05-10\",\n" +
                    "  \"2019-01-01 12:03:52.0\",\n" +
                    "  \"2019-05-11 18:29:26.0\",\n" +
                    "  4,\n" +
                    "  1,\n" +
                    "  1,\n" +
                    "  uuid(),\n" +
                    "  \"b3cb4b1be87ad5e654301ea17224ad9b\",\n" +
                    "  1,\n" +
                    "  \"2019-01-01 12:03:52.0\",\n" +
                    "  63731750400,\n" +
                    "  \"ZC277448872695070720\",\n" +
                    "  \"XM277443498672521216\"\n" +
                    "  );");
            stmt.addBatch("INSERT INTO frombase.con_fin_prt_inc (\n" +
                    "  `loan_id`,`contract_uuid`,`version_no`,`core_finger_print`,`installments`,`create_time`,\n" +
                    "  `repeated`,`time_flag`,`version_order`) VALUES (\n" +
                    "144217791992433235,\n" +
                    "uuid(),\n" +
                    "-55678671,\n" +
                    "\"2f07ec4167896f264f11e3a7a7eec19d\",\n" +
                    "12,\n" +
                    "\"2019-04-09 17:58:34.0\",\n" +
                    "1,\n" +
                    "1566553320,\n" +
                    "1\n" +
                    ");");

            stmt.addBatch("INSERT INTO frombase.borrower (\n" +
                    "  `borrower_uuid`,\n" +
                    "  `borrower_name`,\n" +
                    "  `certificate_type`,\n" +
                    "  `certificate_no`,\n" +
                    "  `gmt_create`\n" +
                    "  ) VALUES(uuid(),\n" +
                    "  \"吴蔚\",\n" +
                    "  0,\n" +
                    "  \"50022719951023043X\",\n" +
                    "  \"2018-12-04 15:14:59.0\"\n" +
                    "  );");
//            stmt.addBatch("INSERT INTO frombase.borrower (\n" +
//                    "  `borrower_uuid`,\n" +
//                    "  `borrower_name`,\n" +
//                    "  `certificate_type`,\n" +
//                    "  `certificate_no`,\n" +
//                    "  `gmt_create`\n" +
//                    "  ) VALUES(uuid(),\n" +
//                    "  \"吴蔚\",\n" +
//                    "  0,\n" +
//                    "  \"50022719951023043X\",\n" +
//                    "  \"2018-12-04 15:14:59.0\"\n" +
//                    "  );");
//        stmt.addBatch("UPDATE frombase.con_fin_prt_inc SET version_no = 101 WHERE id BETWEEN 3 AND 9;");

            stmt.executeBatch();    //4，批量操作执行语句（处理语句）。
            conn.commit();    //5，执行完成提交语句。

            stmt.close();
            conn.close();
        }
    }
}
