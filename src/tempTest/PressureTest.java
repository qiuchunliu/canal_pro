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
//    private static final String DB_URL = "jdbc:mysql://111.231.66.20:3306/fromsdfbase?useSSL=false&serverTimezone=UTC&characterEncoding=utf-8";
    private static final String DB_URL = "jdbc:mysql://192.168.0.159:30115/test_canon_preloan?useSSL=false&serverTimezone=UTC&characterEncoding=utf-8";
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
        File file = new File("D:\\programs\\canal_pro\\canalLog\\financial_contract.sql");
        if (!file.exists()) {
            System.out.println("file missed");
            return;
        }
        try {
            LineIterator iterator = FileUtils.lineIterator(file, "UTf-8");
            long l = 0L;
            int cnt = 1;

            StringBuilder sb = new StringBuilder("INSERT INTO test_canon_preloan.financial_contract (`asset_package_format`, `adva_matuterm`, `adva_start_date`, `contract_no`, `contract_name`, `app_id`, `company_id`, `adva_repo_term`, `thru_date`, `capital_account_id`, `financial_contract_type`, `loan_overdue_start_day`, `loan_overdue_end_day`, `payment_channel_id`, `ledger_book_no`, `financial_contract_uuid`, `sys_normal_deduct_flag`, `sys_overdue_deduct_flag`, `sys_create_penalty_flag`, `sys_create_guarantee_flag`, `unusual_modify_flag`, `sys_create_statement_flag`, `transaction_limit_per_transcation`, `transaction_limit_per_day`, `remittance_strategy_mode`, `app_account_uuids`, `allow_online_repayment`, `allow_offline_repayment`, `allow_advance_deduct_flag`, `adva_repayment_term`, `penalty`, `overdue_default_fee`, `overdue_service_fee`, `overdue_other_fee`, `create_time`, `last_modified_time`, `repurchase_approach`, `repurchase_rule`, `repurchase_algorithm`, `day_of_month`, `pay_for_go`, `repurchase_principal_algorithm`, `repurchase_interest_algorithm`, `repurchase_penalty_algorithm`, `repurchase_other_charges_algorithm`, `repayment_check_days`, `repurchase_cycle`, `days_of_cycle`, `temporary_repurchases`, `allow_freewheeling_repayment`, `capital_party`, `other_party`, `contract_short_name`, `financial_type`, `remittance_object`, `asset_party`, `channel_party`, `supplier`, `sub_account_uuids`, `sys_repayment_order_deduct`, `exclusive_repayment_order_deduct`, `project_status`, `circular_project_type`, `abnormal_time`, `amortization_status`, `amortization_time`, `code`) VALUES");
            while (iterator.hasNext()) {
                if (l > 0L){
                    l = 0L;
                    String sql = sb.toString();
                    if (sql.endsWith(",")){
                        sql = sql.replace("),", ");");
                    }
                    sql = sql.replace(";(", ",(");
                    System.out.println("sql == " + sql);
                    insert(sql);
                    System.out.println("insert " + cnt++ + " times");
//                    if (cnt == 2) break;
//                    Thread.sleep(1000);
                    Thread.sleep(System.currentTimeMillis() % 10000);
                    sb = new StringBuilder("INSERT INTO test_canon_preloan.financial_contract (`asset_package_format`, `adva_matuterm`, `adva_start_date`, `contract_no`, `contract_name`, `app_id`, `company_id`, `adva_repo_term`, `thru_date`, `capital_account_id`, `financial_contract_type`, `loan_overdue_start_day`, `loan_overdue_end_day`, `payment_channel_id`, `ledger_book_no`, `financial_contract_uuid`, `sys_normal_deduct_flag`, `sys_overdue_deduct_flag`, `sys_create_penalty_flag`, `sys_create_guarantee_flag`, `unusual_modify_flag`, `sys_create_statement_flag`, `transaction_limit_per_transcation`, `transaction_limit_per_day`, `remittance_strategy_mode`, `app_account_uuids`, `allow_online_repayment`, `allow_offline_repayment`, `allow_advance_deduct_flag`, `adva_repayment_term`, `penalty`, `overdue_default_fee`, `overdue_service_fee`, `overdue_other_fee`, `create_time`, `last_modified_time`, `repurchase_approach`, `repurchase_rule`, `repurchase_algorithm`, `day_of_month`, `pay_for_go`, `repurchase_principal_algorithm`, `repurchase_interest_algorithm`, `repurchase_penalty_algorithm`, `repurchase_other_charges_algorithm`, `repayment_check_days`, `repurchase_cycle`, `days_of_cycle`, `temporary_repurchases`, `allow_freewheeling_repayment`, `capital_party`, `other_party`, `contract_short_name`, `financial_type`, `remittance_object`, `asset_party`, `channel_party`, `supplier`, `sub_account_uuids`, `sys_repayment_order_deduct`, `exclusive_repayment_order_deduct`, `project_status`, `circular_project_type`, `abnormal_time`, `amortization_status`, `amortization_time`, `code`) VALUES");
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
