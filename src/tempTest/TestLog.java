package tempTest;

import beans.EscapeChar;
import beans.TransTag;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import io.netty.handler.codec.base64.Base64;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class TestLog {

    public static void main(String[] args) {
//        Logger log = Logger.getLogger(TestLog.class);
//        log.info("info message");
//        log.error("error message");
//        System.out.println("mysql-bin.000002".hashCode());
//        System.out.println(System.currentTimeMillis());
//        System.out.println(new Date().getTime());
//        String reg = "frombase\\d";
//        Pattern pattern = Pattern.compile(reg);
//        System.out.println(pattern.matcher("frombase1").matches());

//        Boolean result = (Boolean)AviatorEvaluator.execute("1 < 23");
//        System.out.println(result);

//        String expression = "pro_batch == 22 && state > 13";
////        String expression = "a > 100 && b =~ a[0-9]";
//        // 编译表达式
//        Expression compiledExp = AviatorEvaluator.compile(expression);
//        HashMap<String, Object> env = new HashMap<>();
//        env.put("pro_batch", "12");
//        env.put("state", 11);
//        env.put("report_date_code", 19);
//        env.put("report_date", "2019-12-29");
//        env.put("str", "log_bbin");
////        for (Map.Entry<String, Object> s: env.entrySet()
////             ) {
////            System.out.println(s.getKey() + "--" + s.getValue());
////
////        }
//        Boolean res = (Boolean) compiledExp.execute(env);
//        System.out.println(res);  // false

//        Map<String,Object> env=new HashMap<>();
//        env.put("email","abc3");
////        env.put("email","dugk@foxmail.com");
//        System.out.println(AviatorEvaluator.execute("email=~ /abc\\d/", env));
////        System.out.println(AviatorEvaluator.execute("email=~ /([\\w0-8]+)@\\w+[\\.\\w+]+/", env));

//        String initialCondition = "pro_batch = 22 and state > 13 and  report_date_code >= 20 and report_date_code <= 40 or report_date > '2019-12-20' OR str like '%log_bin%'";
//        String regConditionStr;
//        String replace1 = initialCondition.replace(">=", "biggerThan").replace("<=", "smallerThan").replace("!=", "notEqual");
//        String replace2 = replace1.replace(" and ", " && ").replace(" AND ", " && ").replace(" or ", " || ").replace(" OR ", " || ").replace("=", "==");
//        String replace3 = replace2.replace("biggerThan", ">=").replace("smallerThan", "<=").replace("notEqual", "!=");
//        regConditionStr = replace3.replace(" like ", " =~ ").replace(" LIKE ", " =~ ").replace("\'%", "/.*").replace("%\'", ".*/");
//        System.out.println(regConditionStr);

//        System.out.println("ABCDEa123abc".hashCode());  // 165374702
//        System.out.println("ABCDFB123abc".hashCode()); //  165374702

//        String myText = "regr";
//        String s = UUID.nameUUIDFromBytes((myText).getBytes()).toString();
//        System.out.println(s);

//        String  s = "中文";
//        String s1 = new String(s.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
//        System.out.println(s1);
//        try {
//            System.out.println("232".split("2")[5]);
//        }catch (ArrayIndexOutOfBoundsException e){
//            System.out.println(e.getMessage());
//        }

//        String[] ss = new String[2];
//        ss[0] = "a";
//        ss[1] = "b";
//        try {
//            System.out.println(ss[1]);
//        }catch (IndexOutOfBoundsException e){
////            System.out.println("1111");
//        }

//        long uid = TransTag.getInstance().nextId();
//
//        System.out.println(uid);

//        HashMap<String, Integer> mm = new HashMap<>();
//        mm.put("aaa", 3);
//        mm.put("bbb", 3);
//        mm.put("ccb", 3);
//        mm.put("cb", 3);
//        System.out.println(mm.size());

//        String str = "ledger_book_shelf_.+";
//        System.out.println(Pattern.compile(str).matcher("ledger_book_shelfq_aa232a").matches());

        EscapeChar ec = new EscapeChar();
        String s = "sff中文'";
        System.out.println(ec.convertCol(s));

//        System.out.println(ec.convertChar(s));


    }
}
