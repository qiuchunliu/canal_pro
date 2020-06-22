package beans;

import java.util.HashMap;

public class EscapeChar {

    private HashMap<String, String> specialChar = new HashMap<>();

    public EscapeChar(){
        specialChar.put("'", "\\'");
        specialChar.put("\"", "\\\"");
        specialChar.put("\b", "\\\b");
        specialChar.put("\n", "\\\n");
        specialChar.put("\r", "\\\r");
        specialChar.put("\t", "\\\t");
        specialChar.put("\\", "\\\\");
        specialChar.put("\\%", "\\\\%");
    }

    private String convertChar(String str){
        String afterStr = specialChar.get(str);
        if (afterStr != null){
            return afterStr;
        }else {
            return str;
        }
    }

    public String convertCol(String col){
        StringBuilder sb = new StringBuilder();
        char[] chars = col.toCharArray();
        for (char c : chars){
            sb.append(convertChar(String.valueOf(c)));
        }
        return sb.toString();
    }
}
