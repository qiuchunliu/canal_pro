package tempTest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;


public class ReadText {
    public static void main(String[] args) {
        apacheCommonsIo();

    }
    private static void apacheCommonsIo() {
        File file = new File("D:\\programs\\canal_pro\\canalLog\\asset_set_extra_charge.sql");
        if (!file.exists()) {
            System.out.println("file missed");
            return;
        }

        try {
            LineIterator iterator = FileUtils.lineIterator(file, "UTf-8");
            long l = 0L;
            while (iterator.hasNext()) {
                String line = iterator.nextLine();
                System.out.println(line);
                l++;
                if (l > 100) break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
