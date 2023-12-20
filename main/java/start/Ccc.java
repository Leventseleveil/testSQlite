package start;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Ccc {

    public static void main(String[] args) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.Sss");
        int i = LocalDateTime.parse("2022-01-22 10:23:23.999", formatter)
                .compareTo(LocalDateTime.parse("2022-01-22 10:23:23.888", formatter));
        System.out.println(i);
    }
}
