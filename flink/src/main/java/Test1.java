import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Test1 {
    public static void main(String[] args) {
        String dateStr = "2016年10月25日";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy年MM月dd日");
        LocalDate date = LocalDate.parse(dateStr, formatter);
        date.isBefore(date);
        System.out.println(date);
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy年MM月dd日");
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("hh:mm:ss");
        String nowStr = now.format(dateFormatter);
        String timeStr = now.format(timeFormatter);

        System.out.println(nowStr);
        System.out.println(timeStr);
    }
}
