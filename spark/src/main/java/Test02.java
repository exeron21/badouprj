import java.math.BigDecimal;

public class Test02 {
    public static void main(String agrs[]) {
        double s = new BigDecimal(34.6).setScale(10, BigDecimal.ROUND_UP).doubleValue();
        System.out.println(s);
    }
}
