public class Test02 {
    public static void main(String agrs[]) {
        int line = 16;

        for (int i = 1; i<= line; i++) {
            if (i<= line / 2 + 1) {
                for (int k = 1; k <= line / 2 + 1 - i; k++) {
                    System.out.print(" ");
                }
                for (int k = 1; k <= i; k++) {
                    System.out.print("* ");
                }
                System.out.println();
            } else {
                for (int k = 1; k <= (i - (line / 2 + 1)); k++) {
                    System.out.print(" ");
                }
                for (int k = 1; k <= (2 * (line / 2 + 1) - i); k++) {
                    System.out.print("* ");
                }
                System.out.println();
            }
        }
    }
}
