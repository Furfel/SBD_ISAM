package pg.ppabis.sbd2;

public class Index {
    public static int[] indexes;

    public static int findPageNumberForRecord(int id) {
        int i = 0;
        while (i + 1 < indexes.length && indexes[i + 1] <= id) ++i;
        return i;
    }

}
