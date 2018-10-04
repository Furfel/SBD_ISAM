package pg.ppabis.sbd2;

public class Page {

    public static final int RECORDS_PER_PAGE = 7;
    public static final int BLOCK_SIZE = RECORDS_PER_PAGE * Record.SIZE;

    public static Record[] page;
    public static Record[] overflow;

    public static int overflowRecords = 0;
    public static int mainRecords = 0;

    public static int findPlaceInPageForRecord(int id) {
        int i = 0;
        while (i < page.length && page[i] != null && id > page[i].getId()) ++i;
        return i;
        /*while (i+1 < page.length && page[i+1] != null && page[i+1].getId() > 0 && page[i+1].getId() <= id) {
            ++i;
        }
        if( page[i].getId() < id && ( (i+1>=page.length) || (page[i+1] == null || page[i+1].getId() <= 0) ) ) ++i;
        return i;*/
    }

    public static Record getFromOverflow(int of) {
        return overflow[of];
    }

    public static Record getFromPage(int pagen, int i) {
        page = sample_pages[pagen]; //To be removed
        return page[i];
    }

    public static void setOnPage(int pagen, int i, Record r) {
        page = sample_pages[pagen];
        page[i] = r;
        //savePage()
    }

    public static int putInOverflow(int id, byte[] data, int ov) {
        overflow[overflowRecords] = new Record(id, data, ov);
        overflowRecords++;
        return overflowRecords-1;
    }

    public static Record[][] sample_pages;

    public static void sampleDb() {
        sample_pages = new Record[2][RECORDS_PER_PAGE];
        Index.indexes = new int[2];

        Index.indexes[0] = 100;
        Index.indexes[1] = 600;

        for (int i = 0; i < sample_pages[0].length; ++i)
            sample_pages[0][i] = new Record(50 + i * 30, "12345".getBytes());
        sample_pages[0][0].overflow = 2;

        sample_pages[1][0] = new Record(600, "666".getBytes(), 1);
        sample_pages[1][1] = new Record(700, "123124".getBytes());

        mainRecords = 12;

        overflow = new Record[100];
        overflow[0] = new Record(580, "123412".getBytes());
        overflow[1] = new Record(570, "1234".getBytes(), 0);
        overflow[2] = new Record(20, "123455".getBytes(), 4);
        overflow[4] = new Record(25, "213124".getBytes(), 3);
        overflow[3] = new Record(30, "21312".getBytes());

        overflowRecords = 5;

    }

}
