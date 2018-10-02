package pg.ppabis.sbd2;

public class Main {

    public static final int RECORDS_PER_PAGE = 7;
    public static final int BLOCK_SIZE = RECORDS_PER_PAGE * Record.SIZE;

    public static int[] indexes;
    public static Record[] page;

    public static int findPageNumberForRecord(int id) {
        int i=0;
        while(i+1 < indexes.length && indexes[i+1] <= id) ++i;
        return i;
    }

    public static int findPlaceInPageForRecord(int id) {
        int i=0;
        while(i < page.length && page[i]!=null && id > page[i].getId()) ++i;
        return i;
        /*while (i+1 < page.length && page[i+1] != null && page[i+1].getId() > 0 && page[i+1].getId() <= id) {
            ++i;
        }
        if( page[i].getId() < id && ( (i+1>=page.length) || (page[i+1] == null || page[i+1].getId() <= 0) ) ) ++i;
        return i;*/
    }

    public static int[] findPlaceForRecord(int id) {
        int pagen = findPageNumberForRecord(id);
        page = sample_pages[pagen]; //To be removed
        int place = findPlaceInPageForRecord(id);
        if (place >= RECORDS_PER_PAGE) {
            pagen++;
            place = 0;
        }
        return new int[] {pagen, place};
    }

    public static void main(String[] args) {
        sampleDb();
        if(args.length>0) {
            int id = Integer.parseInt(args[0]);
            int[] res = findPlaceForRecord(id);
            printDb(res[0], res[1]);
        }
    }

    public static void printDb(int markPg, int markRec) {
        for(int i=0;i<sample_pages.length;++i) {
            System.out.println( (i==markPg?"> ":"") + "Page #"+i);
            System.out.println( "--------" );
            for (int j = 0; j < sample_pages[i].length; ++j) {
                System.out.println(( j==markRec && i==markPg ?"\t> ":"") + sample_pages[i][j]);
            }
        }
    }

    private static Record[][] sample_pages;

    public static void sampleDb() {
        sample_pages = new Record[2][RECORDS_PER_PAGE];
        indexes = new int[2];

        indexes[0] = 100;
        indexes[1] = 600;

        for(int i=0;i<sample_pages[0].length; ++i)
            sample_pages[0][i] = new Record(50+i*30, "12345".getBytes());

        sample_pages[1][0] = new Record(600, "666".getBytes());
        sample_pages[1][1] = new Record(700, "123124".getBytes());
    }
}
