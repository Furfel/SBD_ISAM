package pg.ppabis.sbd2;

import java.util.Random;

public class Main {

    public static final int RECORDS_PER_PAGE = 7;
    public static final int BLOCK_SIZE = RECORDS_PER_PAGE * Record.SIZE;

    public static int[] indexes;
    public static Record[] page;
    public static Record[] overflow;

    public static int overflowRecords = 0;
    public static int mainRecords = 0;

    public static int findPageNumberForRecord(int id) {
        int i = 0;
        while (i + 1 < indexes.length && indexes[i + 1] <= id) ++i;
        return i;
    }

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

    public static int[] findInOverflow(int id, int of) {
        if (of == Record.OVERFLOW_NONE) return new int[]{-1, -1}; //Main record has no overflows
        int left = -1;
        int right = of;
        Record r = null;
        do {
            r = getFromOverflow(right);
            if (r.getId() == id) return new int[]{right, right}; //This record was found
            if (r.getId() > id) return new int[]{left, right}; //Found a larger, so return left and right
            left = right;
            right = r.overflow;
            if (right == Record.OVERFLOW_NONE) return new int[]{left, right}; //Return last address and -1
        } while (true);
    }

    public static int putInOverflow(int id, byte[] data, int ov) {
        overflow[overflowRecords] = new Record(id, data, ov);
        overflowRecords++;
        return overflowRecords-1;
    }

    public static int[] findPlaceForRecord(int id) {
        int pagen = findPageNumberForRecord(id);
        page = sample_pages[pagen]; //To be removed
        int place = findPlaceInPageForRecord(id);
        if (place >= RECORDS_PER_PAGE) {
            pagen++;
            place = 0;
        }
        return new int[]{pagen, place};
    }

    public static boolean isPlaceOnRecord(int[] place) {                return place[0] == -1 && place[1] == -1; }
    public static boolean isPlaceOccupied(int[] place) {                return place[0] == place[1]; }
    public static boolean isPlaceAtTheBeginningOfChain(int[] place) {   return place[0] == -1 && place[1] > -1; }
    public static boolean isPlaceAtTheEndOfChain(int[] place) {         return place[0] > -1 && place[1] == -1; }

    public static boolean updateRecord(int[] place, Record r) {
        Record e = getFromPage(place[0], place[1]);
        if(e != null) {
            if(e.getId() != r.getId()) return false;
            e.copyFrom(r);
        }
        else setOnPage(place[0], place[1], r);
        return true;
    }

    public static boolean updateOverflowAddress(int[] place, int ov) {
        Record e = getFromPage(place[0], place[1]);
        if(e == null || e.getId() <= 0) return false;
        e.overflow = ov;
        return true;
    }

    public static boolean updadteOverflowAddressOA(int placeOA, int ov) {
        Record e = getFromOverflow(placeOA);
        if(e == null || e.getId() <= 0) return false;
        e.overflow = ov;
        return true;
    }

    public static int[] insertRecord(int id, byte[] data) {
        int[] place = findPlaceForRecord(id);
        Record r = getFromPage(place[0], place[1]);
        if(r != null && r.getId() == id) {
            System.out.println("!>Rekord juz jest!");
            return place;
        }
        if(r==null || r.getId()<=0) {
            System.out.println("Wstawianie na stronie "+place[0]+"["+place[1]+"]");
            updateRecord(place, new Record(id, data, -1));
        } else {
            int[] placeOA = findInOverflow(id, r.overflow);
            if( isPlaceOnRecord(placeOA) ) {
                int newRec = putInOverflow(id, data, -1);
                updateOverflowAddress(place, newRec);
            } else if( isPlaceAtTheBeginningOfChain(placeOA) ) {
                int newRec = putInOverflow(id, data, placeOA[1]);
                updateOverflowAddress(place, newRec);
            } else if( isPlaceOccupied(placeOA) ) {

            } else {
                int newRec = putInOverflow(id, data, placeOA[1]);
                updadteOverflowAddressOA(placeOA[0], newRec);
            }
        }
        return place;
    }

    public static void main(String[] args) {
        sampleDb();
        if (args.length > 0) {
            int id = Integer.parseInt(args[0]);
            int[] res = findPlaceForRecord(id);
            int[] ovs = {-2, -3};
            if(getFromPage(res[0], res[1])!=null) {
                ovs = findInOverflow(id, getFromPage(res[0], res[1]).overflow);

                if (isPlaceOnRecord(ovs)) {
                    System.out.println("Sprawdzamy rekord glowny");
                    if (id == getFromPage(res[0], res[1]).getId()) {
                        System.out.println("To jest ten rekord! " + getFromPage(res[0], res[1]));
                    } else {
                        System.out.println("Nie ma overflow więc wstawianie za nim " + getFromPage(res[0], res[1]).getId() + "!");
                    }
                } else if (isPlaceOccupied(ovs)) {
                    Record r = getFromOverflow(ovs[1]);
                    System.out.println("Znaleziony jest w overflow [" + ovs[1] + "] #" + r);
                } else if (isPlaceAtTheBeginningOfChain(ovs)) {
                    Record r = getFromOverflow(ovs[1]);
                    System.out.println("Wstawianie zaraz po rekordzie glownym " + sample_pages[res[0]][res[1]].getId() + " > " + id + " < " + r.getId());
                } else if (isPlaceAtTheEndOfChain(ovs)) {
                    Record r = getFromOverflow(ovs[0]);
                    System.out.println("Wstawianie na koncu lancucha " + r.getId() + " < " + id);
                } else {
                    Record r1 = getFromOverflow(ovs[0]);
                    Record r2 = getFromOverflow(ovs[1]);
                    System.out.println("Wstawianie pomiedzy [" + ovs[0] + ", " + ovs[1] + "] czyli {" + r1.getId() + " < " + id + " < " + r2.getId() + " }");
                }
            }
            printDb(res[0], res[1], ovs);

            int[] pl = insertRecord(id, (""+ new Random().nextInt(999999)).getBytes());
            printDb(pl[0], pl[1], pl);
        }
    }

    public static void printDb(int markPg, int markRec, int[] markOf) {
        for (int i = 0; i < sample_pages.length; ++i) {
            System.out.println((i == markPg ? "> " : "") + "Page #" + i);
            System.out.println("--------");
            for (int j = 0; j < sample_pages[i].length; ++j) {
                System.out.println((j == markRec && i == markPg ? "\t> " : "") + sample_pages[i][j]);
            }
        }
        System.out.println();
        for (int i = 0; i < overflow.length; ++i)
            if (overflow[i] != null)
                System.out.println(i+"\t"+(markOf[0] == i || markOf[1] == i ? "\t> " : "") + "#" + overflow[i]);
    }

    private static Record[][] sample_pages;

    public static void sampleDb() {
        sample_pages = new Record[2][RECORDS_PER_PAGE];
        indexes = new int[2];

        indexes[0] = 100;
        indexes[1] = 600;

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
