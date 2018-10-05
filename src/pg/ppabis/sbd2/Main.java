package pg.ppabis.sbd2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import static pg.ppabis.sbd2.Page.*;
import static pg.ppabis.sbd2.Index.*;

public class Main {

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

    public static boolean updateRecordOA(int oa, Record r) {
        Record e = getFromOverflow(oa);
        if(e != null) {
            if(e.getId()!= r.getId()) return false;
            e.copyFrom(r);
        } else setInOverflow(oa, r);
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

    public static void find(int id) {
        int[] place = findPlaceForRecord(id);
        Record r = getFromPage(place[0], place[1]);
        if(r  == null) {
            System.out.println("[!]>Rekord o id "+id+" nie istnieje!");
        } else if(r.getId() == id && r.isDeleted()) {
            System.out.println("[i]>Rekord o id "+id+" jest usuniety.");
        } else if(r.getId() == id) {
            System.out.println("[i]>Rekord znaleziony: strona "+place[0]+"["+place[1]+"] "+r);
        } else {
            place = findInOverflow(id, r.overflow);
            if(isPlaceOnRecord(place))
                System.out.println("[!]>Rekord o id "+id+" nie istnieje!");
            else if(isPlaceOccupied(place)) {
                r = getFromOverflow(place[0]);
                if(r.getId() == id)
                     System.out.println("[i]>Rekord znaleziony w overflow #"+place[0]+" "+r);
                else System.out.println("[!]> Rekord o id "+id+" nie istnieje!");
            } else {
                System.out.println("[!]>Rekord o id "+id+" nie istnieje!");
            }
        }
    }

    public static void delete(int id) {
        int[] place = findPlaceForRecord(id);
        Record r = getFromPage(place[0], place[1]);
        if(r  == null) {
            System.out.println("[!]>Rekord o id "+id+" nie istnieje!");
        } else if(r.getId() == id && r.isDeleted()) {
            System.out.println("[i]>Rekord o id "+id+" jest usuniety.");
        } else if(r.getId() == id) {
            System.out.println("[i]>Usuwanie rekordu: strona "+place[0]+"["+place[1]+"] "+r);
            r.delete();
            updateRecord(place, r);
        } else {
            place = findInOverflow(id, r.overflow);
            if(isPlaceOnRecord(place))
                System.out.println("[!]>Rekord o id "+id+" nie istnieje!");
            else if(isPlaceOccupied(place)) {
                r = getFromOverflow(place[0]);
                if(r.getId() == id) {
                    System.out.println("[i]>Usuwanie rekordu w overflow #"+place[0]+" "+r);
                    r.delete();
                    updateRecordOA(place[0], r);
                } else {
                    System.out.println("[!]>Rekord o id "+id+" nie istnieje!");
                }
            } else {
                System.out.println("[!]>Rekord o id "+id+" nie istnieje!");
            }
        }
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        sampleDb();
        if (args.length > 0) {
            new Script(args[0]).run();
            if(args.length<2) return;
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
            printDb(res[0], res[1], ovs, true);

            int[] pl = insertRecord(id, (""+ new Random().nextInt(999999)).getBytes());
            printDb(pl[0], pl[1], pl, true);
        }
    }

    public static void printDb(int markPg, int markRec, int[] markOf, boolean follow) {
        for (int i = 0; i < sample_pages.length; ++i) {
            System.out.println((i == markPg ? "> " : "") + "Page #" + i);
            System.out.println("--------");
            for (int j = 0; j < sample_pages[i].length; ++j) {
                String ovString = "";
                if(follow && getFromPage(i, j) != null) {
                    int ov = getFromPage(i, j).overflow;
                    while(ov != Record.OVERFLOW_NONE) {
                        Record o = getFromOverflow(ov);
                        ovString+=" -> "+ov+":"+o.getId();
                        ov = o.overflow;
                    }
                }
                System.out.println((j == markRec && i == markPg ? "\t> " : "") + getFromPage(i, j) + ovString);
            }
        }
        if(follow) return;
        System.out.println();
        for (int i = 0; i < overflow.length; ++i)
            if (overflow[i] != null)
                System.out.println(i+"\t"+(markOf[0] == i || markOf[1] == i ? "\t> " : "") + "#" + overflow[i]);
    }


}
