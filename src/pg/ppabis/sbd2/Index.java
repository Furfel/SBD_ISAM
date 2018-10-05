package pg.ppabis.sbd2;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

public class Index {
    public static int[] indexes;

    public static int findPageNumberForRecord(int id) {
        int i = 0;
        while (i + 1 < indexes.length && indexes[i + 1] <= id) ++i;
        return i;
    }

    public static void pushId(int id) {
    	ArrayList<Integer> indexesTemp = new ArrayList<Integer>();
    	for(int i = 0; i<indexes.length; ++i)
    		indexesTemp.add(Integer.valueOf(indexes[i]));
    	indexesTemp.add(id);
    	indexes = indexesTemp.stream().mapToInt(i -> i).toArray();
    }

    public static void loadIndex(String file) throws IOException {
        ArrayList<Integer> indexesTemp = new ArrayList<>();
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        while(dis.available()>=4) {
            int id=dis.readInt();
            indexesTemp.add(Integer.valueOf(id));
        }
        indexes = indexesTemp.stream().mapToInt(i -> i).toArray();
        System.out.println("[i]>Index: wczytano "+indexes.length+" indeksow");
    }

    public static void createEmptyIndex() {
        indexes = new int[0];
    }

    public static boolean isEmpty() {
        return indexes == null || indexes.length<=0;
    }
    
}
