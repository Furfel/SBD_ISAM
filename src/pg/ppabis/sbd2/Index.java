package pg.ppabis.sbd2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    
}
