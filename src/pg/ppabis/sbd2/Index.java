package pg.ppabis.sbd2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class Index {
    public static int[] indexes;
    public static String currentFile = "";

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
    	try {saveIndex();}
    	catch(IOException e) {System.err.println("Error saving index "+e.getMessage());}
    }

    public static void saveIndex() throws IOException {
    	DataOutputStream dos = new DataOutputStream(new FileOutputStream(currentFile));
    	dos.writeInt(Page.mainRecords);
    	dos.writeInt(Page.overflowRecords);
    	for(int i=0;i<indexes.length;++i)
    		dos.writeInt(indexes[i]);
    	dos.close();
    }
    
    public static void loadIndex(String file) throws IOException {
    	currentFile = file;
        ArrayList<Integer> indexesTemp = new ArrayList<>();
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        if(dis.available()>=4) Page.mainRecords = dis.readInt();
        if(dis.available()>=4) Page.overflowRecords = dis.readInt();
        while(dis.available()>=4) {
            int id=dis.readInt();
            indexesTemp.add(Integer.valueOf(id));
        }
        dis.close();
        indexes = indexesTemp.stream().mapToInt(i -> i).toArray();
        System.out.println("[i]>Index: wczytano "+indexes.length+" indeksow");
    }

    public static void saveRecordCounters() throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(Main.FileName+".index", "rw");
        randomAccessFile.seek(0);
        randomAccessFile.writeInt(Page.mainRecords);
        randomAccessFile.writeInt(Page.overflowRecords);
        randomAccessFile.close();
    }

    public static void createEmptyIndex() {
        indexes = new int[0];
    }

    public static boolean isEmpty() {
        return indexes == null || indexes.length<=0;
    }
    
}
