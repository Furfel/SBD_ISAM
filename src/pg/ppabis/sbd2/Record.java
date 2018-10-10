package pg.ppabis.sbd2;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Record {

    public static final int OVERFLOW_NONE=-1;
    public static final int DELETE_MASK = 0x80000000;
    public static final int SIZE = 30 + 4 + 4;

    byte[] data;
    int id, overflow;

    public Record() {
        id = 0;
        overflow = OVERFLOW_NONE;
        data = new byte[30];
    }

    public Record(int id, byte[] data) {
        this();
        this.id = id;
        this.data = Arrays.copyOf(data, this.data.length);
    }

    public Record(int id, byte[] data, int overflow) {
        this(id, data);
        this.overflow = overflow;
    }

    public Record(Record r) {
        this();
        copyFrom(r);
    }

    public void copyFrom(Record r) {
        if(r==null) return;
        id = r.getId();
        data = r.data.clone();
        overflow = r.overflow;
        if(r.isDeleted()) delete();
    }
    
    public Record(byte[] binary) {
    	this();
    	ByteBuffer bb = ByteBuffer.wrap(binary);
    	id = bb.getInt();
    	overflow = bb.getInt();
    	bb.get(data, 0, 30);
    }
    
    public byte[] toBytes() {
    	ByteBuffer bb = ByteBuffer.allocate(SIZE);
    	bb.putInt(id);
    	bb.putInt(overflow);
    	bb.put(data);
    	return bb.array();
    }

    public int getId() {
        return id & ~DELETE_MASK;
    }

    public boolean isDeleted() {
        return (id & DELETE_MASK) != 0;
    }

    public void delete() {
        id |= DELETE_MASK;
    }

    public void undelete() {
        id &= ~DELETE_MASK;
    }

    @Override
    public String toString() {
        return getId()+": "+new String(data)+(overflow==OVERFLOW_NONE?"":" >>"+overflow);
    }
    
    public static Record straznik() {
    	return new Record(Integer.MAX_VALUE, new byte[] {1,2,3});
    }
    
}
