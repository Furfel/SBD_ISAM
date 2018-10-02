package pg.ppabis.sbd2;

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
        this.data = data.clone();
    }

    public Record(int id, byte[] data, int overflow) {
        this(id, data);
        this.overflow = overflow;
    }

    public int getId() {
        return id & ~DELETE_MASK;
    }

    public boolean isDeleted() {
        return (id & DELETE_MASK) > 0;
    }

    public void delete() {
        id |= DELETE_MASK;
    }

    public void undelete() {
        id &= ~DELETE_MASK;
    }

    @Override
    public String toString() {
        return getId()+": "+new String(data)+(overflow==OVERFLOW_NONE?"":" -> "+overflow);
    }
}
