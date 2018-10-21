package pg.ppabis.sbd2;

import java.io.*;
import java.nio.ByteBuffer;

import static pg.ppabis.sbd2.Index.saveRecordCounters;

public class Page {

    public static final int RECORDS_PER_PAGE = 7;
    public static final int BLOCK_SIZE = RECORDS_PER_PAGE * Record.SIZE;
    public static final int ALPHA_RECORDS = (int)(RECORDS_PER_PAGE * 0.5);
    public static final int OVERFLOWS_PER_PAGE = 5;

    public static Record[] page;
    public static int currentPage = -1;
    public static Record[] overflow;

    public static int overflowRecords = 0;
    public static int mainRecords = 0;

    public static int stat_writes = 0;
    public static int stat_reads = 0;

    public static int findPlaceInPageForRecord(int id) {
        int i = 0;
        while (i < page.length && page[i] != null && page[i].getId()>0 && id > page[i].getId()) ++i;
        return i;
        /*while (i+1 < page.length && page[i+1] != null && page[i+1].getId() > 0 && page[i+1].getId() <= id) {
            ++i;
        }
        if( page[i].getId() < id && ( (i+1>=page.length) || (page[i+1] == null || page[i+1].getId() <= 0) ) ) ++i;
        return i;*/
    }

    public static Record getFromOverflow(int of) {
    	requestPageOA(of);
    	return page[of % RECORDS_PER_PAGE];
        //return overflow[of];
    }

    public static Record getFromPage(int pagen, int i) {
    	requestPage(pagen);
        //page = sample_pages[pagen]; //To be removed
        return page[i];
    }

    public static void setOnPage(int pagen, int i, Record r) {
    	requestPage(pagen);
        //page = sample_pages[pagen];
        page[i] = r;
        savePage();
    }

    public static void setInOverflow(int oa, Record r) {
    	requestPageOA(oa);
        page[oa % RECORDS_PER_PAGE] = r;
        savePage();
    }
    
    public static void pushPage(int Indexid) {
        moveOverflowsABlockDown();
    	Index.pushId(Indexid);
    	/*
        ArrayList<Record[]> tempPages = new ArrayList<Record[]>();
    	 tempPages.addAll(Arrays.asList(sample_pages));
    	 tempPages.add(new Record[RECORDS_PER_PAGE]);
    	 sample_pages = tempPages.toArray(sample_pages);*/
    }

    public static int putInOverflow(int id, byte[] data, int ov) {
    	requestPageOA(overflowRecords);
        page[overflowRecords % RECORDS_PER_PAGE] = new Record(id, data, ov);
        overflowRecords++;
        try {
            saveRecordCounters();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        savePage();
        return overflowRecords-1;
    }

    public static void moveOverflowsABlockDown(){
        int _stat_moved = 0;
        try {
            RandomAccessFile database = new RandomAccessFile(Main.FileName, "rw");
            long begin = Index.indexes.length * BLOCK_SIZE;
            byte[] buffer = new byte[BLOCK_SIZE];
            long end = database.length()-BLOCK_SIZE;
            while(end>=begin) {
                database.seek(end);
                database.read(buffer);
                database.seek(end+BLOCK_SIZE);
                database.write(buffer);
                end-=BLOCK_SIZE;
                _stat_moved++;
            }
            generateEmptyPage(buffer);
            database.seek(begin);
            database.write(buffer);
            database.close();
            System.out.println("Przeniesiono "+_stat_moved+" stron overflow.");
            stat_reads+=_stat_moved;
            stat_writes+=_stat_moved;
        } catch(IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public static void generateEmptyPage(byte[] out) {
        final byte[] record = new Record(0,new byte[]{}).toBytes();
        for(int i=0; i<RECORDS_PER_PAGE; ++i) {
            for(int j=0;j < record.length; ++j)
                out[i*Record.SIZE + j] = record[j];
        }
    }

    public static void requestPage(int p) {
    	if(currentPage == p) return;
    	byte[] binary = new byte[BLOCK_SIZE];
    	generateEmptyPage(binary);
    	try {
            RandomAccessFile file = new RandomAccessFile(Main.FileName,"r");
            file.seek(p * BLOCK_SIZE);
            file.read(binary);
            file.close();
    	} catch(IOException e) {
    	    System.err.println(e.getMessage());
        }
    	if(page == null) page = new Record[RECORDS_PER_PAGE];
    	ByteBuffer bb = ByteBuffer.wrap(binary);
    	for(int i=0; i < RECORDS_PER_PAGE; ++i) {
    		byte[] buffer = new byte[Record.SIZE];
    		bb.get(buffer);
    		page[i] = new Record(buffer);
    	}
    	currentPage = p;
    	stat_reads++;
    }
    
    public static void requestPageOA(int addr) {
    	int pageIndex = Index.indexes.length + (addr/RECORDS_PER_PAGE);
    	requestPage(pageIndex);
    }
    
    public static void savePage() {
        try {
            RandomAccessFile file = new RandomAccessFile(Main.FileName, "rw");
            file.seek(currentPage * BLOCK_SIZE);
            for (int i = 0; i < RECORDS_PER_PAGE; ++i) {
                if(page[i]==null || page[i].getId()==0) file.write(new Record(0,new byte[]{}).toBytes());
                else file.write(page[i].toBytes());
            }
            file.close();
            stat_writes++;
        } catch(IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public static void reorder() {
        int _page = 0, _ind = 0;
        int _stat_records = 0, _stat_deleted = 0;
        final byte[] _empty = new Record(0, new byte[] {0}).toBytes();
        try {
        	File newIndex = new File(Main.FileName+".index.tmp");
            File newDb = new File(Main.FileName+".tmp");
            FileOutputStream fos = new FileOutputStream(newDb);
            DataOutputStream ios = new DataOutputStream(new FileOutputStream(newIndex));
            ios.writeInt(0); ios.writeInt(0);
            for(int i=0; i < Index.indexes.length; ++i) {
                for(int j = 0; j < Page.RECORDS_PER_PAGE; ++j) {
                    Record r = Page.getFromPage(i, j);
                    if(r==null) continue;
                    int ov = r.overflow;
                    while(ov != Record.OVERFLOW_NONE) {
                        Record ovr = Page.getFromOverflow(ov);
                        ov = ovr.overflow;
                        if(ovr.isDeleted()) {_stat_deleted++; continue;}
                        if(ovr.getId()<=0) continue;
                        ovr.overflow = Record.OVERFLOW_NONE;
                        fos.write(ovr.toBytes());
                        _stat_records++;
                        if(_ind==0) ios.writeInt(ovr.getId());
                        _ind++;
                        if(_ind == ALPHA_RECORDS) {
                            _ind = 0;
                            _page++;
                            for(int k=0; k<RECORDS_PER_PAGE-ALPHA_RECORDS; ++k)
                                fos.write(_empty);
                        }
                    }
                    r = Page.getFromPage(i, j);
                    if(r.isDeleted()) {_stat_deleted++; continue;}
                    if(r.getId()<=0 || r.getId()==Integer.MAX_VALUE) continue;
                    r.overflow = Record.OVERFLOW_NONE;
                    fos.write(r.toBytes());
                    _stat_records++;
                    if(_ind==0) ios.writeInt(r.getId());
                    _ind++;
                    if(_ind == ALPHA_RECORDS) {
                        _ind = 0;
                        _page++;
                        for(int k=0; k<RECORDS_PER_PAGE-ALPHA_RECORDS; ++k)
                            fos.write(_empty);
                    }
                }
            }
            System.out.println("Wrote "+_page+" pages, "+_stat_records+" records and "+_stat_deleted+" were deleted");
            ios.close();
            for(;_ind<RECORDS_PER_PAGE-1;++_ind) {
            	fos.write(_empty);
            }
            fos.write(Record.straznik().toBytes());
            fos.close();
            RandomAccessFile fos2 = new RandomAccessFile(newDb, "rw");
            fos2.seek(fos2.length()-Record.SIZE);
            fos2.write(Record.straznik().toBytes());
            fos2.close();
            File originalIndex = new File(Main.FileName+".index");
            File originalDb = new File(Main.FileName);
            mainRecords = _stat_records;
            overflowRecords = 0;
            RandomAccessFile indexFile = new RandomAccessFile(newIndex, "rw");
            indexFile.seek(0);
            indexFile.writeInt(mainRecords);
            indexFile.writeInt(0);
            indexFile.close();
            Index.loadIndex(newIndex.getName());
            originalIndex.delete();
            originalDb.delete();
            try {Thread.sleep(200);} catch (Exception e) {}
            newIndex.renameTo(originalIndex);
            newDb.renameTo(originalDb);
            currentPage = -1;
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
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

        mainRecords = sample_pages[0].length+2;

        overflow = new Record[100];
        overflow[0] = new Record(580, "123412".getBytes());
        overflow[1] = new Record(570, "1234".getBytes(), 0);
        overflow[2] = new Record(20, "123455".getBytes(), 4);
        overflow[4] = new Record(25, "213124".getBytes(), 3);
        overflow[3] = new Record(30, "21312".getBytes());

        overflowRecords = 5;

    }

}
