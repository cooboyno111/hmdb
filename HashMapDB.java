import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Single java class NoSQL database USE java NIO
 * code by luqg
 * pc hdd 500G test the class。
 * all db size is 392MB USE Single Thread.
 * 100000 add -------------------- 160 secend。
 * 625 OPS/S。
 * 100000 update -------------------- 80 secend。
 * 1250 OPS/S.
 * 100000 get -------------------- 6 secend。
 * 16666 OPS/S.
 * use same as leveldb。
 */
class DELTAG {
	Integer key;
	Long seek;

	public DELTAG(Integer key, Long seek) {
		this.key = key;
		this.seek = seek;
	}
}

public class HashMapDB {
	private String loc = "c://amd/hashmapkv.db";
	private String indloc = "c://amd/hashmapkv.ind";
	private String delloc = "c://amd/hashmapkv.del";
	// 主数据结构为HashMap<Integer, Long> 第一个Integer key.hashCode,第二个Long为其数据在db中的索引
	private ConcurrentHashMap<Integer, Long> dbmap = new ConcurrentHashMap<Integer, Long>();
	private ArrayList<DELTAG> delchunk = new ArrayList<DELTAG>();
	private final static int CHUNKHEADLEN = 21;
	private final static int MAXDLEN = 4075;

	private byte[] sbyte = new byte[MAXDLEN];
	private byte[] zero = new byte[MAXDLEN];

	public HashMapDB() {
		index_read();
		for (int i = 0; i < delchunk.size(); i++) {
			DELTAG d = delchunk.get(i);
			Integer key = d.key;
			dbmap.remove(key);
		}
		System.out.println("dbmap len=" + dbmap.size());
	}

	synchronized  public boolean put(String key, byte[] data) {
		Long seek = dbmap.get(key.hashCode());
		if (seek != null) {
			return update(key, data);
		} else {
			return add(key, data);
		}
	}

	// 向数据库增加数据 data长度必须小于MAXDLEN,超过部分会被丢弃
	synchronized public boolean add(String key, byte[] data) {
		// 1清空sbyte
		System.arraycopy(zero, 0, sbyte, 0, MAXDLEN);
		// 2 data 2 sbyte
		System.arraycopy(data, 0, sbyte, 0, data.length);
		long time = System.currentTimeMillis();
		// 3 写入数据库文件
		if (delchunk.size() == 0) {
			long seek = append_Nio(loc, dbmap.size(), key.hashCode(), (byte) 0, time, data.length, sbyte, -1);
			// System.out.println(key+" seek=" + seek);
			dbmap.put(key.hashCode(), seek);
			// 这块可以直接append_index_Nio
			append_index_Nio(key.hashCode(), seek);
		} else {
			DELTAG tm = delchunk.remove(0);
			Long tseek = tm.seek;
			long seek = append_Nio(loc, dbmap.size(), key.hashCode(), (byte) 0, time, data.length, sbyte, tseek);
			// System.out.println(key+" seek=" + seek);
			dbmap.put(key.hashCode(), seek);
			// 更新索引文件
			index_write_Nio();
			del_write_Nio();
		}
		return true;
	}

	synchronized public byte[] get(String key) {
		Long seek = dbmap.get(key.hashCode());
		if (seek != null) {
			return read_Nio(loc, seek);
		} else {
			return null;
		}
	}

	synchronized public boolean update(String key, byte[] data) {
		Long seek = dbmap.get(key.hashCode());
		if (seek != null) {
			// 1清空sbyte
			System.arraycopy(zero, 0, sbyte, 0, MAXDLEN);
			// 2 data 2 sbyte
			System.arraycopy(data, 0, sbyte, 0, data.length);
			long time = System.currentTimeMillis();
			return update_Nio(loc, seek, key.hashCode(), (byte) 0, time, data.length, sbyte);
		} else {
			return false;
		}
	}

	synchronized public boolean delete(String key) {
		Long seek = dbmap.get(key.hashCode());
		if (seek != null) {
			// 1清空sbyte
			System.arraycopy(zero, 0, sbyte, 0, MAXDLEN);
			long time = System.currentTimeMillis();
			dbmap.remove(key.hashCode());
			index_write_Nio();
			delchunk.add(new DELTAG(key.hashCode(), seek));
			// delchunk.add(key.hashCode() + ":" + seek);
			del_write_Nio();
			return update_Nio(loc, seek, key.hashCode(), (byte) 1, time, sbyte.length, sbyte);
		} else {
			return false;
		}
	}

	public final void writeInt(OutputStream out, int v) throws IOException {
		out.write((v >>> 24) & 0xFF);
		out.write((v >>> 16) & 0xFF);
		out.write((v >>> 8) & 0xFF);
		out.write((v >>> 0) & 0xFF);
	}

	public final void writeLong(OutputStream out, long v) throws IOException {
		out.write((int) (v >>> 56) & 0xFF);
		out.write((int) (v >>> 48) & 0xFF);
		out.write((int) (v >>> 40) & 0xFF);
		out.write((int) (v >>> 32) & 0xFF);
		out.write((int) (v >>> 24) & 0xFF);
		out.write((int) (v >>> 16) & 0xFF);
		out.write((int) (v >>> 8) & 0xFF);
		out.write((int) (v >>> 0) & 0xFF);
	}

	public final int readInt(InputStream in) throws IOException {
		int ch1 = in.read();
		int ch2 = in.read();
		int ch3 = in.read();
		int ch4 = in.read();
		if ((ch1 | ch2 | ch3 | ch4) < 0)
			throw new EOFException();
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	private byte readBuffer[] = new byte[8];

	public final long readLong(InputStream in) throws IOException {
		in.read(readBuffer, 0, 8);
		return (((long) readBuffer[0] << 56) + ((long) (readBuffer[1] & 255) << 48)
				+ ((long) (readBuffer[2] & 255) << 40) + ((long) (readBuffer[3] & 255) << 32)
				+ ((long) (readBuffer[4] & 255) << 24) + ((readBuffer[5] & 255) << 16) + ((readBuffer[6] & 255) << 8)
				+ ((readBuffer[7] & 255) << 0));
	}

	public void print() {
		System.out.println("hmdb print");
		System.out.println("hmdb size:" + dbmap.size());
		Iterator<Map.Entry<Integer, Long>> iterator = dbmap.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Integer, Long> entry = iterator.next();
			Integer key = entry.getKey();
			Long value = entry.getValue();
			System.out.println(key + ":" + value);
		}
		System.out.println("delchunk size:" + delchunk.size());
		for (int i = 0; i < delchunk.size(); i++) {
			System.out.println(delchunk.get(i).key + ":" + delchunk.get(i).seek);
		}
	}

	synchronized private void index_read() {
		dbmap.clear();
		delchunk.clear();
		try {
			FileInputStream fis = new FileInputStream(delloc);
			// DataInputStream dis = new DataInputStream(fis);
			BufferedInputStream dis = new BufferedInputStream(fis, 1024);
			int dellen = readInt(dis);
			System.out.println("delchunk len=" + dellen);
			for (int i = 0; i < dellen; i++) {
				Integer key = (int) readLong(dis);
				Long val = readLong(dis);
				delchunk.add(new DELTAG(key, val));
			}
			dis.close();
			fis.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
		try {
			FileInputStream fis = new FileInputStream(indloc);
			// DataInputStream dis = new DataInputStream(fis);
			BufferedInputStream dis = new BufferedInputStream(fis, 1024);
			int dbmaplen = dis.available() / 16;
			System.out.println("dbmap len=" + dbmaplen);
			for (int i = 0; i < dbmaplen; i++) {
				Integer key = (int) readLong(dis);
				Long val = readLong(dis);
				dbmap.put(key, val);
			}
			dis.close();
			fis.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	synchronized public void del_write_Nio() {
		System.out.println("hmdb del_write_Nio - " + dbmap.size());
		System.out.println("hmdb del_write_Nio dc - " + delchunk.size());
		int dellen = delchunk.size() * 16 + 4;
		FileChannel fileChannel = null;
		ByteBuffer buffer = ByteBuffer.allocate(dellen);
		try {
			fileChannel = FileChannel.open(Paths.get(delloc), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			buffer.putInt(delchunk.size());
			for (int i = 0; i < delchunk.size(); i++) {
				buffer.putLong(delchunk.get(i).key);
				buffer.putLong(delchunk.get(i).seek);
			}
			buffer.flip();
			fileChannel.write(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	synchronized public void append_index_Nio(Integer key, Long value) {
		FileChannel fileChannel = null;
		ByteBuffer buffer = ByteBuffer.allocate(16);
		try {
			fileChannel = FileChannel.open(Paths.get(indloc), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			fileChannel.position(fileChannel.size());
			buffer.putLong(key);
			buffer.putLong(value);
			buffer.flip();
			fileChannel.write(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	synchronized public void index_write_Nio() {
		System.out.println("hmdb index_write");
		int maplen = dbmap.size() * 16;
		FileChannel fileChannel = null;
		ByteBuffer buffer = ByteBuffer.allocate(maplen);
		try {
			fileChannel = FileChannel.open(Paths.get(indloc), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			// buffer.putInt(dbmap.size());
			Iterator<Map.Entry<Integer, Long>> iterator = dbmap.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<Integer, Long> entry = iterator.next();
				Integer key = entry.getKey();
				Long value = entry.getValue();
				buffer.putLong(key);
				buffer.putLong(value);
			}
			buffer.flip();
			fileChannel.write(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	synchronized private byte[] read_Nio(String loc, Long seek) {
		FileChannel fileChannel = null;
		FileLock fl=null;
		byte[] data = null;
		ByteBuffer buffer = ByteBuffer.allocate(CHUNKHEADLEN + MAXDLEN);
		try {
			fileChannel = FileChannel.open(Paths.get(loc), StandardOpenOption.READ,StandardOpenOption.WRITE);
			fl=fileChannel.tryLock(seek,  CHUNKHEADLEN + MAXDLEN, false);
			if(fl!=null){
			fileChannel.position(seek);// 定位到seek位置
			fileChannel.read(buffer);
			buffer.flip();
			long did = buffer.getLong();
			// System.out.println(did);
			byte del = buffer.get();
			// System.out.println(del);
			long time = buffer.getLong();
			// System.out.println(time);
			int datalen = buffer.getInt();
			// System.out.println(datalen);
			// buffer.get(srbyte, 0, MAXDLEN);
			data = new byte[datalen];
			// System.arraycopy(srbyte, 0, data, 0, datalen);
			buffer.get(data, 0, datalen);
			fl.release();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (OverlappingFileLockException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
		} finally {
			if(fl != null) {  
                try {  
                    fl.release();  
                    fl = null;  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
		    }
			if (fileChannel != null) {
				try {
					fileChannel.close();
					fileChannel=null;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return data;
	}

	synchronized  private boolean update_Nio(String loc, Long seek, long dbid, byte del, long time, int data_len, byte[] val) {
		FileChannel fileChannel = null;
		FileLock fl=null;
		ByteBuffer buffer = ByteBuffer.allocate(CHUNKHEADLEN + MAXDLEN);
		try {
			fileChannel = FileChannel.open(Paths.get(loc), StandardOpenOption.READ,StandardOpenOption.WRITE);
			fl=fileChannel.tryLock(seek, CHUNKHEADLEN + MAXDLEN, false);//对此文件进行加锁(写锁读不锁)
			//fl=fileChannel.lock(seek,  CHUNKHEADLEN + MAXDLEN, false);
			if(fl!=null){
			fileChannel.position(seek);// 定位到seek位置
			buffer.putLong(dbid);
			buffer.put(del);
			buffer.putLong(time);
			buffer.putInt(data_len);
			buffer.put(val);
			buffer.flip();
			fileChannel.write(buffer);
//			fl.release();
			}
			// ByteArrayOutputStream baos = new ByteArrayOutputStream();
			// writeLong(baos, dbid);// 写dbid
			// baos.write(del);// 写删除标记
			// writeLong(baos, time);// 写更新时间
			// writeInt(baos, data_len);
			// baos.write(val);
			// byte[] data = baos.toByteArray();
			// fileChannel.write(ByteBuffer.wrap(data, 0, data.length));
			// baos.close();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (OverlappingFileLockException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			return false;
		} finally {
		    if(fl != null) {  
                try {  
                    fl.release();  
                    fl = null;  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
		    }
			if (fileChannel != null) {
				try {
					fileChannel.close();
					fileChannel=null;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private Long append_Nio(String loc, int dblen, long dbid, byte del, long time, int data_len, byte[] val,
			long tseek) {
		FileChannel fileChannel = null;
		FileLock fl=null;
		long seek = 0;
		ByteBuffer buffer = ByteBuffer.allocate(CHUNKHEADLEN + MAXDLEN);
		try {
			fileChannel = FileChannel.open(Paths.get(loc), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			fl=fileChannel.tryLock(seek, CHUNKHEADLEN + MAXDLEN, true);
			//FileLock fl=fileChannel.lock(seek,  CHUNKHEADLEN + MAXDLEN, true);
			if(fl!=null){
			// fileChannel.position(0);// 定位到文件头
			// ByteArrayOutputStream baos = new ByteArrayOutputStream();
			// writeInt(baos, 12342345);// 写magic//
			// writeInt(baos, 0);// 写版本号0
			// writeLong(baos, dblen);// 写数据库长度
			// fileChannel.write(ByteBuffer.wrap(baos.toByteArray()));
			if (tseek == -1) {
				seek = fileChannel.size();
				fileChannel.position(fileChannel.size());// 定位到文件末尾
			} else {
				seek = tseek;
				fileChannel.position(tseek);
			}
			buffer.putLong(dbid);
			buffer.put(del);
			buffer.putLong(time);
			buffer.putInt(data_len);
			buffer.put(val);
			buffer.flip();
			// writeLong(baos, dbid);// 写dbid
			// baos.write(del);// 写删除标记
			// writeLong(baos, time);// 写更新时间
			// writeInt(baos, data_len);
			// baos.write(val);
			// byte[] data = baos.toByteArray();
			// fileChannel.write(ByteBuffer.wrap(data, HEADLEN, data.length -
			// HEADLEN));
			fileChannel.write(buffer);
			// baos.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (OverlappingFileLockException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
		} finally {
			if(fl != null) {  
                try {  
                    fl.release();  
                    fl = null;  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
		    }
			if (fileChannel != null) {
				try {
					fileChannel.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return seek;
	}

	public int size() {
		return this.dbmap.size();
	}

	public static void print(byte[] data) {
		if (data == null) {
			System.out.println("data=" + null);
		} else {
			System.out.println("data=" + new String(data));
		}
	}
/*
	public static void main(String[] args) {
		long time = System.currentTimeMillis();
		HashMapDB mhdb = new HashMapDB();
		mhdb.put("1", "goodogaosgo".getBytes());
		// mhdb.put("2", "goodogaosgo".getBytes());
		mhdb.put("3", "goodogaosgo".getBytes());
		mhdb.delete("2");
		mhdb.put("4", "asdfasdfasdfgoodogaosgo".getBytes());
		// mhdb.print();
		print(mhdb.get("4"));
		 for (int i = 0; i < 100000; i++) {
		 //boolean ok = mhdb.put("" + i, "goodogaosgo".getBytes());
		 //System.out.println("dbid=" + ok);
		 print(mhdb.get("" + i));
		 // mhdb.get("" + i);
		 }
		// mhdb.index_write_Nio();
		// mhdb.put("2", "goodogaosgo".getBytes());
		// mhdb.print();
		print(mhdb.get("2"));
		// mhdb.update("2", "asdfasdfasdfgoodogaosgo".getBytes());
		// byte[] dat = mhdb.get("2");
		// print(dat);
		// mhdb.delete("2");
		// byte[] dat2 = mhdb.get("2");
		// print(dat2);
		long utime = System.currentTimeMillis() - time;
		System.out.println("-------------------- size=" + mhdb.size());
		System.out.println("-------------------- utime=" + utime);
	}
*/	
	public static void main(String[] args) {

		long time = System.currentTimeMillis();
		HashMapDB mhdb = new HashMapDB();

		for (int i = 0; i < 100000; i++) {
				 boolean ok = mhdb.put("" + i, "goodogaosgo".getBytes());
				 //System.out.println("dbid=" + ok);
	    }
		long utime = System.currentTimeMillis() - time;
		 System.out.println("-------------------- utime 1=" + utime);
		 time = System.currentTimeMillis();
		 for (int i = 0; i < 100000; i++) {
//			 mhdb.get("" + i);
		 boolean ok = mhdb.put("" + i, "goodogaosgo".getBytes());
		// System.out.println("dbid=" + ok);
		 }			
		  utime = System.currentTimeMillis() - time;
		 System.out.println("-------------------- utime 2=" + utime);
		 
		 time = System.currentTimeMillis();
		 for (int i = 0; i < 100000; i++) {
			 mhdb.get("" + i);
		 }
		  utime = System.currentTimeMillis() - time;
		 System.out.println("-------------------- utime 3=" + utime);
	}
}
