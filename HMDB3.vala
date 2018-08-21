//valac --pkg gio-2.0 HMDB3.vala -o hm3
public class HMDB {
  private string loc="hmkv.db";
  private string indloc="hmkv.ind";
  private HashTable<uint, int> dbmap;
  private List<int> dellist;
  //HashTable<string, int> table = new HashTable<string, int> (str_hash, str_equal);
  private int dbind=0;
  private int MAXDLEN=4075;
  private char[] sbyte=new char[4075];
  public HMDB() {
   dbmap = new HashTable<uint, int>(direct_hash, direct_equal);
   dellist=new List<int>();
   index_read();
   var size=dbmap.size();
   stdout.printf(@"index_read final dbmap size=$size\n");
  }
  public void out_print(){
    var size=dbmap.size();
    stdout.printf(@"dbmap size=$size\n");
    /*
    foreach (int val in dbmap.get_values()) {
        stdout.printf ("%d\n", val);
    }
    foreach (int key in dbmap.get_keys ()) {
	stdout.printf ("%d\n", key);
        var val =dbmap.get(key);
        stdout.printf ("%d\n", val);
    }*/
    dbmap.foreach ((key, val) => {
	print ("%llu => %d\n", key, val);
    });
    int len=(int)dellist.length();
    print ("dellist len=%d\n",len);
    dellist.foreach ((val) => {
	print ("%llu\n",val);
    });
  }
  public int size(){
      return (int)dbmap.size();
  }
  public void index_read(){
     File file = File.new_for_path (indloc);
     bool tmp = file.query_exists ();
     if (tmp == true) {
	try {
		FileIOStream stream = file.open_readwrite ();
		DataInputStream dis = new DataInputStream (stream.input_stream);
                dbind= (int)dis.read_uint32();
                print ("dbind=%d\n",dbind);
                int len= (int)dis.read_uint32();
                print ("index len=%d\n",len);
                for (int i=0; i<len; i++) {
                   int dbid=(int)dis.read_uint32();
                   int seek=(int)dis.read_uint32();
                   dbmap.insert(dbid,seek);
                }
                int dlen= (int)dis.read_uint32();
                print ("dellist len=%d\n",dlen);
                for (int i=0; i<dlen; i++) {
                   int seek=(int)dis.read_uint32();
                   dellist.append(seek);
                }
	} catch (Error e) {
		print ("Error: %s\n", e.message);
	}
     }
  }
  public void index_write(){
      try {
           FileIOStream stream;
           File file = File.new_for_path (indloc);
           bool tmp = file.query_exists ();
           if(tmp==false){
             stream = file.create_readwrite(FileCreateFlags.PRIVATE);
           }else{
             stream = file.open_readwrite ();
           }
           DataOutputStream dos = new DataOutputStream (stream.output_stream);
           //写dbind
           dos.put_uint32 (dbind);
           //写dbmap长度
           dos.put_uint32 (dbmap.size());
           //写dbmap内容
           dbmap.foreach ((key, val) => {
		//print ("index_write %d => %d\n", key, val);
                dos.put_uint32(key);
                dos.put_uint32(val);
           });
           //写dellist长度
           dos.put_uint32 (dellist.length());
           //写dellist内容
           dellist.foreach ((val) => {
	      dos.put_uint32(val);
           });
      } catch (Error e) {
	print ("Error: %s\n", e.message);
      }
  }
  public void read_file(int seek){
     File file = File.new_for_path (loc);
      bool tmp = file.query_exists ();
      if (tmp == true) {
	try {
		FileIOStream stream = file.open_readwrite ();
                stream.seek (seek, SeekType.CUR);
		DataInputStream dis = new DataInputStream (stream.input_stream);
                int dbid= (int)dis.read_uint32();
                print ("dbid=%d\n",dbid);
                int dlen= (int)dis.read_uint32();
                print ("dlen=%d\n",dlen);
                if(dlen!=-1){
                 uint8[] data =new uint8[dlen];
                 for (int i=0; i<dlen; i++) {
                    data[i]=dis.read_byte();
                 }
                 print ("read_file-%s\n",(string)data);
                }else{
                 print ("delete chunk-%d\n",seek);
                }
	} catch (Error e) {
		print ("Error: %s\n", e.message);
	}
      }
  }
 public void update_file(int seek,uint dbid,char [] data,int dlen){
     File file = File.new_for_path (loc);
      bool tmp = file.query_exists ();
      if (tmp == true) {
	try {
		FileIOStream stream = file.open_readwrite ();
                stream.seek (seek, SeekType.CUR);
                DataOutputStream dos = new DataOutputStream (stream.output_stream); 
		//写dbid
                dos.put_uint32 (dbid);
                //写数据长度
                dos.put_uint32 (dlen);
                //写块内容
                for (int i=0; i<data.length; i++) {
                    dos.put_byte(data[i]);
                }
	} catch (Error e) {
		print ("Error: %s\n", e.message);
	}
      }
  }
 public void delete_file(int seek){
     File file = File.new_for_path (loc);
      bool tmp = file.query_exists ();
      if (tmp == true) {
	try {
		FileIOStream stream = file.open_readwrite ();
                stream.seek (seek, SeekType.CUR);
                DataOutputStream dos = new DataOutputStream (stream.output_stream); 
		//写dbid
                dos.put_uint32 (-1);
                //写数据长度
                dos.put_uint32 (-1);
	} catch (Error e) {
		print ("Error: %s\n", e.message);
	}
      }
  }
  public void append_file(uint dbid,char [] data,int dlen){
        FileIOStream stream;
	try {
		File file = File.new_for_path (loc);
	        bool tmp = file.query_exists ();
	        if (tmp == true) {
		 //print ("File exists\n");
                 //FileOutputStream os = file.append_to (FileCreateFlags.PRIVATE);
                 //DataOutputStream dos = new DataOutputStream (os);
                 stream = file.open_readwrite ();
                 stream.seek (0, SeekType.END);
	        } else {
		 //print ("File does not exist\n");
                 stream = file.create_readwrite(FileCreateFlags.PRIVATE);
	        }
                DataOutputStream dos = new DataOutputStream (stream.output_stream); 
		//写dbid
                dos.put_uint32 (dbid);
                //写数据长度
                dos.put_uint32 (dlen);
                //写块内容
                for (int i=0; i<data.length; i++) {
                    dos.put_byte(data[i]);
                }
	} catch (Error e) {
		print ("Error: %s\n", e.message);
	}
  }
  public int put(string key,char[] data){
     uint hash=key.hash();
     bool has=dbmap.contains(hash);
     if(has){
      int seek=dbmap.get(hash);
      print ("update chunk\n");
      update(key,data);
     }else{
      if(dellist.length()==0){
        print ("add chunk\n");
        add(key,data);
      }else{
        print ("update chunk\n");
        int topseek = dellist.nth_data(0);
        print ("topseek %d\n",topseek);
        dellist.remove(topseek);
        //uint lastseek = dellist.nth_data(0);
        //print ("lastseek %d\n",lastseek);
        update_file(topseek,hash,data,data.length);
        dbmap.insert(hash,topseek);
        index_write();
      }
     }
     return 0;
  }
  public int add(string key,char[] data){
     uint hash=key.hash();
     int dlen=data.length;
     int seek=(4075+8)*dbind;
     array_clean(0,sbyte,MAXDLEN);
     array_copy(data,sbyte,data.length);
     array_print(sbyte,sbyte.length);
     append_file(hash,sbyte,dlen);
     dbmap.insert(hash,seek);
     dbind++;
     index_write();
     return seek;
  }
  public char[] get(string key)
  {
     uint hash=key.hash();
     bool has=dbmap.contains(hash);
     if(has){
      int seek=dbmap.get(hash);
      print ("seek=%d\n",seek);
      read_file(seek);
     }else{
      print ("nohas val\n");
     }
     return null;
  }
  public void update(string key,char[] data)
  {
     uint hash=key.hash();
     bool has=dbmap.contains(hash);
     if(has){
      int seek=dbmap.get(hash);
      print ("seek=%d\n",seek);
      update_file(seek,hash,data,data.length);
     }else{
      print ("nohas val\n");
     }
  }
  public void delete(string key)
  {
     uint hash=key.hash();
     bool has=dbmap.contains(hash);
     if(has){
      int seek=dbmap.get(hash);
      print ("seek=%d\n",seek);
      delete_file(seek);
      dellist.append(seek);
      dbmap.remove(hash);
      index_write();
     }else{
      print ("nohas val\n");
     }
  }
}
//======================== START OF FUNCTION ==========================//
// FUNCTION: string_to_char_array                                      //
//=====================================================================//
char[] string_to_char_array(string str) {
    char[] char_array = new char[str.length];
    var size=char_array.length;
    stdout.printf(@"toCharArray=$size\n");
    for (int i = 0; i < str.length; i++){
        char_array[i] = (char)str.get_char(str.index_of_nth_char(i));
    }
    
    return char_array;
}
//======================== START OF FUNCTION ==========================//
// FUNCTION: string_to_unichar_array                                   //
//=====================================================================//
unichar[] string_to_unichar_array(string str) {
    unichar[] char_array = new unichar[str.length];

    for (int i = 0; i < str.length; i++){
        char_array[i] = str.get_char(str.index_of_nth_char(i));
    }

    return char_array;
}

//array_equals
bool array_equals(char[] array_one, char[] array_two){
    if(array_one.length != array_two.length) return false;
    for (int i=0; i< array_one.length; i++) {
        if(array_one[i] != array_two[i]) {
            return false;
        }
    }
    return true;
}
//int position = array_search(1,array);
int array_search(char needle, char[] haystack){
    int result = -1;
    for (int i=0; i < haystack.length; i++) {
        if(needle == haystack[i]) return i;
    }
    return result;
}

void array_copy(char[] src,char[] dst,int length){
    for (int i=0; i < length; i++) {
       dst[i]=src[i]; 
    }
}
void array_clean(char src,char [] dst,int length){
    for (int i=0; i < length; i++) {
       dst[i]=src; 
    }
}
void array_print(char[] src,int length){
 for (int i=0; i<length; i++) {
    stdout.printf("%c", (char)src[i]);
 }
 stdout.printf("\n");
}
public static void main() {
  HMDB db = new HMDB();
  db.put("1","good".to_utf8());
  db.put("2","goodgood".to_utf8());
  db.put("3","goodgoodgood".to_utf8());
  db.delete("1");
  db.get("2");
  db.out_print();
/*
  db.put("good".to_utf8());
  db.put("goodgood".to_utf8());
  db.put("goodgoodgood".to_utf8());
  db.update(1,"asdfasdfasdf".to_utf8());
  db.delete(1);
  db.out_print();
  for (int i = 0; i < db.size(); i++){
    db.get(i);
  }
  string hi = "Hello, world!";
  uint hs=hi.hash();
  print ("hi.hash=%llu\n",hs);

string hi = "Hello, world!";
char[] hi_char_array = string_to_char_array(hi);
for (int i=0; i<hi_char_array.length; i++) {
    stdout.printf("%c\n", (char)hi_char_array[i]);
}
string str2=(string)hi_char_array;
stdout.printf("%s\n", str2);*/
}
