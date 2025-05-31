import org.rocksdb.*;
;
public class SimpleRocksDBpool {
    static {
        RocksDB.loadLibrary();
    }
    public static void main(String[] args){
        Options options=new Options().setCreateIfMissing(true);
        try (RocksDB db=RocksDB.open(options,"/usr")){
            db.put("key1".getBytes(),"value1".getBytes());
            byte[] value=db.get("key1".getBytes());
            System.out.println("value:"+new String(value));
        }catch(RocksDBException e){
            e.printStackTrace();
        }
    }
}
