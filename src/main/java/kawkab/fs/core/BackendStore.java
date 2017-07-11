package kawkab.fs.core;

import org.iq80.leveldb.*;
import static org.fusesource.leveldbjni.JniDBFactory.*;
import java.io.*;

public class BackendStore{

	private final String dbPath;

	public BackendStore(String dbPath){
		this.dbPath = dbPath;
	}
	
	public void put(String blockID, byte[] block) throws IOException{
		Options options = new Options();
		options.createIfMissing(true);

		DB db = factory.open(new File(dbPath), options);
		
		try{
			db.put(bytes(blockID),block);
		}finally{
			db.close();
		}
	}

	public byte[] get(String blockID) throws IOException{
		Options options = new Options();
		options.createIfMissing(true);

		DB db = factory.open(new File(dbPath), options);
		
		try{
			return db.get(bytes(blockID));
		}finally{
			db.close();
		}
	}
}



