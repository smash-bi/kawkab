package kawkab.test;

import kawkab.fs.persistence.Ibmap;

public class IbmapTest {
	public static void main(String args[]){
		IbmapTest tester = new IbmapTest();
		tester.testInodeNumber();
	}
	
	public void testInodeNumber(){
		int inodeNumber = 51;
		Ibmap ibmap = new Ibmap(0);
		
		for(int i=0; i<1000; i++){
			int inode = ibmap.consumeInode();
			assert i == inode;
		}
		
		
	}
}
