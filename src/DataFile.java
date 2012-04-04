import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


public class DataFile {
	/*
	 * 512 page / 4 ID + 18 ClientName + 12 Phone = 512/34 = 15 records per block
	 */
	private int BLOCKING_FACTOR = 15;
	private FileInputStream fis;
	private DataInputStream dis;
	private BufferedReader br;
	private ArrayList<Page> pages;
	
	public DataFile(String file_name, String search_method, int num_pages){

		pages = new ArrayList<Page>();
		//TODO Build the DataFile - sort
		try {
			fis = new FileInputStream(file_name);
			dis = new DataInputStream(fis);
			br = new BufferedReader(new InputStreamReader(dis));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

//	public boolean addRecord(Record r){
//		for(int i=0;i<pages.size();i++){
//			if(!pages.get(i).isFull()){
//				addRecord
//			}
//			
//		}
//		pages.add(new Page(r));
//		
//	}
	
	public boolean closeDataFile(){
		try {
			br.close();
			dis.close();
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	
	private class Page extends ArrayList<Record>{
		
//		public Page(){
//			
//		}
//		
//		public Page(Record r){
//			addRecord(r);
//		}
		
		public boolean isFull(){
			return (this.size()==BLOCKING_FACTOR);
		}
		
//		public boolean addRecord(Record r){
//			if(this.size()==BLOCKING_FACTOR){
//				return false;
//			}
//			this.add(r);
//			return true;
//		}
		
	}
}