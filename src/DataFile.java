import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;


public class DataFile {
	public String filename;
	public ObjectInputStream inputStream;
	public ObjectOutputStream outputStream;
	public FileOutputStream fos;
	private ArrayList <Integer> page_ids;
	
	public DataFile(String _fn){
		filename = _fn;
		
		try {
			inputStream = new ObjectInputStream(new FileInputStream(filename));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			fos = new FileOutputStream(filename);
			outputStream = new ObjectOutputStream(fos);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		page_ids = new ArrayList<Integer>();
		
	}
	
	public boolean addPageID(int pid){
		return page_ids.add(new Integer(pid));
	}
	
	public int getPageIDByIndex(int index){
		if(index> page_ids.size()){
			return -1;
		}
		return page_ids.get(index).intValue();
	}
	
	public int getPageIDByID(int id){
		return page_ids.get(page_ids.indexOf(new Integer(id)));
	}
	
	public boolean close(){
		try {
			inputStream.close();
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
}
