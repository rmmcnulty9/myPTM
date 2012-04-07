import java.io.BufferedReader;
import java.io.File;
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
	public FileInputStream fis;
	private ArrayList <Integer> page_ids;
	
	public DataFile(String _fn){
		filename = _fn;
		File f = new File(filename);
		try {
			f.createNewFile();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			fos = new FileOutputStream(f);
			outputStream = new ObjectOutputStream(fos);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		try {
			fis = new FileInputStream(f);
			inputStream = new ObjectInputStream(fis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		page_ids = new ArrayList<Integer>();
		
	}
	
	public boolean addPageID(int pid){
		return page_ids.add(new Integer(pid));
	}
	
	public int getPageIDByIndex(int index){
		if(index>= page_ids.size()){
			return -1;
		}
		return page_ids.get(index).intValue();
	}
	
	public int getPageIDByID(int id){
		return page_ids.get(page_ids.indexOf(new Integer(id)));
	}
	
	public boolean isEmpty(){
		return page_ids.size()==0;
	}

	public boolean delete(){
		File f = new File("filename");
		return f.delete();
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
