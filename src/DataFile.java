import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;


public class DataFile{
	public String filename;
	public FileOutputStream fos_overwrite;
	public ByteArrayOutputStream baos;
	public ObjectOutputStream outputStream;
	public FileInputStream fis;
	public BufferedInputStream bis;
	public ObjectInputStream inputStream;
	public File f;
	
	/*
	 * @summary The page ids indexed by block ids
	 */
	private ArrayList <Integer> page_ids;
	
	public DataFile(String _fn){
		filename = _fn;
		f = new File(filename);
		try {
			f.createNewFile();
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(0);
		}
		try {
			fos_overwrite = new FileOutputStream(f);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		try {
			fis = new FileInputStream(f);
			bis = new BufferedInputStream(fis);
//			inputStream = new ObjectInputStream(bis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		page_ids = new ArrayList<Integer>();
		
	}
	
	public int getPageIDByBlockID(int bid){
		if(bid>= page_ids.size()){
			return -1;
		}
		return page_ids.get(bid).intValue();
	}
	
	public boolean isEmpty(){
		return page_ids.size()==0;
	}
	public boolean delete(){
		return f.delete();
	}
	
	public boolean close(){
		try {
			if(null!=inputStream)inputStream.close();
			if(null!=outputStream)outputStream.close();
			if(null!=fis)fis.close();
			if(null!=fos_overwrite)fos_overwrite.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	public boolean addPageID(int pid){
		return page_ids.add(new Integer(pid));
	}
	public void addPageIDToBlockIDIndex(int i, int pid) {
		page_ids.add(i, new Integer(pid));
		
	}

//	public int getPageCount() {
//		return page_ids.size();
//	}

	public int getPIDIndexByPID(int page_id) {
		return page_ids.indexOf(page_id);
	}

	public int BlockCount() {
		return page_ids.size();
	}

	public void initializeDataFileSize(int i) {
		page_ids = new ArrayList<Integer>();
		for(int k=0	;k<i;k++)
			page_ids.add(new Integer(-1));
	}

	public void setPageIDInBlockIDIndex(int block_id, int page_id) {
		page_ids.set(block_id, new Integer(page_id));
	}
}
