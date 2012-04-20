import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.util.ArrayList;


public class DataFile{
	public String filename;
	public int df_id;
	public boolean isDeleted;
	public RandomAccessFile raf;
	public File f;
	
	/*
	 * @summary The page ids indexed by block ids
	 */
	private ArrayList <Integer> page_ids;
	
	
	public DataFile(String _fn, int dfid){
		filename = _fn;
		df_id = dfid;
		isDeleted = false;
		//Create the File
		f = new File(filename);
		try {
			f.createNewFile();
			raf = new RandomAccessFile(f,"rw");
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		page_ids = new ArrayList<Integer>();
		
	}
	
	public int getPageIDByBlockID(int bid){
		if(bid>= page_ids.size() || bid<0){
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
//			if(null!=inputStream)inputStream.close();
//			if(null!=outputStream)outputStream.close();
//			if(null!=fis)fis.close();
//			if(null!=fos)fos.close();
//			if(null!=bw)bw.close();
//			if(null!=br)br.close();
			if(null!=raf)raf.close();

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
