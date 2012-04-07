import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


public class DataManager extends Thread {
	public static int next_global_page_id;
	
	public ArrayList<Operation> current_op = null;
	
	private boolean shutdown_flag=false;
	private int checkpoint_op_cnt;
	private int buffer_size;
	private String search_method;
	private Journal journal;
	private int buffer_mgmt_table[][];
	private ArrayList<Page> buffer;
	
	public ArrayList<DataFile> data_files;
	
	public DataManager(ArrayList<Operation> _current_op, int _buffer_size, String _search_method, Scheduler s) {
		current_op = _current_op;
		buffer_size = _buffer_size;
		next_global_page_id=0;
		/*
		 * Buffer Management table has(in order):
		 * 	block ID, dirty bit, fix count, page LSN, Stable-LSN and page number
		 */
		
		buffer_mgmt_table = new int[buffer_size][6];
		buffer = new ArrayList<Page>(buffer_size);
		
		
		search_method = _search_method;
		
		//TODO Initialize LockTree??
		data_files = new ArrayList<DataFile>();
		journal = new Journal();
		
		
	}

	public void run(){
		
		while(!shutdown_flag){
			if(!current_op.isEmpty()){
				Operation op = current_op.get(0);
				
				//TODO I think B(Begin) is processed by the Scheduler, I don't think it's needed in here
				
				if(op.type.equals("A")){
					
				}else if(op.type.equals("C")){
					
				}else{
					assert(op.filename!=null);
					//If the filename is not in pages file list add it
					//TODO might not be able to use contains here on BufferedReaders??
					if(!data_files.contains(op.filename)){
						data_files.add(new DataFile(op.filename));
					}
				}
				
				
				if(search_method.equals("scan")){
					
					if(op.type.equals("R")){
						DataFile df = getDataFile(op.filename);
						//First check the buffer for desired page
						for(int j=0;j<buffer.size();j++){
							Page p = buffer.get(j);
							Record r = p.getRecordFromPage(Integer.parseInt(op.val));
							if(r!=null){
								System.out.println(r);
								//TODO pass back to Scheduler Record
							}
						}
						
						//If not check if buffer is full if so do replacement, else just add Page
						if(buffer.size()==buffer_size){
							//TODO Replacement
						}else{
						
						int i=0;					
						while(df.getPageIDByIndex(i)!= -1){
							int page_id = df.getPageIDByIndex(i);
							Page p = loadPage(df, page_id);
							Record r = p.getRecordFromPage(Integer.parseInt(op.val));
							if(r!=null){
								System.out.println(r);
								//TODO pass back to Scheduler Record
							}
							i++;
						}
						//TODO pass back to Scheduler null Record
						}
						
					}else if(op.type.equals("W")){
						DataFile df = getDataFile(op.filename);
						next_global_page_id = addRecordToFile(df, next_global_page_id);
						
						
					}else if(op.type.equals("D")){
						//TODO we may want to physically delete the data, but for now just remove from data file list
						DataFile df = getDataFile(op.filename);
						df.close();
						data_files.remove(df);
					}
					
					
				}else if(search_method.equals("hash")){

					if(op.type.equals("R")){
						
						
					}else if(op.type.equals("W")){
						
					}else if(op.type.equals("D")){
						//TODO we may want to physically delete the data, but for now just remove from data file list
						DataFile df = getDataFile(op.filename);
						df.close();
						data_files.remove(df);
					}
				}else{
					System.out.println("Bad Search Method.");
					System.exit(0);
				}
				
			}
		}
	}
	
	private int addRecordToFile(DataFile df, int next_pid) {
		
		
		return next_pid;
	}

	public DataFile getDataFile(String fn){
		for(int i=0;i<data_files.size();i++){
			if(data_files.get(i).filename.equals(fn)){
				return data_files.get(i);
			}
		}
		return null;
	}
	
	public Page loadPage(DataFile df, int page_id){
		Page page = new Page(df.filename, page_id);
		char [] cbuf = new char[Page.PAGE_SIZE_BYTES];
		try {
			df.inputStream.skipBytes(Page.PAGE_SIZE_BYTES*page_id);
			page = (Page)df.inputStream.readObject();
			System.out.println(page);
			return page;
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		/*
		 * Terminate with NULL we have 2 free bytes per page, we could do something else
		 */
//		for(int i=0;i<cbuf.length-1;i+=Page.RECORD_SIZE_BYTES){
//			String record = new String(cbuf,i, Page.PAGE_SIZE_BYTES);
//			System.out.println("Records in this page:");
//			System.out.println(record);
//			
//			Byte id = new Byte(cbuf, i, 4);
//			String client_name = new String(cbuf, i+4, 4+18);
//			String phone = new String(cbuf, i+4+18, 12);
//			
//			if(cbuf[i+1]=='\0'){
//				return page;
//			}
//		}
		
		return null;
	}
	
	public boolean flushPage(Page page){
		DataFile df = getDataFile(page.file_of_origin);
		
		try {
			//Position
			df.fos.getChannel().position(page.page_id * Page.PAGE_SIZE_BYTES);
			df.outputStream.writeObject(page);
			df.outputStream.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	private class Page extends ArrayList<Record> implements java.io.Serializable{
		/**
		 * Generated serial ID so that Page objects can be written out to file
		 */
		private static final long serialVersionUID = -2938553534535400394L;
		public static final int RECORDS_PER_PAGE = 15;
		public static final int PAGE_SIZE_BYTES = 512;
		public static final int RECORD_SIZE_BYTES = 34;
		String file_of_origin;
		int page_id;
		public Page(String f, int p){
			file_of_origin = f;
			page_id = p;
		}
		
		public boolean isFull(){
			
			assert(this.size()<=RECORDS_PER_PAGE);
			
			return this.size()== RECORDS_PER_PAGE;
		}
		
		public Record getRecordFromPage(int id){
			for(int i=0;i<this.size();i++){
				if(this.get(i).ID == id){
					return this.get(i);
				}
			}
			return null;
		}
		
		
		public String toString(){
			String s="Page "+page_id+"\n";
			for(int i=0;i<RECORDS_PER_PAGE;i++){
				s+=	"Record "+i+": "+this.get(i).toString()+"\n";
			}
			return s;
		}
	}
	
}
