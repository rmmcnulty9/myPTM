import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;


public class DataManager extends Thread {

	public static int next_global_page_id;

	public ArrayList<Operation> current_ops = null;

    // List of operations that have been executed.
	public ArrayList<Operation> completed_ops = null;

	private int write_count=0;
	private int read_count=0;
	private int delete_count=0;
	
	private int checkpoint_op_cnt;
	private int buffer_size;
	private String search_method;
	private Journal journal;
	private int buffer_mgmt_table[][];
	private ArrayList<Page> buffer;
    // Flag indicating when the TM has completed its work.
    private boolean schedDoneFlag = false;

    private Scheduler scheduler;
    
    
	public ArrayList<DataFile> data_files;

	public DataManager(ArrayList<Operation> _current_op, ArrayList<Operation> _completed_ops, int _buffer_size, String _search_method, Scheduler s) {
		current_ops = _current_op;
        completed_ops = _completed_ops;
		buffer_size = _buffer_size;
		next_global_page_id=0;
		schedDoneFlag = false;
		scheduler  = s;
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
		/*
		 * TODO run as long as there are operations. This needs to be changed
		 */
		while(!schedDoneFlag || !current_ops.isEmpty()){
			
			if(!current_ops.isEmpty()){
				Operation op = current_ops.remove(0);
				if(op == null){
					System.out.println("Operations is null??");
					System.exit(0);
				}

				if(op.type.equals("A")){

				}else if(op.type.equals("C")){
					//TODO flush only dirty pages?
					while(!buffer.isEmpty()){
						flushPage(buffer.get(0));
					}
				}else{
					assert(op.filename!=null);
					//If the filename is not in pages file list add it
					if(null == getDataFile(op.filename)){
						data_files.add(new DataFile(op.filename));
					}
				}


				if(search_method.equals("scan")){

					if(op.type.equals("R")){
						read_count++;
						System.out.println("Scan Method: Reading "+read_count+"...");
						DataFile df = getDataFile(op.filename);

						//First check the buffer for desired page
						for(int j=0;j<buffer.size();j++){
							Page p = buffer.get(j);
							Record r = p.getRecordFromPage(Integer.parseInt(op.val));
							if(r!=null){
								System.out.println("[DM]"+r);
								//TODO pass back to Scheduler Record
							}
						}

						//If not check if buffer is full if so do replacement, else just add Page
						if(buffer.size()==buffer_size){
							//TODO replace
							Page old_p = buffer.remove(0);
							flushPage(old_p);
						}

						int i=0;
						while(df.getPageIDByIndex(i)!= -1){
							int page_id = df.getPageIDByIndex(i);
							Page p = loadPage(df, page_id);
							if(p==null){
								System.out.println("Page not found!");

							}else{
								Record r = p.getRecordFromPage(Integer.parseInt(op.val));
								if(r!=null){
									System.out.println("[DM]"+r);
									//TODO pass back to Scheduler Record
								}
							}
							i++;
						}
						//TODO pass back to Scheduler null Record
						System.out.println("[DM] Could not find ID="+op.val+" in "+op.filename);


					}else if(op.type.equals("W")){
						write_count++;
						System.out.println("Scan Method: Writing "+write_count+"...");
						DataFile df = getDataFile(op.filename);
						next_global_page_id = addRecordToFile(op.record, df, next_global_page_id);
						System.out.println(op);

					}else if(op.type.equals("D")){
						delete_count++;
						System.out.println("Scan Method: Delete "+delete_count+"...");
						DataFile df = getDataFile(op.filename);
						df.close();
						//TODO back up the DataFile in case of abort
						df.delete();
						data_files.remove(df);
					}


				}else if(search_method.equals("hash")){

					if(op.type.equals("R")){
						System.out.println("Hash Method: Reading...");
						System.exit(0);

					}else if(op.type.equals("W")){
						System.out.println("Hash Method: Writing...");
						System.exit(0);

					}else if(op.type.equals("D")){

						System.out.println("Hash Method: Delete...");
						DataFile df = getDataFile(op.filename);
						df.close();
						df.delete();
						data_files.remove(df);
					}
				}else{
					System.out.println("Bad Search Method.");
					System.exit(0);
				}

				//Add the completed op to the completeed_op list
				completed_ops.add(op);
			}
		}
		
		scheduler.setDMExitFlag();
		
	}

	private int addRecordToFile(Record new_r, DataFile df, int next_pid) {
		//If dataFile is empty add page
		if(df.isEmpty()){
			Page p = new Page(df.filename,next_pid);
			if(!p.add(new_r)){
				System.out.println("Can not add to this page!");
			}
			df.addPageID(next_pid);
			if(buffer.size() == buffer_size){
				//TODO replace
				Page old_p = buffer.remove(0);
				flushPage(old_p);
			}
			buffer.add(p);
			return (next_pid+1);
		}

		//Check buffer from page this belongs to
		for(int j=0;j<buffer.size();j++){
			Page p = buffer.get(j);
			for(int k=0;k<p.size();k++){
				Record r = p.get(k);
				//If record is present
				if(r.ID == new_r.ID){
					p.remove(k);
					if(!p.addAtIndex(k, new_r)){
						System.out.println("Can not add to this page!");
						return splitPage(df, p, new_r,next_pid, k);
					}
					return next_pid;
				}else if(j==(buffer.size()-1) && p.isFull()){
					//Split the Page & shift
					return splitPage(df,p,new_r,next_pid,k);
				}else if(r.ID>new_r.ID){
					if(!p.addAtIndex(k,new_r)){
						System.out.println("Can not add to this page!");
						return splitPage(df, p, new_r,next_pid, k);
					}
					return next_pid;
				}else if (k==(p.size()-1)){
					if(!p.addAtIndex(k+1,new_r)){
						System.out.println("Can not add to this page!");
						return splitPage(df, p, new_r,next_pid, k);
					}
					return next_pid;
				}
			}
		}

		// Check the file for the page
		int i=0;
		while(df.getPageIDByIndex(i)!= -1){
			int page_id = df.getPageIDByIndex(i);
			//Need room in memory for scan
			if(buffer.size() == buffer_size){
				//TODO replace
				Page old_p = buffer.remove(0);
				flushPage(old_p);
			}
			Page p = loadPage(df, page_id);
			if(p==null){
				System.out.println("Page not found!");
			}else{
				for(int k=0;k<p.size();k++){
					Record r = p.get(k);
					//If record is present
					if(r.ID == new_r.ID){
						p.remove(k);
						if(!p.addAtIndex(k, new_r)){
							System.out.println("Can not add to this page!");
							return splitPage(df, p, new_r,next_pid, k);
						}
						return next_pid;
					}else if(df.getPageIDByID(i+1)==-1 && p.isFull()){
						//Split the Page & shift
						return splitPage(df,p,new_r,next_pid,k);
					}else if(r.ID>new_r.ID){
						if(!p.addAtIndex(k,new_r)){
							System.out.println("Can not add to this page!");
							return splitPage(df, p, new_r,next_pid, k);
						}
						return next_pid;
					}else if (k==(p.size()-1)){
						if(!p.addAtIndex(k+1,new_r)){
							System.out.println("Can not add to this page!");
							return splitPage(df, p, new_r,next_pid, k);
						}
						return next_pid;
					}
				}
			}
			i++;
		}

	return next_pid;
}



    /* @summary
     * This method sets the shutdown flag.
     */
    public void setSchedDoneFlag(){
        schedDoneFlag = true;
    }



private int splitPage(DataFile df, Page p, Record r, int next_pid, int i) {
	Page new_p = new Page(df.filename, next_pid);
	//Move all records from the one larger than the new record on to the new page
	while(p.size()!=i){
		if(!new_p.add(p.remove(i))){
			System.out.println("Can not add to this page!");
		}
	}
	//Add the new record
	if(!p.add(r)){
		System.out.println("Can not add to this page!");
	}
	//Add page t buffer and DataFile
	if(buffer.size() == buffer_size){
		//TODO replace
		Page old_p = buffer.remove(0);
		flushPage(old_p);
	}
	df.addPageID(next_pid);
	buffer.add(new_p);


	return next_pid+1;
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
	Page page; // = new Page(df.filename, page_id);

	try {
		df.inputStream.skipBytes(Page.PAGE_SIZE_BYTES*page_id);
		page = (Page)df.inputStream.readObject();
		System.out.println(page);
		return page;
	}catch (EOFException e){
		return null;
	}
	catch (IOException e) {
		e.printStackTrace();
		System.exit(0);
	} catch (ClassNotFoundException e) {
		e.printStackTrace();
		System.exit(0);
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
		System.exit(0);
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

	@Override
	public boolean add(Record r){
		if(r==null){
			System.out.println("null!!!");
		}
		if ((this.size()+1)> RECORDS_PER_PAGE) return false;
		return super.add(r);
	}
	
	
	public boolean addAtIndex(int i, Record r){
		if(r==null){
			System.out.println("null!!!");
		}
		if ((this.size()+1)> RECORDS_PER_PAGE) return false;
		super.add(i,r);
		return true;
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
