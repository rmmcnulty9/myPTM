import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;


public class DataManager extends Thread {

	public static int next_global_page_id;
	public static int next_global_LSN;

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
	private ArrayList<Page> buffer;
    // Flag indicating when the TM has completed its work.
    private boolean schedDoneFlag = false;

    private Scheduler scheduler;
    
    
	public ArrayList<DataFile> data_files;

	public DataManager(ArrayList<Operation> _current_op, ArrayList<Operation> _completed_ops, int _buffer_size, String _search_method, Scheduler s) {
		current_ops = _current_op;
        completed_ops = _completed_ops;
		buffer_size = _buffer_size;
		next_global_page_id = 0;
		next_global_LSN = 0;
		schedDoneFlag = false;
		scheduler  = s;
		/* TODO move BMT to the Page
		 * Buffer Management table has(in order):
		 * 	block ID, dirty bit, fix count, page LSN, Stable-LSN and page number
		 */

		buffer = new ArrayList<Page>(buffer_size);

		search_method = _search_method;

		data_files = new ArrayList<DataFile>();
		journal = new Journal();


	}

	public void run(){
		
		while(!schedDoneFlag || !current_ops.isEmpty()){
			
			if(!current_ops.isEmpty()){
				Operation op = current_ops.remove(0);
				if(op == null){
					System.out.println("[DM] Operations is null??");
					System.exit(0);
				}

				if(op.type.equals("A")){
					//TODO Write undo function
				}else if(op.type.equals("C")){
					System.out.println("[DM] Committing...");
					flushTransactionsPages(op.tid);
					completed_ops.add(op);
					continue;
				}else{
					assert(op.filename!=null);
					//If the filename is not in pages file list add it
					if(null == getDataFileByName(op.filename)){
						data_files.add(new DataFile(op.filename));
					}
				}

				DataFile df = getDataFileByName(op.filename);

				/**************
				 * SCAN METHOD
				 **************/
				
				if(search_method.equals("scan")){

					if(op.type.equals("R")){
						read_count++;
						System.out.println("[DM] Scan Method: Reading "+read_count+"...");

						//First check the buffer for desired page
						Record r = getRecordFromBufferedPage(Integer.parseInt(op.val));
						 if(r != null){
								System.out.println("[DM]"+r);
								completed_ops.add(op);
								continue;
						 }
						 //Then check file
						r = getRecordFromDataFile(df, op.tid, Integer.parseInt(op.val));
						if(r != null){
							System.out.println("[DM]"+r);
							completed_ops.add(op);
							continue;
						}else{
							System.out.println("[DM] Records not found!");
							completed_ops.add(op);
							continue;
						}


					}else if(op.type.equals("W")){
						write_count++;
						System.out.println("[DM] Scan Method: Writing "+write_count+"...");
						
						next_global_page_id = addRecordToFileScan(op.record, df, op.tid, next_global_page_id);
						completed_ops.add(op);
						continue;
						

					}else if(op.type.equals("D")){
						delete_count++;
						System.out.println("[DM] Scan Method: Delete "+delete_count+"...");
						df.close();
						//TODO back up the DataFile in case of abort
						df.delete();
						next_global_page_id=0;
						//TODO For now assume only one datafile so remove everything from buffer
						buffer.clear();
						data_files.remove(df);
						completed_ops.add(op);
						continue;
					}


					/**************
					 * HASH METHOD
					 **************/
				}else if(search_method.equals("hash")){

					/*
					 * TODO Find page id via hashing
					 * page_id = op.record.ID % df.getPageCount()
					 * is this page id in buffer? no - make room in buffer to loadPage(page_id) - 
					 *  if W: check for room in this page - if not do rehashing?? 
					 *  	 binary search to insert/replace
					 * if R: binary search to read
					 */
									
					
					if(op.type.equals("R")){
						System.out.println("[DM] Hash Method: Reading "+read_count+"...");
						int page_id = op.record.ID % df.getPageCount();
						Page p = getBufferedPageByID(page_id);
						if(p==null){
							// Not in buffer look in file
							p = loadPage(df, op.tid, page_id);
						}
						if(p == null){
							System.out.println("[DM] Records not found!");
							completed_ops.add(op);
							continue;
						}else{
							Record r = p.getRecordFromPage(Integer.parseInt(op.val));
							if(r==null){
								System.out.println("[DM] Records not found!");
								completed_ops.add(op);
								continue;
							}else{
								System.out.println("[DM]"+r);
								completed_ops.add(op);
								continue;
							}
						}
					}else if(op.type.equals("W")){
						System.out.println("[DM] Hash Method: Writing "+write_count+"...");
						int page_id = op.record.ID % df.getPageCount();
						Page p = getBufferedPageByID(page_id);
						if(p==null){
							// Not in buffer look in file
							p = loadPage(df, op.tid, page_id);
						}
						if(p == null){
							System.out.println("[DM] Records not found!");
							//TODO add page & rehash file - double pages in file??
							completed_ops.add(op);
							continue;
						}else{
							//TODO Add records to page now in buffer
							completed_ops.add(op);
							continue;
						}

					}else if(op.type.equals("D")){

						System.out.println("[DM] Hash Method: Delete "+delete_count+"...");
						df.close();
						//TODO back up the DataFile in case of abort
						df.delete();
						next_global_page_id=0;
						//TODO For now assume only one datafile so remove everything from buffer
						buffer.clear();
						data_files.remove(df);
						completed_ops.add(op);
						continue;
					}
				}else{
					System.out.println("[DM] Bad Search Method.");
					System.exit(0);
				}

			}
		}
		//Close the datafile
		System.out.println("[DM] Closing all data files...");
		while(!data_files.isEmpty()){
			DataFile df = data_files.remove(0);
			df.close();
		}
		scheduler.setDMExitFlag();
		
        System.out.println("[DM] Data Manager is exiting...");
		
	}

	private void flushTransactionsPages(int tid) {
		int orig_size = buffer.size();
		int touched = 0;
		while(touched!= orig_size){
			Page p = buffer.get(touched);
			boolean isDirty = p.dirtied_by.remove(new Integer(tid));
			p.fixed_by.remove(new Integer(tid));
			if(isDirty){
				flushPage(p);
				if(p.fixed_by.size()==0 ){
					//TODO Made sure this will actually remove the Page - scary remove(object)
					buffer.remove(p);
					touched--;
					orig_size--;
				}
			}
			touched++;
		}
		
	}

	private Record getRecordFromDataFile(DataFile df, int tid, int id) {
		
		int i=0;
		while(df.getPageIDByIndex(i)!= -1){
			
			int page_id = df.getPageIDByIndex(i);
			Page p = loadPage(df, tid, page_id);
			if(p != null){
				Record r = p.getRecordFromPage(id);
				if(r!=null){
					return r;
				}
			}
			i++;
		}
		return null;
	}

	private Record getRecordFromBufferedPage(int id) {
		//TODO possibly a binary search?
		for(int i=0;i<buffer.size();i++){
			Page p = buffer.get(i);
			Record r = p.getRecordFromPage(id);
			if(r != null) return r;
		}
		return null;
	}

	private Page getBufferedPageByID(int pid) {
		for(int i=0;i<buffer.size();i++){
			if(buffer.get(i).page_id==pid){
				return buffer.get(i);
			}
		}
		return null;
	}

	private int addRecordToFileScan(Record new_r, DataFile df, int tid, int next_pid) {
		//If dataFile is empty add page
		if(df.isEmpty()){
			Page p = new Page(df.filename,next_pid, next_global_LSN);
			next_global_LSN+=1;
			if(!p.add(new_r)){
				System.out.println("[DM] Can not add to this page!");
			}
			df.addPageID(next_pid);
			if(buffer.size() == buffer_size){
				//TODO replace
				Page old_p = buffer.remove(0);
				flushPage(old_p);
			}
			addToBuffer(p);
//			buffer.add(p);
			return (next_pid+1);
		}

		//TODO implement scan as binary search
		/*
		 * 
		 */
		//Check buffer from page this belongs to
		for(int j=0;j<buffer.size();j++){
			Page p = buffer.get(j);
			for(int k=0;k<p.size();k++){
				Record r = p.get(k);
				if(r.ID == new_r.ID){	//Replace if its present
					p.remove(k);
					if(!p.addAtIndex(k, new_r)){
						System.out.println("[DM] Can not add to this page!");
						p.dirtied_by.add(tid);
						return splitPage(df, p, new_r,next_pid, tid, k);
					}
					p.dirtied_by.add(tid);
					return next_pid;
				}else if(r.ID>new_r.ID){ // Add in front of first record with greater ID
					if(!p.addAtIndex(k,new_r)){
						System.out.println("[DM] Can not add to this page!");
						p.dirtied_by.add(tid);
						return splitPage(df, p, new_r,next_pid, tid, k);
					}
					p.dirtied_by.add(tid);
					return next_pid;
				}
				else if (k==(p.size()-1) && (j==(buffer.size()-1))){ //If end of last page in buffer
					if(!p.addAtIndex(k+1,new_r)){
						System.out.println("[DM] Can not add to this page!");
						p.dirtied_by.add(tid);
						return splitPage(df, p, new_r,next_pid, tid, k);
					}
					p.dirtied_by.add(tid);
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
			Page p = loadPage(df, tid, page_id);
			if(p==null){
				System.out.println("[DM] Page not found!");
			}else{
				for(int k=0;k<p.size();k++){
					Record r = p.get(k);
					//If record is present
					if(r.ID == new_r.ID){
						p.remove(k);
						if(!p.addAtIndex(k, new_r)){
							System.out.println("[DM] Can not add to this page!");
							p.dirtied_by.add(tid);
							return splitPage(df, p, new_r,next_pid, tid, k);
						}
						p.dirtied_by.add(tid);
						return next_pid;
					}else if(df.getPageIDByIndex(i+1)==-1 && p.isFull()){
						//Split the Page & shift
						p.dirtied_by.add(tid);
						return splitPage(df, p, new_r, next_pid, tid, k);
					}else if(r.ID>new_r.ID){
						if(!p.addAtIndex(k,new_r)){
							System.out.println("[DM] Can not add to this page!");
							p.dirtied_by.add(tid);
							return splitPage(df, p, new_r,next_pid, tid, k);
						}
						p.dirtied_by.add(tid);
						return next_pid;
					}
//					else if (k==(p.size()-1)){
//						if(!p.addAtIndex(k+1,new_r)){
//							System.out.println("Can not add to this page!");
//							return splitPage(df, p, new_r,next_pid, k);
//						}
//						return next_pid;
//					}
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



private int splitPage(DataFile df, Page p, Record r, int next_pid, int tid, int i) {
	Page new_p = new Page(df.filename, next_pid, next_global_LSN);
	next_global_LSN+=1;
	//Move all records from the one larger than the new record on to the new page
	while(p.size()!=i){
		if(!new_p.add(p.remove(i))){
			System.out.println("[DM] Can not add to this page!");
		}
	}
	//Add the new record
	if(!p.add(r)){
		System.out.println("[DM] Can not add to this page!");
	}
	new_p.dirtied_by.add(tid);
	//Add page t buffer and DataFile
	if(buffer.size() == buffer_size){
		//TODO replace
		Page old_p = buffer.remove(0);
		flushPage(old_p);
	}
	df.addPageID(p.page_id+1,next_pid);
	addToBuffer(new_p);
//	buffer.add(new_p);


	return next_pid+1;
}

private void addToBuffer(Page new_page) {
	if(buffer.size()==0){
		buffer.add(new_page);
		return;
	}
	//Maintain ordering of pages in buffer
	for(int i=0;i<buffer.size();i++){
		if(buffer.get(i).get(0).ID>new_page.get(0).ID){
			buffer.add(i,new_page);
			return;
		}
	}

	buffer.add(new_page);
	return;
	
}

public DataFile getDataFileByName(String fn){
	for(int i=0;i<data_files.size();i++){
		if(data_files.get(i).filename.equals(fn)){
			return data_files.get(i);
		}
	}
	return null;
}

public Page loadPage(DataFile df, int tid, int page_id){
	Page page; // = new Page(df.filename, page_id);
	
	//If not check if buffer is full if so do replacement, else just add Page
	if(buffer.size()==buffer_size){
		//TODO replace
		Page old_p = buffer.remove(0);
		flushPage(old_p);
	}
	
	try {
		long pos = Page.PAGE_SPACING_BYTES*df.getPIDIndexByPID(page_id);
		df.fis.getChannel().position(pos);
		df.inputStream = new ObjectInputStream(new BufferedInputStream(df.fis));
		page = (Page)df.inputStream.readObject();
		System.out.println("PAGE LOADED: "+page);
		page.fixed_by.add(tid);
		addToBuffer(page);
//		buffer.add(page);
		
		return page;
	}catch (EOFException e){
//		e.printStackTrace();
		System.out.println("[DM] Page not found!");
		return null;
	}
	catch (IOException e) {
		e.printStackTrace();
		System.exit(0);
	} catch (ClassNotFoundException e) {
		e.printStackTrace();
		System.exit(0);
	}
	return null;
}
/*
 * @summary 
 * The page MUST be removed from the buffer in calling function
 * 
 * @param Page page - page to be written to file
 */
public boolean flushPage(Page page){
	DataFile df = getDataFileByName(page.file_of_origin);
	//Current LSN will become stable & LSn will be zero while not in memory buffer
	page.stableLSN = page.LSN;
	page.LSN = 0;
	
	try {
		//Position
		long pos = df.getPIDIndexByPID(page.page_id) * Page.PAGE_SPACING_BYTES;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		df.outputStream = new ObjectOutputStream(bos);

		df.outputStream.writeObject(page);
		df.outputStream.flush();

		df.fos.getChannel().position(pos);
		bos.writeTo(df.fos);
		df.outputStream.close();
		return true;
	} catch (IOException e) {
		e.printStackTrace();
		System.exit(0);
	}

	return false;
}
}
