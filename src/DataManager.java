import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.FileChannel;
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
						int bid = op.record.ID % df.BlockCount();
						Page p = getBufferedPageByPageID(bid);
						if(p==null){
							// Not in buffer look in file
							p = loadPage(df, op.tid, bid);
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
						int page_id = op.record.ID % df.BlockCount();
						Page p = getBufferedPageByPageID(page_id);
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
							for(int i=0;i<p.size();i++){
								Record cur_r = p.get(i);
								if(cur_r.ID>op.record.ID){
									if(!p.addAtIndex(i, op.record)){
										rehashDataFile(df,op.record,op.tid,next_global_page_id);
									}
								}else if(i==Page.RECORDS_PER_PAGE-1){
									rehashDataFile(df,op.record,op.tid,next_global_page_id);
								}
							}
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

	private void rehashDataFile(DataFile df, Record record, int tid, int next_pid) {
		//TODO add page & rehash file - double pages in file??
		// Create temp file with double the number of block in df
		// Load all bloacks from df rehashing their records and flushing
		
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
		
		int bid=0;
		while(df.getPageIDByBlockID(bid)!= -1){
			
			Page p = loadPage(df, tid, bid);
			if(p != null){
				Record r = p.getRecordFromPage(id);
				if(r!=null){
					return r;
				}
			}
			bid++;
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

	private Page getBufferedPageByPageID(int pid) {
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
			if(buffer.size() == buffer_size){
				removePageInBuffer(null);
			}
			//Page creation
			Page p = new Page(df.filename,next_pid, 0, next_global_LSN);
			next_global_LSN+=1;
			df.addPageIDToBlockIDIndex(p.block_id,p.page_id);
			p.dirtied_by.add(tid);
			
			if(!p.add(new_r)){
				System.out.println("Must be able to add to this page.");
				System.exit(-1);
			}
			addToBuffer(p);
			
			return (next_pid+1);
		}

		int bid_min = 0;
		int bid_max = df.BlockCount()-1;
		
		while(bid_max >= bid_min){
			int bid_mid = (int) Math.ceil((bid_min+bid_max)/2.0);
			
			int location = findRecordLocationInPage(df, tid, bid_mid,new_r, 0);
			
			if(location==1){
				bid_min = bid_mid + 1;
			}else if(location==-1){
				bid_max = bid_mid - 1;
			}else if(location==0){
				int pid = df.getPageIDByBlockID(bid_mid);
				Page loc_page = getBufferedPageByPageID(pid);
				if(loc_page==null){
					loc_page = loadPage(df,tid,bid_mid);
				}
				for(int i=0;i<loc_page.size();i++){
					Record cur_r = loc_page.get(i);
					if(cur_r.ID>new_r.ID){
						if(!loc_page.addAtIndex(i, new_r)){
							return splitPage(df, loc_page, new_r, next_pid, tid, i);
						}
						return next_pid;
					}else if(i==Page.RECORDS_PER_PAGE-1){
						return splitPage(df, loc_page, new_r, next_pid, tid, i);
					}
				}
				//Add to end
				loc_page.add(new_r);
				return next_pid;
				
			}else{
				System.out.println("Bad location: "+location);
				System.exit(0);
			}
		}
		System.out.println("Should not get here!");
		System.exit(0);
		
	return next_pid;
}



    private void removePageInBuffer(Page in_use_now) {
		// TODO REPLACEMENT ALGORITHM - something smarter
    	int fewest_fix=0;
    	for(int i=0;i<buffer.size();i++){
    		Page cur = buffer.get(i);
    		//If being used now (ex: being split)
    		if(in_use_now!=null && cur.page_id==in_use_now.page_id)continue;
    		//If clean remove
    		if(cur.dirtied_by.isEmpty()){
    			buffer.remove(i);
    			return;
    		}
    		if(cur.fixed_by.size()<=buffer.get(fewest_fix).fixed_by.size()) fewest_fix=i;
    		
    	}
		Page old_p = buffer.remove(fewest_fix);
		flushPage(old_p);
	}

	/**
     * @param df
     * @param tid
     * @param bid_mid
     * @param new_r
     * @param recursed_dir - direction of base front +1 after -1 (opposite of direction of recursion)
     * @return -1 if correct page is before, 0 if this is correct page, 1 if correct page is after
     */
    private int findRecordLocationInPage( DataFile df, int tid, int bid_mid, Record new_r, int recursed_dir) {
    	Page p_mid = null;
    	//Is page in buffer
    	for(int i=0;i<buffer.size();i++){
    		if(buffer.get(i).block_id==bid_mid){
    			p_mid = buffer.get(i);
    			break;
    		}
    	}
    	// If not load page
    	if(p_mid==null){
    		p_mid = loadPage(df, tid, bid_mid);
    	}
    	//If page is in neither buffer datafile
    	if(0!=recursed_dir && p_mid==null){
    		return recursed_dir;
    	}else if(p_mid==null){
    		System.out.println("Error there must be a page here");
    		System.exit(0);
    	}
    	
    	
		// If new_r.ID< p_mid[0] - look backward
    	if(new_r.ID <p_mid.get(0).ID){
    		if(0==recursed_dir){
	    		int look_back = findRecordLocationInPage(df, tid, bid_mid-1, new_r, 1);
	    		if(look_back==1) return 0;	//belongs in this block/page
	    		else return -1;				//belongs in a block/page before this one
    		}
//    		if(p_mid.size()<Page.RECORDS_PER_PAGE && recursed_dir==1) return 0; //Belongs in this page because there is room
    		return -1;	//belongs in a block/page before this one
    	}
		//else if new_r.ID> p_mid[15] - look forward
//    	else if(p_mid.size()==Page.RECORDS_PER_PAGE && new_r.ID > p_mid.get(Page.RECORDS_PER_PAGE-1).ID){
        	else if(new_r.ID > p_mid.get(p_mid.size()-1).ID){
    		if(0==recursed_dir){ 
	    		int look_forward = findRecordLocationInPage(df, tid, bid_mid+1, new_r, -1);
	    		if(look_forward == -1) return 0;		//belongs in this block/page
	    		else return 1;						//belongs in a block/page after this one
    		}
    		if(p_mid.size()<Page.RECORDS_PER_PAGE && recursed_dir==1) return 0; //Belongs in this page because there is room
    		return 1;		//belongs in a block/page after this one
    	}
    	else{
    		return 0; 	//else the ID must fall in this range and belong in this block
    	}
	}

	/* @summary
     * This method sets the shutdown flag.
     */
    public void setSchedDoneFlag(){
        schedDoneFlag = true;
    }



/**
 * @param df - DataFile the pages will/do belong to
 * @param old_p - the page to split
 * @param r - The record to add after the split
 * @param next_pid - the pid for the new page
 * @param tid - the transactions involving this operations
 * @param i - the location to split
 * @return the new next_pid value
 */
private int splitPage(DataFile df, Page old_p, Record r, int next_pid, int tid, int i) {
	System.out.println("SPLITTING:\n"+old_p+"\n due to :"+r);
	
	//Add page t buffer and DataFile
	if(buffer.size() == buffer_size){
		removePageInBuffer(old_p);
	}
	//Page creation
	Page new_p = new Page(df.filename, next_pid, old_p.block_id+1, next_global_LSN);
	next_global_LSN+=1;
//	df.addPageIDToBlockIDIndex(new_p.block_id,new_p.page_id);
	new_p.dirtied_by.add(tid);
	
	
	
	if(i!=0){
		df.addPageIDToBlockIDIndex(new_p.block_id,new_p.page_id);
		if(!new_p.add(r)){		//Add the new record at beginning of new page
			System.out.println("[DM] Can not add to this page!");
		}
		flushPage(new_p);
	}
	//If NOT the last record in page then
	if(i!=Page.RECORDS_PER_PAGE-1){
		//Move record from i...last
				while(old_p.size()!=i){
					if(!new_p.add(old_p.remove(i))){
						System.out.println("Must have room in this page.");
						System.exit(0);
					}
				}
	}
	
		if(i==0){
			//Switch the block ids
//			int tmp = old_p.block_id;
//			old_p.block_id = new_p.block_id;
//			new_p.block_id = tmp;
			df.addPageIDToBlockIDIndex(new_p.block_id,new_p.page_id);
			if(!old_p.add(r)){		//Add the new record at beginning of OLD page
				System.out.println("[DM] Can not add to this page!");
			}
			flushPage(new_p);
		}
	
	
	addToBuffer(new_p);
	
	
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

public Page loadPage(DataFile df, int tid, int block_id){
	Page page;
	
	try {		
		if(block_id<0 || block_id>=df.BlockCount()) return null;
		
		long pos = Page.PAGE_SPACING_BYTES * block_id;
		FileChannel fc = df.fis.getChannel().position(pos);
		System.out.println(fc.position()+" "+fc.size());
		df.inputStream = new ObjectInputStream(new BufferedInputStream(df.fis));
		page = (Page)df.inputStream.readObject();
		page.fixed_by.add(tid);
		//If not check if buffer is full if so do replacement, else just add Page
		if(buffer.size()==buffer_size){
			removePageInBuffer(null);
		}
		addToBuffer(page);
//		System.out.println("PAGE LOADED: "+page);
		
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
public boolean flushPage(Page page_in_buffer){
	System.out.println("FLUSHING:\n"+page_in_buffer);
	
	DataFile df = getDataFileByName(page_in_buffer.file_of_origin);
//	page_in_buffer.block_id = df.getPIDIndexByPID(page_in_buffer.page_id);
	try {
		Page page_in_file = null;
		//Position
		long pos = page_in_buffer.block_id * Page.PAGE_SPACING_BYTES;

		ByteArrayOutputStream bos = new ByteArrayOutputStream(Page.PAGE_SPACING_BYTES);
		df.outputStream = new ObjectOutputStream(bos);
		// If this page does not already exist get it's version from the file
		if(page_in_buffer.page_id!=page_in_buffer.block_id){
			FileChannel fc = df.fis.getChannel().position(pos);
			System.out.println(fc.position()+" "+fc.size());
			if(fc.position()<fc.size()){
				df.inputStream = new ObjectInputStream(new BufferedInputStream(df.fis));
				page_in_file = (Page)df.inputStream.readObject();
			}
//			df.inputStream.close();
		}
		
		//Current LSN will become stable & LSN will be zero while not in memory buffer
		page_in_buffer.stableLSN = page_in_buffer.LSN;
		page_in_buffer.LSN = 0;
		
		df.outputStream.writeObject(page_in_buffer);
		df.outputStream.flush();
		FileChannel fc_o=null, fc_a=null;
		//If the page_id in file is different from the one in buffer APPEND
		if(null != page_in_file && page_in_file.page_id != page_in_buffer.page_id){
			//Get the page to be over written
			FileChannel fc = df.fis.getChannel().position(pos);
			System.out.println(fc.position()+" "+fc.size());
			if(fc.position()<fc.size()){
				df.inputStream = new ObjectInputStream(new BufferedInputStream(df.fis));
				page_in_file = (Page)df.inputStream.readObject();
			}
			
			fc_a = df.fos_overwrite.getChannel().position(pos);
			System.out.println(fc_a.position()+" "+fc_a.size());
			bos.writeTo(df.fos_overwrite);
			System.out.println(fc_a.position()+" "+fc_a.size());
			page_in_file.block_id++;
//			if(fc.position()<fc.size()){
				flushPage(page_in_file);
//			}
//			System.exit(0);
		}else{
//			df.fos_append.getChannel().position(pos);
//			bos.writeTo(df.fos_append);
			fc_a = df.fos_overwrite.getChannel().position(pos);
			System.out.println(fc_a.position()+" "+fc_a.size());
			bos.writeTo(df.fos_overwrite);
			System.out.println(fc_a.position()+" "+fc_a.size());
//			System.exit(0);
		}
		df.outputStream.close();
//		df.fos_append.close();
		return true;
	} catch (IOException e) {
		e.printStackTrace();
		System.exit(0);
	} 
	catch (ClassNotFoundException e) {
		e.printStackTrace();
		System.exit(0);
	}

	return false;
}
}
