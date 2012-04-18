import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
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
				DataFile df = getDataFileByName(op.filename);
				if(op.type.equals("A")){
					//TODO Write undo function
				}else if(op.type.equals("C")){
					System.out.println("[DM] Committing...");
					flushTransactionsPages(op.tid);
					completed_ops.add(op);
					continue;
				}else{
					//If the filename is not in pages file list add it
					if(null == df){
						df = new DataFile(op.filename);
						data_files.add(df);
					}
				}

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
						
						next_global_page_id = addRecordToFileScan(df, op.record, op.tid, next_global_page_id);
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
						read_count++;
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
						write_count++;
						System.out.println("[DM] Hash Method: Writing "+write_count+"...");
						//If dataFile is empty add page
						if(df.isEmpty()){
							if(buffer.size() == buffer_size){
								System.out.println("Buffer should be empty here.");
								System.exit(0);
//								removePageInBuffer(df, null);
							}
							//Page creation
							Page p = new Page(df.filename,next_global_page_id, 0, next_global_LSN);
							next_global_page_id++;
							next_global_LSN+=1;
							df.addPageIDToBlockIDIndex(p.block_id,p.page_id);
							p.dirtied_by.add(op.tid);
							
							if(!p.add(op.record)){
								System.out.println("Must be able to add to this page.");
								System.exit(-1);
							}
							addToBuffer(p);
							completed_ops.add(op);
							continue;
						}
						
						int page_id = op.record.ID % df.BlockCount();
						Page p = getBufferedPageByPageID(page_id);
						if(p==null){
							// Not in buffer look in file
							p = loadPage(df, op.tid, page_id);
						}
						if(p == null){
							System.out.println("[DM] Records not found!");
							next_global_page_id = rehashDataFile(df,op.record,op.tid,next_global_page_id);
							completed_ops.add(op);
							continue;
						}else{
							//TODO Add records to page now in buffer
							for(int i=0;i<p.size();i++){
								Record cur_r = p.get(i);
								if(cur_r.ID>op.record.ID){
									if(!p.addAtIndex(i, op.record)){
										next_global_page_id = rehashDataFile(df,op.record,op.tid,next_global_page_id);
									}
									break;
								}else if(i==Page.RECORDS_PER_PAGE-1){
									next_global_page_id = rehashDataFile(df,op.record,op.tid,next_global_page_id);
									break;
								}else if(i==p.size()-1){
									if(!p.add(op.record)){
										System.out.println("This should not trip!");
										System.exit(0);
									}
									break;
								}
							}
							completed_ops.add(op);
							continue;
						}

					}else if(op.type.equals("D")){
						delete_count++;
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

	private int rehashDataFile(DataFile df, Record record, int tid, int next_pid) {
		//TODO add page & rehash file - double pages in file??
		
		//First flush the whole buffer
		while(!buffer.isEmpty()){
			System.out.println("Emtpying buffer!");
			flushPage(df, buffer.remove(0));
		}
		
		// Create temp file with double the number of block in df
		File tmp;
//		try {
			DataFile new_df = new DataFile(df.filename+"_TEMP");
			new_df.initializeDataFileSize(df.BlockCount() * 2);
			data_files.add(new_df);
			ByteArrayOutputStream bos = new ByteArrayOutputStream(Page.PAGE_SIZE_BYTES);
			
//			new_df.outputStream = new ObjectOutputStream(bos);
			for(int i=0;i<df.BlockCount();i++){
				if(df.getPageIDByBlockID(i)==-1)continue;
				Page p = loadPage(df,tid,i);
				if(p==null){
					System.out.println("Page must exist!");
					System.exit(0);
				}
				//Rehash each record into new DataFile
				for(int k=0;k<p.size();k++){
					int new_bid =  p.get(k).ID % new_df.BlockCount();
					Page new_page = getBufferedPageByPageID(df.getPageIDByBlockID(new_bid));
					if(new_page==null){
						new_page = loadPage(new_df,tid,new_bid);
					}
					if(new_page==null){
						new_page = new Page(new_df.filename, next_pid, new_bid, next_global_LSN);
						next_pid++;
						next_global_LSN+=1;
						new_df.setPageIDInBlockIDIndex(new_page.block_id,new_page.page_id);
						new_page.dirtied_by.add(tid);
					}
					new_page.add(p.get(k));
//					flushPage(new_df, new_page);
				}
				//Remove the old page so it does not get flushed to the new datafile
				buffer.remove(p);
			}
			
			//Remove the old DataFile and rename the current one
			df.close();
			if(!df.delete()){
				System.out.println("failed to delete old DF");
				System.exit(0);
			}
			//Everything in buffer than be thrown out
			buffer.clear();
			
			data_files.remove(new_df);
			boolean ret = new_df.f.renameTo(df.f);
			data_files.remove(df);
			new_df.filename = df.filename;
			data_files.add(new_df);
			
			// Block that the new record belongs in
			int new_record_bid = record.ID % new_df.BlockCount();
			
			//Change the file of origin for each page
			for(int i=0;i<new_df.BlockCount();i++){
				if(new_df.getPageIDByBlockID(i)==-1)continue;
				Page p = loadPage(new_df,tid,i);
				if(p==null){
					System.exit(0);
				}
				//Add the new record
				if(p.block_id == new_record_bid){
					for(int k=0;k<p.size();k++){
						if(p.get(k).ID>record.ID){
							p.add(k, record);
							break;
						}
						if(k==p.size()-1){
							p.add(record);
							break;
						}
					}
				}
				p.file_of_origin = df.filename;
				flushPage(new_df, p);
			}
			//TODO Reinitialize the File Streams
			System.out.println("Not done yet!");
			System.exit(0);
//			new_df.fis.close();
//			new_df.bis.close();
//			new_df.f = new File(new_df.filename);
//			boolean r = new_df.f.exists();
//			new_df.fis = new FileInputStream(new_df.f);
////			df.bis = new BufferedInputStream(df.fis);
//			new_df.fos.close();
//			new_df.fos = new FileOutputStream(new_df.f);
			
//		} 
//		catch (IOException e) {
//			e.printStackTrace();
//			System.exit(0);
//		}
		return next_pid;

//		return next_global_page_id+df.BlockCount();
	}

	private void flushTransactionsPages(int tid) {
		int orig_size = buffer.size();
		int touched = 0;
		while(touched!= orig_size){
			Page p = buffer.get(touched);
			boolean isDirty = p.dirtied_by.remove(new Integer(tid));
			p.fixed_by.remove(new Integer(tid));
			if(isDirty){
				DataFile df = getDataFileByName(p.file_of_origin);
				flushPage(df, p);
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

	private int addRecordToFileScan( DataFile df, Record new_r, int tid, int next_pid) {
		//If dataFile is empty add page
		if(df.isEmpty()){
			if(buffer.size() == buffer_size){
				removePageInBuffer(df, null);
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



    private void removePageInBuffer(DataFile df, Page in_use_now) {
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
		flushPage(df, old_p);
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
    		if(buffer.get(i).page_id==df.getPageIDByBlockID(bid_mid)){
    			buffer.get(i).block_id = bid_mid;
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
		removePageInBuffer(df, old_p);
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
		flushPage(df, new_p);
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
			df.addPageIDToBlockIDIndex(new_p.block_id,new_p.page_id);
			if(!old_p.add(r)){		//Add the new record at beginning of OLD page
				System.out.println("[DM] Can not add to this page!");
			}
			flushPage(df, new_p);
		}
	
	
	addToBuffer(new_p);
//	flushPage(df,old_p);
//	flushPage(df,new_p);
	
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

//TODO Rewrite to read in text and fill in a Page object
public Page loadPage(DataFile df, int tid, int bid){
	if(bid<0 || bid>=df.BlockCount()||-1==df.getPageIDByBlockID(bid)) return null;
	
	try{	
		String line_in_file = "dumby";
		int line_ctr=0;
		while(null!=line_in_file){
			
			line_in_file = df.raf.readLine();
			
			if(line_in_file!=null && line_ctr%(16)==0){
				//Found page
				String header_parts[] = line_in_file.split(" ");
				if(header_parts.length!=4){
					System.out.println("Error:"+line_in_file);
					System.exit(0);
				}
				//get rest of page
				String records[] = new String[Page.RECORDS_PER_PAGE];
				for(int i=0;i<records.length;i++){
					records[i] = df.raf.readLine();
					line_ctr++;
				}
				return new Page(df.filename, header_parts[1], header_parts[3], records);
			}
			line_ctr++;
		}
		return null;
	} catch (IOException e) {
		e.printStackTrace();
		System.exit(0);
	}
	
	return null;
}

//public Page loadPage(DataFile df, int tid, int bid){
//	if(bid<0 || bid>df.BlockCount()|| -1==df.getPageIDByBlockID(bid)) return null;
//	int bid_ctr=0;
//	try {
//
//		df.inputStream = new ObjectInputStream(new BufferedInputStream(df.fis));
//		while(df.inputStream.available()>0){
//			Page p = (Page)df.inputStream.readObject();
//			if(bid_ctr==bid){
//				if(p.block_id==bid){
//					System.out.println("Both block ids match");
//				}
//				return p;
//			}
//			bid_ctr++;
//		}
//	}catch (EOFException e) {
//		//This means there is no page
//		return null;
//	}catch (IOException e) {
//		e.printStackTrace();
//		System.exit(0);
//	} catch (ClassNotFoundException e) {
//		e.printStackTrace();
//		System.exit(0);
//	}
//	return null;
//}

// Old loadPage using seek. Might not work
//public Page loadPage(DataFile df, int tid, int block_id){
//	Page page;
//	
//	try {		
//		if(block_id<0 || block_id>=df.BlockCount()||-1==df.getPageIDByBlockID(block_id)) return null;
//		
//		long pos = Page.PAGE_SPACING_BYTES * block_id;
//		FileChannel fc = df.fis.getChannel().position(pos);
//		System.out.println(fc.position()+" "+fc.size());
//		df.inputStream = new ObjectInputStream(new BufferedInputStream(df.fis));
//		page = (Page)df.inputStream.readObject();
//		page.fixed_by.add(tid);
//		//If not check if buffer is full if so do replacement, else just add Page
//		if(buffer.size()==buffer_size){
//			removePageInBuffer(df, null);
//		}
//		addToBuffer(page);
////		System.out.println("PAGE LOADED: "+page);
//		
//		return page;
//	}catch (EOFException e){
////		e.printStackTrace();
//		System.out.println("[DM] Page not found!");
//		return null;
//	}
//	catch (IOException e) {
//		e.printStackTrace();
//		System.exit(0);
//	} catch (ClassNotFoundException e) {
//		e.printStackTrace();
//		System.exit(0);
//	}
//	return null;
//}

public boolean flushPage(DataFile df, Page page_in_buffer){	
	if(page_in_buffer.block_id != df.getPIDIndexByPID(page_in_buffer.page_id)){
		System.out.println("Block IDs are different.");
		System.exit(0);
	}

	try {
		df.raf.seek(0);
		String line_in_file = "dumby";
		int line_ctr=0, bid_ctr=0;
		while(null!=line_in_file){
			long fd_ptr_before = df.raf.getFilePointer();
			line_in_file = df.raf.readLine();
			
			
			if(line_in_file!=null && line_ctr%16==0){
				//Beginning of page!
				String header_parts[] = line_in_file.split(" ");
				if(header_parts.length!=4){
					System.out.println("Error:"+line_in_file);
					System.exit(0);
				}
				if(Integer.parseInt(header_parts[3])==page_in_buffer.block_id || page_in_buffer.block_id==bid_ctr){
					//get rest of page
					String records[] = new String[Page.RECORDS_PER_PAGE];
					for(int i=0;i<records.length;i++){
						records[i] = df.raf.readLine();
						line_ctr++;
					}
					Page page_in_file = new Page(df.filename, header_parts[1], header_parts[3], records);
					//Write
					df.raf.seek(fd_ptr_before);
					
					//Some Pages may have more bytes than others, here we pad with whitespace (remove and replace the '\n' though)
					
					String toWrite = page_in_buffer.toString();
					toWrite = toWrite.substring(0, toWrite.length()-1);
					toWrite = String.format("%-"+(Page.PAGE_SIZE_BYTES-1)+"s",toWrite);
					toWrite+="\n";
					int new_size = toWrite.getBytes().length;
					df.raf.writeBytes(toWrite);
					
					//If not the same page, shift the old page down in file
					if(page_in_file.page_id != page_in_buffer.page_id){
						if(page_in_file.block_id == page_in_buffer.block_id){
							page_in_file.block_id++;
						}
						return flushPage(df, page_in_file);
					}
					return true;
				}
				bid_ctr++;
			}
			line_ctr++;
		}
		//Write
		String toWrite = page_in_buffer.toString();
		toWrite = toWrite.substring(0, toWrite.length()-1);
		toWrite = String.format("%-"+(Page.PAGE_SIZE_BYTES-1)+"s",toWrite);
		toWrite+="\n";
		df.raf.writeBytes(toWrite);
		return true;
	} catch (IOException e) {
		e.printStackTrace();
	}
	
	return false;
}

//public boolean flushPage(DataFile df, Page page_in_buffer){
//	int bid_ctr=0;
//	try {
//		ByteArrayOutputStream bos = new ByteArrayOutputStream();
//		df.outputStream = new ObjectOutputStream(bos);
////		df.fos.getChannel().position(0);
//		System.out.println("FIS: "+df.fis.getChannel().position()+" "+df.fis.getChannel().size());
//		System.out.println("FOS: "+df.fos.getChannel().position()+" "+df.fos.getChannel().size());
//		
//		try {
////			df.fis.getChannel().position(0);
//			df.inputStream = new ObjectInputStream(new BufferedInputStream(df.fis));
//			df.fis.getChannel().position(0);
//			
//		} catch (EOFException e) {
//			byte o[] = toBytes(page_in_buffer);
//			df.outputStream.writeObject(page_in_buffer);
//			df.outputStream.flush();
//			bos.writeTo(df.fos);
//			System.out.println("FIS: "+df.fis.getChannel().position()+" "+df.fis.getChannel().size());
//			System.out.println("FOS: "+df.fos.getChannel().position()+" "+df.fos.getChannel().size());
//			return true;
//		}catch (Exception e){
//			e.printStackTrace();
//			System.exit(0);
//		}
//
//		System.out.println("FIS: "+df.fis.getChannel().position()+" "+df.fis.getChannel().size());
//		System.out.println("FOS: "+df.fos.getChannel().position()+" "+df.fos.getChannel().size());
//		while(df.fis.getChannel().position()<df.fis.getChannel().size()){
////			df.inputStream = new ObjectInputStream(new BufferedInputStream(df.fis));
//			System.out.println("FIS: "+df.fis.getChannel().position()+" "+df.fis.getChannel().size());
//			System.out.println("FOS: "+df.fos.getChannel().position()+" "+df.fos.getChannel().size());
//
//			Page page_in_file = (Page)df.inputStream.readObject();
//			Page p2 = (Page)df.inputStream.readObject();
//
//			byte [] b = toBytes(page_in_file);
//			df.fis.getChannel().position(b.length);
//
//			System.out.println("FIS: "+df.fis.getChannel().position()+" "+df.fis.getChannel().size());
//			System.out.println("FOS: "+df.fos.getChannel().position()+" "+df.fos.getChannel().size());
//			
//			if(page_in_file.block_id == page_in_buffer.block_id || page_in_buffer.block_id==bid_ctr){
//				df.outputStream.writeObject(page_in_buffer);
//				df.outputStream.flush();
////				df.fos.getChannel().position(df.fis.getChannel().position());
//				bos.writeTo(df.fos);
//				System.out.println("FIS: "+df.fis.getChannel().position()+" "+df.fis.getChannel().size());
//				System.out.println("FOS: "+df.fos.getChannel().position()+" "+df.fos.getChannel().size());
////			buffer.remove(page_in_buffer);
//				if(page_in_file.page_id != page_in_buffer.page_id){
//					if(page_in_file.block_id == page_in_buffer.block_id){
//						page_in_file.block_id++;
//					}
//					return flushPage(df, page_in_file);
//				}
//				return true;
//			}
//			bid_ctr++;
//		}
//		df.outputStream.writeObject(page_in_buffer);
//		df.outputStream.flush();
//		bos.writeTo(df.fos);
//		return true;
//	}catch (EOFException e) {
//		e.printStackTrace();
//		System.exit(0);
//	}catch (IOException e) {
//		e.printStackTrace();
//		System.exit(0);
//	} catch (ClassNotFoundException e) {
//		e.printStackTrace();
//		System.exit(0);
//	}
//	return false;
//}

// Old flushPage function. Might not work all the time
///**
// * @param page_in_buffer; NOTE must be removed from buffer else where
// * @return if it was flushed or not
// */
//public boolean flushPage(DataFile df, Page page_in_buffer){
//	System.out.println("FLUSHING:\n"+page_in_buffer);
//	
////	DataFile df = getDataFileByName(page_in_buffer.file_of_origin);
////	if(df==null && page_in_buffer.file_of_origin.contains("_TEMP")){
////		page_in_buffer.file_of_origin = page_in_buffer.file_of_origin.split("_TEMP")[0];
////		df = getDataFileByName(page_in_buffer.file_of_origin);
////		if(df == null){
////			System.exit(0);
////		}
////	}
////	page_in_buffer.block_id = df.getPIDIndexByPID(page_in_buffer.page_id);
//	
//		Page page_in_file = null;
//		//Position
//		long pos = page_in_buffer.block_id * Page.PAGE_SPACING_BYTES;
//
//		ByteArrayOutputStream bos = new ByteArrayOutputStream(Page.PAGE_SPACING_BYTES);
//		try {
//			df.outputStream = new ObjectOutputStream(bos);
//			// If this page does not already exist get it's version from the file
//			if(page_in_buffer.page_id!=page_in_buffer.block_id){
//				FileChannel fc = df.fis.getChannel().position(pos);
////				System.out.println(fc.position()+" "+fc.size()+" "+df.fis.available());
//				if(fc.position()<fc.size()){
//					df.inputStream = new ObjectInputStream(new BufferedInputStream(df.fis));
//					page_in_file = (Page)df.inputStream.readObject();
//				}
////			df.inputStream.close();
//			}
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//			System.exit(0);
//		} catch (ClassNotFoundException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		try {
//		//Current LSN will become stable & LSN will be zero while not in memory buffer
//		page_in_buffer.stableLSN = page_in_buffer.LSN;
//		page_in_buffer.LSN = 0;
//		
//		df.outputStream.writeObject(page_in_buffer);
//		df.outputStream.flush();
//		FileChannel fc_o=null, fc_a=null;
//		//If the page_id in file is different from the one in buffer APPEND
//		if(null != page_in_file && page_in_file.page_id != page_in_buffer.page_id){
//			//Get the page to be over written
//			FileChannel fc = df.fis.getChannel().position(pos);
////			System.out.println(fc.position()+" "+fc.size());
//			if(fc.position()<fc.size()){
//				df.inputStream = new ObjectInputStream(new BufferedInputStream(df.fis));
//				page_in_file = (Page)df.inputStream.readObject();
//			}
//			
//			fc_a = df.fos_overwrite.getChannel().position(pos);
//			System.out.println("Aft:"+fc_a.position()+" "+fc_a.size());
//			bos.writeTo(df.fos_overwrite);
////			bos.w
//			bos.flush();
////			System.out.println(fc_a.position()+" "+fc_a.size());
//			page_in_file.block_id++;
////			if(fc.position()<fc.size()){
//				flushPage(df, page_in_file);
////			}
////			System.exit(0);
//		}else{
////			df.fos_append.getChannel().position(pos);
////			bos.writeTo(df.fos_append);
//			fc_a = df.fos_overwrite.getChannel().position(pos);
//			System.out.println("Bef"+fc_a.position()+" "+fc_a.size());
//			long before_size = fc_a.size();
//			bos.writeTo(df.fos_overwrite);
//			while(fc_a.size()<(before_size+Page.PAGE_SPACING_BYTES)){
//				bos.write(0);
//			}
//			bos.flush();
//			System.out.println("Aft"+fc_a.position()+" "+fc_a.size());
////			System.exit(0);
//		}
//		df.outputStream.close();
////		df.fos_append.close();
//		return true;
//	} catch (IOException e) {
//		e.printStackTrace();
//		System.exit(0);
//	} 
//	catch (ClassNotFoundException e) {
//		e.printStackTrace();
//		System.exit(0);
//	}
//
//	return false;
//}

/** Converts an object to an array of bytes . Uses the Logging
utilities in j2sdk1.4 for
* reporting exceptions.
* @param object the object to convert.
* @return the associated byte array.
*/
public static byte[] toBytes(Object object){
ByteArrayOutputStream baos = new ByteArrayOutputStream();
try{
ObjectOutputStream oos = new ObjectOutputStream(baos);
oos.writeObject(object);
}catch(java.io.IOException ioe){
System.exit(0);
}
return baos.toByteArray();
}

/** Converts an array of bytes back to its constituent object. The
input array is assumed to
* have been created from the original object. Uses the Logging
utilities in j2sdk1.4 for
* reporting exceptions.
* @param bytes the byte array to convert.
* @return the associated object.
*/
public static Object toObject(byte[] bytes){
Object object = null;
try{
object = new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
}catch(IOException ioe){
	System.exit(0);
}catch(ClassNotFoundException cnfe){
	System.exit(0);
}
return object;
} 
}
