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
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;


public class DataManager extends Thread {

	public static int next_global_page_id;
	public static int next_df_id;

	public ArrayList<Operation> scheduled_ops = null;

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

	public DataManager(ArrayList<Operation> _scheduled_op, ArrayList<Operation> _completed_ops, int _buffer_size, String _search_method, Scheduler s) {
		scheduled_ops = _scheduled_op;
		completed_ops = _completed_ops;
		buffer_size = _buffer_size;
		next_df_id=0;
		next_global_page_id = 0;
		schedDoneFlag = false;
		scheduler  = s;

		buffer = new ArrayList<Page>(buffer_size);

		search_method = _search_method;

		data_files = new ArrayList<DataFile>();
		journal = new Journal();


	}

	public void run(){

		while(!schedDoneFlag || !scheduled_ops.isEmpty()){

			if(!scheduled_ops.isEmpty()){
				Operation op = scheduled_ops.remove(0);
				if(op == null){
					System.out.println("[DM] Operations is null??");
					System.exit(0);
				}
				DataFile df = getDataFileByName(op.filename);
				if(op.type.equals("A")){
					undoAbortedTransaction(op.tid);
					completed_ops.add(op);
					continue;
				}else if(op.type.equals("C")){
					System.out.println("[DM] Committing...");
					flushTransactionsPages(op.tid);
					//Clean those entries from the Journal
					//TODO Not tested!
					ArrayList<Integer> old_df_ids = journal.cleanByTID(op.tid);
					while(!old_df_ids.isEmpty()){
						DataFile old_df = getDataFileByDFID(old_df_ids.remove(0));
						if(old_df.isDeleted){
							old_df.close();
							old_df.delete();
							data_files.remove(old_df);
						}
					}
					completed_ops.add(op);
					continue;
				}else{
					//If the filename is not in pages file list add it
					if(null == df){
						df = new DataFile(op.filename,next_df_id);
						next_df_id++;
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
						flushBufferedPageByDataFile(df);
						df.isDeleted = true;
						journal.addEntry(op.tid, df.df_id, -1, df.df_id, -1);
						completed_ops.add(op);
						continue;
					}


					/**************
					 * HASH METHOD
					 **************/
				}else if(search_method.equals("hash")){

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
						
						next_global_page_id = addRecordToFileHash(df, op.record,op.tid,next_global_page_id);
						completed_ops.add(op);
						continue;
						
					}else if(op.type.equals("D")){
						delete_count++;
						System.out.println("[DM] Hash Method: Delete "+delete_count+"...");
						flushBufferedPageByDataFile(df);
						df.isDeleted = true;
						journal.addEntry(op.tid, df.df_id, -1, df.df_id, -1);
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

	private int addRecordToFileHash(DataFile df, Record record, int tid, int next_pid) {

		//If dataFile is empty add page
		if(df.isEmpty()){
			if(buffer.size() == buffer_size){
				System.out.println("Buffer should be empty here.");
				System.exit(0);
				//								removePageInBuffer(df, null);
			}
			//Create new page
			Page p = new Page(df.df_id,next_pid, 0);
			next_pid++;
			df.addPageIDToBlockIDIndex(p.block_id,p.page_id);
			p.dirtied_by.add(tid);

			if(!p.add(record)){
				System.out.println("Must be able to add to this page.");
				System.exit(-1);
			}else{
				journal.addEntry(tid, df.df_id, -1, -1, df.df_id);
			}
			addToBuffer(p);

			return next_pid;
		}

		int page_id = record.ID % df.BlockCount();
		Page p = getBufferedPageByPageID(page_id);
		if(p==null){
			// Not in buffer look in file
			p = loadPage(df, tid, page_id);
		}
		if(p == null){
			System.out.println("[DM] Records not found!");
			return rehashDataFile(df, record, tid, next_pid);
		}else{
			for(int i=0;i<p.size();i++){
				Record cur_r = p.get(i);
				if(cur_r.ID>=record.ID){
					if(cur_r.ID==record.ID){
						if(!p.setAtIndex(i, record)){
							System.out.println("Have to be able to update this record!");
							System.exit(0);
						}else{
							journal.addEntry(tid, df.df_id, p.page_id, cur_r.toString(), record.toString());
							return next_pid;
						}
					}else if(cur_r.ID>record.ID){
						if(!p.addAtIndex(i, record)){
							return rehashDataFile(df, record, tid, next_pid);
						}else{
							journal.addEntry(tid, df.df_id, p.page_id, null, record.toString());
							return next_pid;
						}
					}else{
						System.out.println("This record can not be less here.");
						System.exit(0);
					}
				}else if(i==Page.RECORDS_PER_PAGE-1){
					return rehashDataFile(df, record, tid, next_pid);
					
				}else if(i==p.size()-1){
					if(!p.add(record)){
						System.out.println("This should not trip!");
						System.exit(0);
					}else{
						journal.addEntry(tid, df.df_id, p.page_id, null, record.toString());
						return next_pid;
					}
				}
			}
			
		}
		return next_pid;
	}

	private void undoAbortedTransaction(int tid) {
		//Get list of operations to undo
		ArrayList<JournalEntry> undos = journal.undoByTID(tid);
		if(undos==null || undos.isEmpty()) return;
		
		while(!undos.isEmpty()){
			JournalEntry cur = undos.remove(0);
			DataFile df = getDataFileByDFID(cur.df_id);
			//If deleted file: restore the old data file
			if(Integer.parseInt(cur.before_image) == cur.df_id){
	
					//TODO implement the rest of this!
				return;
			}
			//If file created: Delete that datafile
			if(Integer.parseInt(cur.after_image) == cur.df_id){
				df.isDeleted = true;
				df.close();
				df.delete();
				data_files.remove(df);
				return;
			}
			//If an update: execute the before image
			if(!cur.before_image.equals("-1") && !cur.after_image.equals("-1")){
				Record before_r = new Record(cur.before_image);
				if(search_method.endsWith("scan")){
					next_global_page_id = addRecordToFileScan(df, before_r, tid, next_global_page_id);
				}else if(search_method.equals("hash")){
					next_global_page_id = addRecordToFileHash(df, before_r, tid, next_global_page_id);
				}else{
					System.out.println("Bad search method.");
					System.exit(0);
				}
			}
			
			// If an add: execute a delete record
			if(cur.before_image.equals("-1") && !cur.after_image.equals("-1")){
				//TODO write this function
			}
		}
	}

	private void flushBufferedPageByDataFile(DataFile df) {
		int orig_size = buffer.size();
		int touched = 0;
		while(touched!= orig_size){
			Page p = buffer.get(touched);
			if(p.df_id==df.df_id){
				buffer.remove(p);
				touched--;
				orig_size--;
			}
			touched++;
		}
	}

	private int rehashDataFile(DataFile df, Record record, int tid, int next_pid) {
		
		//First flush the whole buffer
		while(!buffer.isEmpty()){
			System.out.println("Emptying buffer!");
			flushPage(df, buffer.remove(0));
		}

		// Create temp file with double the number of block in df
		DataFile new_df = new DataFile(df.filename+"_TEMP", next_df_id);
		next_df_id++;
		new_df.initializeDataFileSize(df.BlockCount() * 2);
		data_files.add(new_df);

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
					new_page = new Page(new_df.df_id, next_pid, new_bid);
					next_pid++;
					new_df.setPageIDInBlockIDIndex(new_page.block_id,new_page.page_id);
					new_page.dirtied_by.add(tid);
				}
				new_page.add(p.get(k));
				flushPage(new_df, new_page);
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
		if(!ret){
			System.out.println("Failed to rename new datafile.");
			System.exit(0);
		}
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
			p.df_id = df.df_id;
			flushPage(new_df, p);
		}

		try {
			df.raf.close();
			new_df.f = new File(new_df.filename);
			boolean r = new_df.f.exists();
			df.raf = new RandomAccessFile(new_df.f,"rw");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}

		return next_pid;

		//		return next_global_page_id+df.BlockCount();
	}

	/**
	 * @param tid - flush all pages associated with the transaction with tid
	 */
	private void flushTransactionsPages(int tid) {
		int orig_size = buffer.size();
		int touched = 0;
		while(touched!= orig_size){
			Page p = buffer.get(touched);
			boolean isDirty = p.dirtied_by.remove(new Integer(tid));
			p.fixed_by.remove(new Integer(tid));
			if(isDirty){
				DataFile df = getDataFileByDFID(p.df_id);
				flushPage(df, p);
				if(p.fixed_by.size()==0 ){
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

	/**
	 * @param df - The data file the record will belong to
	 * @param new_r - the record to be added
	 * @param tid - the requesting transaction
	 * @param next_pid - the next pid to be assigned to a new page
	 * @return the possibly updated pid counter
	 */
	private int addRecordToFileScan( DataFile df, Record new_r, int tid, int next_pid) {
		//If dataFile is empty add page
		if(df.isEmpty()){
			if(buffer.size() == buffer_size){
				removePageInBuffer(df, null);
			}
			//Page creation
			Page p = new Page(df.df_id, next_pid, 0);
			df.addPageIDToBlockIDIndex(p.block_id,p.page_id);
			p.dirtied_by.add(tid);

			if(!p.add(new_r)){
				System.out.println("Must be able to add to this page.");
				System.exit(-1);
			}else{
				journal.addEntry(tid, df.df_id, -1, -1, df.df_id);
			}
			addToBuffer(p);

			return (next_pid+1);
		}

		/*
		 * Binary search to find correct bid in file
		 */
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
					if(cur_r.ID>=new_r.ID){
						if(cur_r.ID == new_r.ID){
							if(!loc_page.setAtIndex(i, new_r)){
								System.out.println("Updating existing record can't fail.");
								System.exit(0);
							}else{
								journal.addEntry(tid, df.df_id, loc_page.page_id, cur_r.toString(), new_r.toString());
								return next_pid;
							}
						}else if(cur_r.ID>new_r.ID){
							if(!loc_page.addAtIndex(i, new_r)){
								return splitPage(df, loc_page, new_r, next_pid, tid, i);
							}else{
								journal.addEntry(tid, df.df_id, loc_page.page_id, null, new_r.toString());
								return next_pid;
							}
						}
					}else if(i==Page.RECORDS_PER_PAGE-1){
						return splitPage(df, loc_page, new_r, next_pid, tid, i);
					}
				}
				//Add to end
				if(!loc_page.add(new_r)){
					System.out.println("Adding record can't fail here.");
					System.exit(0);
				}else{
					journal.addEntry(tid, df.df_id, loc_page.page_id, null, new_r.toString());
					return next_pid;
				}

			}else{
				System.out.println("Bad location: "+location);
				System.exit(0);
			}
		}
		System.out.println("Should not get here!");
		System.exit(0);

		return next_pid;
	}



	/**
	 * @param df - data file to flush the page from
	 * @param in_use_now - the page in use that should not be flushed
	 */
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
		//Create new page
		Page new_p = new Page(df.df_id, next_pid, old_p.block_id+1);
		new_p.dirtied_by.add(tid);

		//If not the first index then the record goes in first spot in new page
		if(i!=0){
			df.addPageIDToBlockIDIndex(new_p.block_id,new_p.page_id);
			//Add the new record at beginning of new page
			if(!new_p.add(r)){		
				System.out.println("Adding record can't fail here.");
				System.exit(0);
			}else{
				journal.addEntry(tid, df.df_id, new_p.page_id, null, r.toString());
				return next_pid;
			}
			flushPage(df, new_p);
		}
		
		//If NOT the last record in page then move all that are after i into new page
		if(i!=Page.RECORDS_PER_PAGE-1){
			//Move record from i...last
			while(old_p.size()!=i){
				Record cur_r = old_p.remove(i);
				journal.addEntry(tid, df.df_id, old_p.page_id, cur_r.toString(), null);
				if(!new_p.add(cur_r)){
					System.out.println("Must have room in this page.");
					System.exit(0);
				}else{
					journal.addEntry(tid, df.df_id, new_p.page_id, null, cur_r.toString());
				}
			}
		}
		// If i is zero only add the new record to the new page
		if(i==0){
			df.addPageIDToBlockIDIndex(new_p.block_id,new_p.page_id);
			if(!old_p.add(r)){		//Add the new record at beginning of OLD page
				System.out.println("[DM] Can not add to this page!");
			}else{
				journal.addEntry(tid, df.df_id, old_p.page_id, null, r.toString());
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

	public DataFile getDataFileByDFID(int id){
		for(int i=0;i<data_files.size();i++){
			if(data_files.get(i).df_id == id){
				return data_files.get(i);
			}
		}
		return null;
	}

	private DataFile getDataFileByName(String filename) {
		for(int i=0;i<data_files.size();i++){
			DataFile df = data_files.get(i);
			if(df.filename.equals(filename)&& !df.isDeleted){
				return df;
			}
		}
		return null;
	}

	/**
	 * @param df
	 * @param tid
	 * @param bid
	 * @return the page from the data file with matching bid fixed by transaction with tid
	 */
	public Page loadPage(DataFile df, int tid, int bid){
		if(bid<0 || bid>=df.BlockCount()||-1==df.getPageIDByBlockID(bid)) return null;

		try{	
			String line_in_file = "dumby";
			df.raf.seek(0);
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
					if (Integer.parseInt(header_parts[3])==bid) {
						//get rest of page
						String records[] = new String[Page.RECORDS_PER_PAGE];
						for (int i = 0; i < records.length; i++) {
							records[i] = df.raf.readLine();
							line_ctr++;
						}
						Page p = new Page(df.df_id, header_parts[1], header_parts[3], records);
						p.fixed_by.add(new Integer(tid));
						return p;
					}
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

	/**
	 * @param df - DataFile to flush to
	 * @param page_in_buffer - the page in to flush from the buffer
	 * @return - success or failure
	 */
	public boolean flushPage(DataFile df, Page page_in_buffer){	
		page_in_buffer.block_id = df.getPIDIndexByPID(page_in_buffer.page_id);

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
						Page page_in_file = new Page(df.df_id, header_parts[1], header_parts[3], records);
						//Write
						df.raf.seek(fd_ptr_before);

						//Some Pages may have more bytes than others, here we pad with whitespace (remove and replace the '\n' though)

						String toWrite = page_in_buffer.toString();
						toWrite = toWrite.substring(0, toWrite.length()-1);
						toWrite = String.format("%-"+(Page.PAGE_SIZE_BYTES-1)+"s",toWrite);
						toWrite+="\n";
//						int new_size = toWrite.getBytes().length;
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
}
