import java.util.ArrayList;


public class DataManager extends Thread {
	public ArrayList<Pair> current_op = null;
	
	private boolean shutdown_flag=false;
	private int checkpoint_op_cnt;
	private int buffer_size;
	private String search_method;
	private Journal journal;
	private int buffer_mgmt_table[][];
	private ArrayList<Page> buffer;
	private ArrayList<Integer> page_ids;
	
	public ArrayList<String> data_files;
	
	public DataManager(ArrayList<Pair> _current_op, int _buffer_size, String _search_method, Scheduler s) {
		current_op = _current_op;
		buffer_size = _buffer_size;
		/*
		 * Buffer Management table has(in order):
		 * 	block ID, dirty bit, fix count, page LSN, Stable-LSN and page number
		 */
		
		buffer_mgmt_table = new int[buffer_size][6];
		buffer = new ArrayList<Page>(buffer_size);
		page_ids = new ArrayList<Integer>();
		
		search_method = _search_method;
		
		//TODO Initialize LockTree??
		data_files = new ArrayList<String>();
		journal = new Journal();
		
	}

	public void run(){
		
		while(!shutdown_flag){
			if(!current_op.isEmpty()){
				Pair cur_pair = current_op.get(0);
				
				Operation op = cur_pair.op;
				
				//TODO I think B(Begin) is processed by the Scheduler, I don't think it's needed in here
				
				if(op.type.equals("A")){
					
				}else if(op.type.equals("C")){
					
				}else{
					assert(op.filename!=null);
					//If the filename is not in pages file list add it
					if(!data_files.contains(op.filename)){
						
					}
				}
				
				
				if(search_method.equals("scan")){
					
					if(op.type.equals("R")){
						
						
					}else if(op.type.equals("W")){
						
					}else if(op.type.equals("D")){
						
					}
					
					
				}else if(search_method.equals("hash")){

					if(op.type.equals("R")){
						
						
					}else if(op.type.equals("W")){
						
					}else if(op.type.equals("D")){
						
					}
				}else{
					System.out.println("Bad Search Method.");
					System.exit(0);
				}
				
			}
		}
	}
	
	private class Page extends ArrayList<Record>{
		final int RECORDS_PER_PAGE = 15;
		String file_of_origin;
		
		public Page(String f){
			file_of_origin = f;
		}
		
		public boolean isFull(){
			
			assert(this.size()<=RECORDS_PER_PAGE);
			
			return this.size()== RECORDS_PER_PAGE;
		}
	}
	
}
