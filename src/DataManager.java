import java.util.ArrayList;


public class DataManager extends Thread {
	public Pair current_op = null;
	
	private boolean shutdown_flag=false;
	private int checkpoint_op_cnt;
	private int num_pages;
	private String search_method;
	private Journal journal;
	private int buffer_mgmt_table[][];
	public ArrayList<Page> data_file;
	
	public DataManager(Pair _current_op, int buffer, String df_name, String _search_method) {
		current_op = _current_op;
		num_pages = buffer;
		/*
		 * Buffer Management table has(in order):
		 * 	block ID, dirty bit, fix count, page LSN, Stable-LSN and page number
		 */
		
		buffer_mgmt_table = new int[num_pages][6];
		
		
		search_method = _search_method;
		
		//TODO Initialize LockTree/ open DataFile
		data_file = new ArrayList<Page>();
		journal = new Journal();
		
	}

	public void run(){
		
		while(!shutdown_flag){
			if(current_op!=null){
				
				if(search_method.equals("scan")){
					
				}else if(search_method.equals("hash")){
					
				}else{
					System.out.println("Bad Search Method.");
					System.exit(0);
				}
				
			}
		}
	}
}
