
public class DataManager extends Thread {
	public Pair current_op = null;
	
	private int checkpoint_op_cnt;
	private int num_pages;
	
	public DataManager(Pair _current_op, int buffer) {
		current_op = _current_op;
		num_pages = buffer;
	}

	public void run(){
		
		if(current_op!=null){
			
		}
	}
}
