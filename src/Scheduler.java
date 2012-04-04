import java.util.ArrayList;
import java.util.Map;


public class Scheduler extends Thread{
	
	public Pair current_op = null;
	
	private ArrayList<Transaction> transactions = null;
	private int deadlock_op_cnt;
	
	private DataManager dm = null;
	
	public Scheduler(ArrayList<Transaction> _transactions, int buffer, String df_name, String search_method){
		transactions = _transactions;
		
		if(dm == null){
			dm = new DataManager(current_op, buffer, df_name, search_method);
			dm.run();
		}
	}
	
	public void run(){
		
		
	}
	
}
