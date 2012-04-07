import java.util.ArrayList;
import java.util.Map;


public class Scheduler extends Thread{

	private int buffer_size;
	private String search_method;
	
	public ArrayList<Operation> current_ops = null;

	private ArrayList<Transaction> transactions = null;
	private int deadlock_op_cnt;

	private DataManager dm_task = null;

	public Scheduler(ArrayList<Transaction> _transactions, int _buffer_size, String _search_method){
		buffer_size = _buffer_size;
		search_method = _search_method;
		transactions = _transactions;
		current_ops = new ArrayList<Operation>();
		
	}

	public void run(){
		if(dm_task == null){
			dm_task = new DataManager(current_ops, buffer_size, search_method, this);
			System.out.println("Started DataManager...");
			dm_task.start();
		}
		while(!transactions.isEmpty()){
			if(!transactions.get(0).isEmpty()){
				Operation o = transactions.get(0).get(0);
				transactions.get(0).remove(0);
				if(o.type.equals("B")){
					continue;
				}
				System.out.println("Scheduled this operation: "+o.toString());
				current_ops.add(o);
			}
		}


	}

}
