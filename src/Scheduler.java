import java.util.ArrayList;
import java.util.Map;


public class Scheduler extends Thread{

	public ArrayList<Operation> current_op = null;

	private ArrayList<Transaction> transactions = null;
	private int deadlock_op_cnt;

	private DataManager dm_task = null;

	public Scheduler(ArrayList<Transaction> _transactions, int buffer, String search_method){
		transactions = _transactions;

		if(dm_task == null){
			dm_task = new DataManager(current_op, buffer, search_method, this);
			dm_task.run();
}
	}

	public void run(){


	}

}
