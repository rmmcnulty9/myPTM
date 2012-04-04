
public class DataManager extends Thread {
	public Pair current_op = null;

	private int checkpoint_op_cnt;
	private int num_pages;
    private Scheduler parent_sched_task = null;

	public DataManager(Pair _current_op, int buffer, Scheduler _parent_sched_task) {
		current_op = _current_op;
		num_pages = buffer;
        parent_sched_task = _parent_sched_task;
	}

	public void run(){

		if(current_op!=null){

		}
	}
}
