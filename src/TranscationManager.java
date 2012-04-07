import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;


public class TranscationManager extends Thread{

	public ArrayList<Transaction> transactions = new ArrayList<Transaction>();

	private int MAX_OPS_TO_READ = 10;
	private boolean shutdown_flag = false;
	private boolean random_read = false;
	private boolean rr_read = false;
	private Random rgen = null;

	private Scheduler scheduler = null;

	public void run(){
		int next_tid=1;

		while(!transactions.isEmpty()){
			
			if(rr_read){

				String op = null;
				if(isOpLeftInFile(next_tid)){
					try {
						op = getByTID(next_tid).br.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
					if (op == null){
						setOpsLeftInFile(next_tid, false);
					}else{
						getByTID(next_tid).add(new Operation(next_tid, op));
					}
				}
				next_tid+=1;
				if(next_tid>transactions.size()) next_tid=1;

			}else if(random_read){
				next_tid = rgen.nextInt(transactions.size());
				int num_lines = rgen.nextInt(MAX_OPS_TO_READ);

				if(isOpLeftInFile(next_tid)){
					for(int i=0;i<num_lines;i++){
						String op = null;
						try {
							op = getByTID(next_tid).br.readLine();
						} catch (IOException e) {
							e.printStackTrace();
						}
						if (op == null){
							setOpsLeftInFile(next_tid, false);
						}else{
							getByTID(next_tid).add(new Operation(next_tid, op));
						}
					}
				}
			}
		}
	}

	public TranscationManager(String read_method, int buffer, 
			String search_method, ArrayList<String> file_list){

		//Create transactions, one for each file
		for(int i=0;i<file_list.size();i++){
			try{
				FileInputStream fis = new FileInputStream(file_list.get(i));
				// Get the object of DataInputStream
				DataInputStream in = new DataInputStream(fis);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String strLine;
				//Read File Line By Line
				while ((strLine = br.readLine()) != null)   {
					// Print the content on the console
					System.out.println (strLine);
				}
				transactions.add(new Transaction(fis, in, br, i));
			}catch(IOException e){
				e.printStackTrace();
				System.exit(0);
			}
		}

		//Set flags for how to read from the transaction files
		if(read_method.equals("rr")){
			rr_read = true;
			random_read = false;
		}else{
			rr_read = false;
			random_read = true;
			rgen = new Random(Integer.parseInt(read_method));
		}

		if(scheduler==null){
			scheduler = new Scheduler(transactions, buffer, search_method);
			scheduler.run();
		}
	}


	
	public Transaction getByTID(int tid){
		for(int i=0;i<transactions.size();i++){
			if(transactions.get(i).tid == tid){
				return transactions.get(i);
			}
		}
		return null;
	}

	public boolean setOpsLeftInFile(int tid, boolean val){
		for(int i=0;i<transactions.size();i++){
			if(transactions.get(i).tid == tid){
				 transactions.get(i).ops_left_in_file = val;
				 return true;
			}
		}
		return false;
	}
	
	public boolean isOpLeftInFile(int tid){
		for(int i=0;i<transactions.size();i++){
			if(transactions.get(i).tid == tid){
				 return transactions.get(i).ops_left_in_file;
			}
		}
		return false;
	}
	
//	Not needed anymore - done in Scheduler
//	
//	public boolean removeByTID(int tid){
//		for(int i=0;i<transactions.size();i++){
//			if(transactions.get(i).tid == tid){
//				transactions.remove(i);
//				return true;
//			}
//		}
//		return false;
//	}

}
