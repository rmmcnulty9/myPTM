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
	
	
	private boolean shutdown_flag = false;
	private boolean random_read = false;
	private boolean rr_read = false;
	private Random rgen = null;
	
	public void run(){
		int next_tid=1;
		
		
		while(!shutdown_flag){
			try{
				if(rr_read){
					
					String op = transactions.get(next_tid).br.readLine();
					Map schedule_entry = new 
				
					schedule.add(new Map(new Integer(next_tid),op));
					
				}else if(random_read){
					
				}
			}catch(IOException e){
				e.printStackTrace();
				System.exit(0);
			}
		}
	}
	
	public TranscationManager(String read_method, int seed, ArrayList<String> file_list){
		
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
		}else if(read_method.equals("rand")){
			rr_read = false;
			random_read = true;
			rgen = new Random();
		}else{
			System.exit(0);
		}
		
	}
	
	public void getNextOperation(){
		
	}
	
	//create Scheduler
//	public void passTransactionToDM(Transaction t){
//		Scheduler.getTransaction
//	
//	}
	
}
