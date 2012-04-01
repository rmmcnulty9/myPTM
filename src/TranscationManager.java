import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;


public class TranscationManager extends Thread{
	
	private ArrayList<Transaction> transactions = new ArrayList<Transaction>();
	private boolean shutdown_flag=false;
	
	
	
	public void run(){
		while(!shutdown_flag){
			
		}
	}
	
	public TranscationManager(String read_method, int seed, ArrayList<String> file_list){
		
		//Create transactions, one for each file
		for(int i=0;i<file_list.size();i++){
			try{
				FileInputStream fis = new FileInputStream(file_list.get(i));
				
			transactions.add(new Transaction(fis));
			}catch(IOException e){
				e.printStackTrace();
				System.exit(0);
			}
		}
		
		
		
	}
	
	public void getNextOperation(){
		
	}
	
	
}
