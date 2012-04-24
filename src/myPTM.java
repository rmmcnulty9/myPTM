import java.util.ArrayList;


public class myPTM {

	static final boolean verbose = false;
	
	public static void main(String[] args) {
		/*
		 * myPTM --scheduler <rr|seed#> --buffer <integer> --search <scan|hash> --processes <1...N file names> 
		 */
		//For TM
		String scheduler = null;
		ArrayList<String> files = new ArrayList<String>();
		
		//For DM
		String search_method=null;
		int buffer = -1;
		
		for(int i=0;i<args.length;i++){
			if(args[i].equals("--scheduler")){
				scheduler = args[i+1];
			}
			
			if(args[i].equals("--buffer")){
				buffer = Integer.parseInt(args[i+1]);
			}
			
			if(args[i].equals("--search")){
				search_method = args[i+1];
			}
			
			if(args[i].equals("--processes")){
				for(int j=i+1;j<args.length;j++){
					files.add(args[j]);
				}
				break;
			}
		}
		if(scheduler == null){
			System.out.println("Bad Scheduler");
			System.out.println("myPTM --scheduler <rr|seed#> --buffer <integer> --search <scan|hash> --processes <1...N file names>");
			System.exit(0);
		}
		
		if(search_method == null){
			System.out.println("Bad Search Method");
			System.out.println("myPTM --scheduler <rr|seed#> --buffer <integer> --search <scan|hash> --processes <1...N file names>");
			System.exit(0);
		}
		
		if(buffer == -1){
			System.out.println("Bad Buffer Size");
			System.out.println("myPTM --scheduler <rr|seed#> --buffer <integer> --search <scan|hash> --processes <1...N file names>");
			System.exit(0);
		}
		
		if(files.size()==0){
			System.out.println("Need to specify files");
			System.out.println("myPTM --scheduler <rr|seed#> --buffer <integer> --processes <1...N file names> ");
			System.exit(0);
		}
		
		TranscationManager tm = new TranscationManager(scheduler, buffer, search_method, files, verbose);
		System.out.println("Started TransactionManager...");
		tm.start();

	}

}
