import java.util.ArrayList;


public class myPTM {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*
		 * myPTM --scheduler <rr|seed#> --buffer <integer> --processes <1...N file names> 
		 */
		String scheduler = null;
		int buffer = -1;
		ArrayList<String> files = new ArrayList<String>();
		for(int i=0;i<args.length;i++){
			if(args[i].equals("--scheduler")){
				scheduler = args[i+1];
			}
			
			if(args[i].equals("--buffer")){
				buffer = Integer.parseInt(args[i+1]);
			}
			
			if(args[i].equals("--preocesses")){
				for(int j=i+1;j<args.length;j++){
					files.add(args[j]);
				}
				break;
			}
		}
		if(scheduler == null){
			System.out.println("Bad Scheduler");
			System.out.println("myPTM --scheduler <rr|seed#> --buffer <integer> --processes <1...N file names> ");
			System.exit(0);
		}
		
		if(buffer == -1){
			System.out.println("Bad Buffer Size");
			System.out.println("myPTM --scheduler <rr|seed#> --buffer <integer> --processes <1...N file names> ");
			System.exit(0);
		}
		
		if(files.size()==0){
			System.out.println("Need to specify files");
			System.out.println("myPTM --scheduler <rr|seed#> --buffer <integer> --processes <1...N file names> ");
			System.exit(0);
		}
		
		TranscationManager tm = new TranscationManager(scheduler, buffer, files);
		tm.start();


	}

}
