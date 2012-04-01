
public class myPTM {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*
		 * myPTM --scheduler <rr|seed#> --buffer <integer> --processes <1...N file names> 
		 */
		TranscationManager tm = new TranscationManager(null, 0, null);
		tm.start();
		// created in TM
//		Scheduler s = new Scheduler();
//		s.start();
//		//created in Scheduler
//		DataManager dm = new DataManager();
//		dm.start();

	}

}
