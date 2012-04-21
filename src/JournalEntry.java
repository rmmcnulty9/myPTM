
public class JournalEntry {

	int tid;
	int df_id;
	int pid;
	String before_image;
	String after_image;
	
	public JournalEntry(String t, String d, String p, String b, String a) {
		try{
			tid = Integer.parseInt(t);
			df_id = Integer.parseInt(d);
			pid = Integer.parseInt(p);
			before_image=  b;
			after_image = a;
						
		}catch(NumberFormatException nex){
			nex.printStackTrace();
			System.exit(0);
		}
	}
	
	public String toString(){
		return tid+" "+df_id+" "+pid+" "+before_image+" "+after_image+"\n";
	}

}
