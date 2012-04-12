
public class Record implements java.io.Serializable{
	/**
	 * Serialize Records in order to save them to file
	 */
	private static final long serialVersionUID = -3205728889502067129L;
	public int ID;
	public String ClientName;
	public String Phone;
	
	public Record(int i, String c, String p){
		ID = i;
		ClientName = c;
		Phone = p;
	}
	
	public String toString(){
		return "["+ID+" "+ClientName+" "+Phone+"]";
	}
}
