import java.util.ArrayList;


public class Page extends ArrayList<Record> implements java.io.Serializable{
	/**
	 * Generated serial ID so that Page objects can be written out to file
	 */
	private static final long serialVersionUID = -2938553534535400394L;
	public static final int RECORDS_PER_PAGE = 15;
	public static final int PAGE_SIZE_BYTES = 512;
	public static final int RECORD_SIZE_BYTES = 34;

	String file_of_origin;
	int page_id;

	public Page(String f, int p){
		file_of_origin = f;
		page_id = p;
	}

	public boolean isFull(){

		assert(this.size()<=RECORDS_PER_PAGE);

		return this.size()== RECORDS_PER_PAGE;
	}

	public Record getRecordFromPage(int id){
		for(int i=0;i<this.size();i++){
			if(this.get(i).ID == id){
				return this.get(i);
			}
		}
		return null;
	}

	@Override
	public boolean add(Record r){
		if(r==null){
			System.out.println("null!!!");
		}
		if ((this.size()+1)> RECORDS_PER_PAGE) return false;
		return super.add(r);
	}
	
	
	public boolean addAtIndex(int i, Record r){
		if(r==null){
			System.out.println("null!!!");
		}
		if ((this.size()+1)> RECORDS_PER_PAGE) return false;
		super.add(i,r);
		return true;
	}
	
	public String toString(){
		String s="Page "+page_id+"\n";
		for(int i=0;i<RECORDS_PER_PAGE;i++){
			s+=	"Record "+i+": "+this.get(i).toString()+"\n";
		}
		return s;
	}
}
