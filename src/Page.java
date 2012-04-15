import java.util.ArrayList;


public class Page extends ArrayList<Record> implements java.io.Serializable{
	/**
	 * Generated serial ID so that Page objects can be written out to file
	 */
	private static final long serialVersionUID = -2938553534535400394L;
	public static final int RECORDS_PER_PAGE = 15;
	public static final int PAGE_SPACING_BYTES = 2048;

	String file_of_origin;
	
//	page ID, dirty bit, fix count, page LSN, Stable-LSN and page number
	int page_id;
	int block_id;
	ArrayList <Integer> dirtied_by;
	ArrayList <Integer> fixed_by;
	int LSN;
	int stableLSN;

	public Page(String f, int p, int b, int _LSN){
		file_of_origin = f;
		page_id = p;
		block_id = b;
		dirtied_by = new ArrayList <Integer>();
		fixed_by = new ArrayList <Integer>();
		LSN = LSN;
		stableLSN = 0;
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
		if ((super.size()+1)> RECORDS_PER_PAGE) return false;
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
		String s="Page "+page_id+" Block "+block_id+"\n";
		for(int i=0;i<this.size();i++){
			s+=	"Record "+i+": "+this.get(i).toString()+"\n";
		}
		return s;
	}
}
