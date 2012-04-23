import java.util.ArrayList;


public class Page extends ArrayList<Record> implements java.io.Serializable{
	/**
	 * Generated serial ID so that Page objects can be written out to file
	 */
	private static final long serialVersionUID = -2938553534535400394L;
	public static final int RECORDS_PER_PAGE = 15;
	public static final int PAGE_SIZE_BYTES = 512;
	
//	page ID, dirty bit, fix count, page LSN, Stable-LSN and page number
	int page_id;
	int block_id;
	int df_id;
	ArrayList <Integer> dirtied_by;
	ArrayList <Integer> fixed_by;

	public Page(int dfid, int p, int b){
		df_id= dfid;
		page_id = p;
		block_id = b;
		
		if(block_id<0 || page_id<0){
			System.exit(0);
		}
		
		dirtied_by = new ArrayList <Integer>();
		fixed_by = new ArrayList <Integer>();	
	}

	public Page(int dfid, String pid, String bid, String[] records) {
		df_id= dfid;
		page_id = Integer.parseInt(pid);
		block_id = Integer.parseInt(bid);
		dirtied_by = new ArrayList <Integer>();
		fixed_by = new ArrayList <Integer>();
		
		if(block_id<0 || page_id<0){
			System.exit(0);
		}
		
		for(int i=0;i<records.length;i++){
			if(!records[i].contains("~~ FREE ~~")) this.add(new Record(records[i]));
		}
	}

	public boolean isFull(){

		assert(this.size()<=RECORDS_PER_PAGE);

		return this.size()== RECORDS_PER_PAGE;
	}

	public Record getRecordFromPage(int rid){
		for(int i=0;i<this.size();i++){
			if(this.get(i).ID == rid){
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
			return false;
		}
		if ((this.size()+1)> RECORDS_PER_PAGE) return false;
		super.add(i,r);
		return true;
	}
	
	public boolean setAtIndex(int i, Record r){
		if(r==null){
			System.out.println("null!!!");
			return false;
		}
		super.set(i,r);
		return true;
	}
	
	public String toString(){
		String s="Page "+page_id+" Block "+block_id+"\n";
		for(int i=0;i<RECORDS_PER_PAGE;i++){
			if(i>=this.size()){
				s+= "~~ FREE ~~\n";
			}else{
				s+=	this.get(i).toString()+"\n";
			}
		}
		return s;
	}

	public void removeByRecordID(int ID) {
		for(int i=0;i<this.size();i++){
			if(this.get(i).ID==ID){
				this.remove(i);
				return;
			}
		}
		
	}
}
