import java.util.concurrent.*;


public class TransactionList extends CopyOnWriteArrayList<Transaction>{
    /**
	 *
	 */
	private static final long serialVersionUID = -6986839082145919601L;


	/*
     * @summary
     * This is the class ctor.
     */
    public TransactionList(){
        // Nothing to initialize here.
    }


    /*
     * @summary
     *
     */
    public Transaction getByTID(int tid){
        for (int index = 0; index < size(); index++){
            if (get(index).tid == tid){
                return get(index);
            }
        }

        return null;
    }


    /*
     * @summary
     * 
     */
    public Transaction getByIndex(int index){
        if (index >= size()){
            return null;
        }

        return get(index);
    }


    /*
     * @summary
     *
     */
    public boolean setOpsLeftInFile(int tid, boolean val){
        for (int i = 0; i<size(); i++){
            if (get(i).tid == tid){
                get(i).ops_left_in_file = val;
                return true;
            }
        }

        return false;
    }


    /*
     * @summary
     * Convenience function.
     */
    public boolean isOpLeftInFile(int tid){
        return getByTID(tid).ops_left_in_file;
    }


    /*
     * @summary
     * This method removes the transaction by it's ID.
     */
    public boolean removeByTID(int tid){
        for (int i = 0; i < size(); i++){
            if(get(i).tid == tid){
                remove(i);
                return true;
            }
        }

        return false;
    }
}
