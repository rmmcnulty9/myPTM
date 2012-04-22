/*
 * This class stores what is an instance of a lock.  Its type
 * is determined by the operation for which it was granted and
 * the mode under which the txn/process is running.
 * 
 * LockTypes live within a Lock. A transaction can have only one lock
 * type within a lock. So a lock type can be upgraded if necessary.
 */
public class LockType {
    public static enum LockTypeFlag {
        PROC_READ, PROC_WRITE, TRAN_READ, TRAN_WRITE
    }

	public LockTypeFlag lockType;
	public Transaction lockHolder;
	public RecordLock parentRecordLock = null;
	
	
	/*
	 * 
	 */
	public LockType(Transaction _lockHolder, RecordLock _parentRecordLock){
		// Set the lock type.
		// (NOTE: Deletes are handled in file locks. Aborts and Commits never show up here.)
		if (_lockHolder.mode == 0){
			// This is a process.
			if (_lockHolder.get(0).type.equals("R")){
				// This is a process read.
				lockType = LockTypeFlag.PROC_READ;
			}
			else {
				// This is a process write.
				lockType = LockTypeFlag.PROC_WRITE;
			}
		}
		else {
			// This is a transaction.
			if (_lockHolder.get(0).type.equals("R")){
				// This is a transaction read.
				lockType = LockTypeFlag.TRAN_READ;
			}
			else {
				// This is a transaction write.
				lockType = LockTypeFlag.TRAN_WRITE;
			}
		}
		
		lockHolder = _lockHolder;
		parentRecordLock = _parentRecordLock;
	}
}
