import java.util.TreeMap;


/*
 *
 */
public class LockTree {
	// This tree organizes the locks by filename.
	private TreeMap<String,RecordLockTree> fileTree = null;
	//private TreeMap<String,TreeMap<Integer, Lock>> fileTree = null;

	
	/*
	 * Class ctor.
	 */
	public LockTree(){
		fileTree = new TreeMap<String, RecordLockTree>();
	}


	/* @summary
	 * This method attempts to acquire the lock for the next operation
	 * in the target transaction.
	 *
	 * @return
	 * This method returns true if the lock was successfully acquired.
	 *
	 * @postcondition
	 * The transaction has either acquired its lock or is queued to
	 * receive it when it becomes available.
	 */
	public boolean acquireLock(Transaction targetTxn){
		Operation currOp = targetTxn.get(0);
		RecordLock targetLock = null;

		RecordLockTree recordTree = fileTree.get(currOp.filename);

		if (recordTree == null){
			// TODO: (jmg199) REMOVE AFTER TESTING.
			System.out.println("[Sched.LockTree] Need new RecordLockTree for file [" + currOp.filename + "]");
			
			// No locks yet exist for this file.
			recordTree = new RecordLockTree();

			// Add the Record tree to the file tree.
			fileTree.put(currOp.filename, recordTree);

			if (currOp.type.equals("D")){
				// Deletes occur at the file level. As twisted as this sounds, the
				// record tree contains locks for all records in a file, therefore
				// when locking a file the transaction doing the delete is stored
				// in the record tree.
				
				// Directly acquire the lock because the tree was just created (i.e. empty).
				recordTree.acquireFileLock(targetTxn);
				
				// Store the record lock tree in the Txn so that the file lock can
				// be released quickly.
				targetTxn.grantedFileLocks.add(recordTree);
				
				return true;
			}
			else {
				// Create the record lock.
				targetLock = new RecordLock(targetTxn);

				// Insert it into the record tree.
				recordTree.put(targetLock.recordLockId, targetLock);

				// Attempt to acquire the lock for the txn. Return whether it
				// actually gets it now.
				return targetLock.attemptAcquire(targetTxn);
			}
		}
		else{
			// TODO: (jmg199) REMOVE AFTER TESTING.
			System.out.println("[Sched.LockTree] Located RecordLockTree for file [" + currOp.filename + "]");
			
			// TODO: (jmg199) REMOVE AFTER TESTING.
			System.out.println("[Sched.LockTree] OPERATION TYPE [" + currOp.type + "]");

			if (currOp.type.equals("D")){
				// Deletes occur at the file level. As twisted as this sounds, the
				// record tree contains locks for all records in a file, therefore
				// when locking a file the transaction doing the delete is stored
				// in the record tree.
				
				// TODO: (jmg199) REMOVE AFTER TESTING.
				System.out.println("[Sched.LockTree] Processing a delete operation.");
				
				if (recordTree.hasFileLock(targetTxn)){
					// TODO: (jmg199) REMOVE AFTER TESTING.
					System.out.println("[Sched.LockTree] Txn already has file lock.");
					
					// Already have the lock.
					return true;
				}
				else {
					// TODO: (jmg199) REMOVE AFTER TESTING.
					System.out.println("[Sched.LockTree] Attempting to acquire file lock for file [" + currOp.filename + "]");
					
					return recordTree.attemptAcquireFileLock(targetTxn);
				}
			}
			else {
				if (recordTree.hasFileLock(targetTxn)){
					// Already have the lock. The record lock is not necessary.
					return true;
				}
				else {
					// A write has a record.  The others just have a val.
					if (currOp.type.equals("W")){
						targetLock = recordTree.get(currOp.record.ID);
					}
					else {
						targetLock = recordTree.get(Integer.valueOf(currOp.val));
					}

					if (targetLock == null){
						targetLock = new RecordLock(targetTxn);

						// Insert it into the record tree.
						recordTree.put(targetLock.recordLockId, targetLock);
					}

					// Attempt to acquire the lock for the txn. Return whether it
					// actually gets it now.
					return targetLock.attemptAcquire(targetTxn);
				}
			}
		}
	}
}
