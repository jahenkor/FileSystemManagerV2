package edu.gmu.cs475;

import edu.gmu.cs475.struct.NoSuchTagException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import static org.hibernate.validator.internal.util.CollectionHelper.newArrayList;

public class FileManagerClient extends AbstractFileManagerClient {

	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock readLock = readWriteLock.readLock();
	private final Lock writeLock = readWriteLock.writeLock();
	HashMap<Long, String> transactionWrites = new HashMap<>(); //stores transactions and their respective files
	HashMap<String, String> clientFiles = new HashMap<>();// list of tagged files
	HashMap<String, String> originalClientFiles = new HashMap<>();
	HashMap<Long, String> lockMap = new HashMap<>();//holds file and lock values
        private HashMap<Long, String> readLocked = new HashMap<>();
    private HashMap<Long, String> writeLocked = new HashMap<>();


        
	public FileManagerClient(String host, int port) {
		super(host, port);

	}

	/**
	 * Used for tests without a real server
	 *
	 * @param server
	 */
	public FileManagerClient(IFileManagerServer server) {
		super(server);
		startReplica();
	}

	/**
	 * Initialzes this read-only replica with the current set of files
	 *
	 * @param files A map from filename to file contents
	 */
	@Override
	protected void initReplica(HashMap<String, String> files) {

		writeLock.lock();
		try {
			clientFiles.putAll(files);
			originalClientFiles.putAll(files);
		}
		finally {
			writeLock.unlock();
		}
	}

	/**
	 * Lists all of the paths to all of the files that are known to the client
	 *
	 * @return the file paths
	 */
	@Override
	public Iterable<String> listAllFiles() {
		List<String> fileList = new ArrayList<>();
		writeLock.lock();
		try {
			for(String x : clientFiles.keySet()){
                            fileList.add(x);
                        }
		} finally {
			writeLock.unlock();
		}

		return fileList;
	}

	/**
	 * Prints out all files. Must internally synchronize
	 * to guarantee that the list of files does not change
	 * during its call, and that each file printed does not change during its
	 * execution (using a read/write lock). You should acquire all of the locks,
	 * then read all of the files and release the locks. Your code should not
	 * deadlock while waiting to acquire locks.
	 *
	 * @return The concatenation of all of the files
	 * @throws NoSuchTagException If no tag exists with the given name
	 * @throws IOException        if any IOException occurs in the underlying read, or if the
	 *                            read was unsuccessful (e.g. if it times out, or gets
	 *                            otherwise disconnected during the execution
	 */
	@Override
	public String catAllFiles() throws NoSuchTagException, IOException {
		String catFiles = "";
		List<String> fileList = new ArrayList<>();
               
                writeLock.lock();
                try{
                for(String file : listAllFiles()){
                    fileList.add(file);
                }
                }
                
                finally{
                    writeLock.unlock();
                }
                
                //readLock.lock();
                try{
		for (String name : fileList) {//lock all files to be used
			long stamp = lockFile(name, false);
                        readLocked.put(stamp, name);
		}
	//	try {
			if(!fileList.iterator().hasNext()){
				throw new NoSuchTagException();
			}
          //      }
                //finally{
                  //  readLock.unlock();
                //}

          
			//Find file content according to tag, and store in string
			System.out.println(fileList);
			for (String tagFile : fileList) {
				catFiles += readFile(tagFile);
			}
		}finally {
			for(Long stamp : readLocked.keySet()){
                            super.unLockFile(readLocked.get(stamp), stamp, false);
                        }
		}

		return catFiles;
	}

	/**
	 * Echos some content into all files. Must internally
	 * synchronize to guarantee that the list of files does
	 * not change during its call, and that each file being printed to does not
	 * change during its execution (using a read/write lock)
	 * <p>
	 * Given two concurrent calls to echoToAllFiles, it will be indeterminate
	 * which call happens first and which happens last. But what you can (and
	 * must) guarantee is that all files will have the *same* value (and not
	 * some the result of the first, qnd some the result of the second). Your
	 * code should not deadlock while waiting to acquire locks.
	 * <p>
	 * Must use a transaction to guarantee that all writes succeed to all replicas (or none).
	 *
	 * @param content The content to write out to each file
	 * @throws NoSuchTagException If no tag exists with the given name
	 * @throws IOException        if any IOException occurs in the underlying write, or if the
	 *                            write was unsuccessful (e.g. if it times out, or gets
	 *                            otherwise disconnected during the execution)
	 */
	@Override
	public void echoToAllFiles(String content) throws IOException {
            
            long transID = super.startNewTransaction();
            boolean writeStatus = false;
               
		int i = 0;
                List<String> fileList = new ArrayList<>();
                
                writeLock.lock();
                try{
                for(String file : clientFiles.keySet()){
                    fileList.add(file);
                    
                }}
                finally{
                   writeLock.unlock();
                }
                
                readLock.lock();
                try{
		for (String name : fileList) {//lock all files to be used
			long stamp = super.lockFile(name, true);
                        writeLocked.put(stamp, name);
		}
                }
                finally{
                    readLock.unlock();
                }
             

		try {
			if (!fileList.iterator().hasNext()) {
				throw new NoSuchTagException();
			}
                        
                       

			for (String tagFile : fileList) {//write to all files in list
				writeStatus = super.writeFileInTransaction(tagFile, content, transID);
                                if(!writeStatus){
                                    throw new IOException("File write failed");
                                }
			}
                        super.issueCommitTransaction(transID);
		} catch(IOException e){
                 super.issueAbortTransaction(transID);
                    
                }finally {//unlock files now that we are done
			for(Long stamp : writeLocked.keySet()){
                            super.unLockFile(writeLocked.get(stamp), stamp, true);
                        }
		}
	}

	/**
	 * Return a file as a byte array.
	 *
	 * @param file Path to file requested
	 * @return String representing the file
	 * @throws IOException if any IOException occurs in the underlying read
	 */
	@Override
	public String readFile(String file) throws RemoteException, IOException {
		readLock.lock();
		String content = "";
		String content2 = "";
		try{
			System.out.println();
			System.out.println("Original clientFiles");
			content = originalClientFiles.get(file);
			System.out.println(content);
			System.out.println();
			content2 = clientFiles.get(file);
			System.out.println("clientFiles");
			System.out.println(content2);
            } finally{
                readLock.unlock();
            }
		return content;
	}

	/**
	 * Write (or overwrite) a file
	 *
	 * @param file    Path to file to write out
	 * @param content String representing the content desired
	 * @param xid     Transaction ID, if this write is associated with any transaction, or 0 if it is not associated with a transaction
	 *                If it is associated with a transaction, then this write must not be visible until the replicant receives a commit message for the associated transaction ID; if it is aborted, then it is discarded.
	 * @return true if the write was successful and we are voting to commit
	 * @throws IOException if any IOException occurs in the underlying write
	 */
	@Override
	public boolean innerWriteFile(String file, String content, long xid) throws RemoteException, IOException {

		writeLock.lock();
		try{
			if (xid == 0) {

				System.out.println("xid = 0");
				originalClientFiles.put(file, content);
				//clientFiles.put(file, content);

			}
			else{
				System.out.println("xid != 0");
				transactionWrites.put(xid, file);
				clientFiles.put(file, content);
				//originalClientFiles.put(file, content);
				}
            }
            finally{
                writeLock.unlock();
            }
           
            
                
                
		return true;
	}

	/**
	 * Commit a transaction, making any pending writes immediately visible
	 *
	 * @param id transaction id
	 * @throws RemoteException
	 * @throws IOException     if any IOException occurs in the underlying write
	 */
	@Override
	public void commitTransaction(long id) throws RemoteException, IOException {
            
            if(id == 0){
                return;
            }
           
            //Move file from temp cache to memory
            writeLock.lock();
            try{
            	System.out.println("writing to original file id: " + id+ "file: " + transactionWrites.get(id) + "content: " + clientFiles.get(transactionWrites.get(id)));
            originalClientFiles.put(transactionWrites.get(id), clientFiles.get(transactionWrites.get(id)));
            }
            
            finally{
                writeLock.unlock();
            }
            //super.issueCommitTransaction(id);
            
           
	}

	/**
	 * Abort a transaction, discarding any pending writes that are associated with it
	 *
	 * @param id transaction id
	 * @throws RemoteException
	 */
	@Override
	public void abortTransaction(long id) throws RemoteException {

            writeLock.lock();
            try{
            //Remove transaction
                String file = transactionWrites.remove(id);
                System.out.println( "aborttransaction file is "+ file +" at id " + id);
                
                //Revert commit
				System.out.println("aborting transaction");
                //clientFiles.put(file, originalClientFiles.get(file));
				clientFiles.put(file, originalClientFiles.get(file));
            }
            finally{
                writeLock.unlock();
            }
	}
	private void unlockAll(List<String> fileList, boolean forWrite) throws NoSuchFileException, RemoteException {
		int i = 0;
		List<Long> toRemove = new ArrayList();
		while( i < fileList.size()) {
			for (Map.Entry<Long, String> st : lockMap.entrySet()) {//iterate over map
				if (fileList.get(i).equals( st.getValue())) {
					toRemove.add(st.getKey());//add files to remove to list
				}
			}
			i++;
		}
		for(Long stamp : toRemove){//remove them all now be we cant remove from a list while iterating it
			unLockFile(lockMap.get(stamp), stamp, forWrite);
		}
	}

}

