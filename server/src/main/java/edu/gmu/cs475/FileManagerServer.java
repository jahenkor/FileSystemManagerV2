package edu.gmu.cs475;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

public class FileManagerServer extends AbstractFileManagerServer {
	HashMap<String, String> serverFiles = new HashMap<>();// list of tagged files
	HashMap<String, IFileReplica> clientCache = new HashMap<String, IFileReplica>();// cache of clients
	HashMap<Long, String> lockMap = new HashMap<>();//holds file and lock values
        private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock readLock = readWriteLock.readLock();
	private final Lock writeLock = readWriteLock.writeLock();
	private final StampedLock lock = new StampedLock();
        private int transactionID; 
        
         void incTransaction(){
            
            synchronized (this){
            transactionID++;
            }
        }
	/**
	 * Initializes the server with the state of the files it cares about
	 *
	 * @param files list of the files
	 */
	public void init(List<Path> files) {

                transactionID = 0;
		String content = null;

		for (Path file : files) {//iterate thru files
			try {
				content = readFileLocally(file.toString());
				serverFiles.put(file.toString(), content);
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("file failed to read");
			}
		}
	}

	/**
	 * Registers a replica with the server, returning all of the files that currently exist.
	 *
	 * @param hostname   the hostname of the replica talking to you (passed again at disconnect)
	 * @param portNumber the port number of the replica talking to you (passed again at disconnect)
	 * @param replica    The RMI object to use to signal to the replica
	 * @return A HashMap of all of the files that currently exist, mapping from filepath/name to its contents
	 * @throws IOException in case of an underlying IOException when reading the files
	 */
	@Override
	public HashMap<String, String> registerReplica(String hostname, int portNumber, IFileReplica replica) throws IOException {
	
            writeLock.lock();
            try{
		String key = hostname.concat(String.valueOf(portNumber));
		clientCache.put(key, replica);
		return serverFiles;
                }
                finally{
                    writeLock.unlock();
                }
	}

	/**
	 * Write (or overwrite) a file.
	 * You must not allow a client to register or depart during a write.
	 *
	 * @param file    Path to file to write out
	 * @param content String representing the content desired
	 * @throws IOException if any IOException occurs in the underlying write, OR if the write is not succesfully replicated
	 */
	@Override
	public void writeFile(String file, String content) throws RemoteException, IOException {

		Collection<IFileReplica> replicaList = clientCache.values();
		Boolean writeStatus = true;
		long stamp = lockFile(file, false);
		long transID = startNewTransaction();
		System.out.println("transid is "+transID);
                readLock.lock();
		try {
			
            synchronized (file) {
                for (IFileReplica replica : replicaList) {
                    writeStatus = replica.innerWriteFile(file, content, transID);
                    if (!writeStatus) {
                        System.out.println("writefailed");
                       // issueAbortTransaction(transID);
                        throw new IOException("error writing file " + file);
                   }
                }
                issueCommitTransaction(transID);
            }
		}catch (IOException e){
                    System.out.println("Error writing " + file );
		    
                issueAbortTransaction(transID);
            
            throw new IOException("error writing file " + file);
        }finally {
                    
			unLockFile(file, stamp, false);
                    
                        
			readLock.unlock();

		}
                
                
                writeLock.lock();
                try{
                    
                serverFiles.put(file, content);
                }
                finally{
                    writeLock.unlock();
                }
		
	}

	/**
	 * Write (or overwrite) a file. Broadcasts the write to all replicas and locally on the server
	 * You must not allow a client to register or depart during a write.
	 *
	 * @param file    Path to file to write out
	 * @param content String representing the content desired
	 * @param xid     Transaction ID to use for any replications of this write
	 * @return True if all replicas replied "OK" to this write
	 * @throws IOException if any IOException occurs in the underlying write, OR if the write is not succesfully replicated
	 */
	@Override
	public boolean writeFileInTransaction(String file, String content, long xid) throws RemoteException, IOException {
		Collection<IFileReplica> replicaList = clientCache.values();
		Boolean writeStatus = true;
		//long stamp = lockFile(file, false);

               readLock.lock();
		try {
			//readLock.lock();
			for (IFileReplica replica : replicaList) {
						writeStatus = replica.innerWriteFile(file, content, xid);
				if(!writeStatus){
					issueAbortTransaction(xid);
					throw new IOException("error writing file "+ file);
				}
			}
                        
                       // issueCommitTransaction(xid);
                         serverFiles.put(file, content);
                        
		}//catch (IOException e){
                   // System.out.println("writeFileInTransaction error "+ file);
                   // System.out.println(e);
		   // if(writeStatus) {
               // issueAbortTransaction(xid);
            //}
            //throw new IOException("error writing file " + file);
//        }
finally{
			//unLockFile(file, stamp,false);
			
			readLock.unlock();
		}
                
                /*
                writeLock.lock();
                try{
                    
               
                }
                finally{
                    writeLock.unlock();
                }*/
                return true;
	}

	/**
	 * Acquires a read or write lock for a given file.
	 *
	 * @param name     File to lock
	 * @param forWrite True if a write lock is requested, else false
	 * @return A stamp representing the lock owner (e.g. from a StampedLock)
	 * @throws NoSuchFileException If the file doesn't exist
	 */
	@Override
	public long lockFile(String name, boolean forWrite) throws RemoteException, NoSuchFileException {
		if(!fileExists(name)){
			throw new NoSuchFileException(name);
		}

		long stamp = 0;
		if (forWrite) {
			System.out.println("write lock granted for " + name);
			stamp = lock.writeLock();
		} else {
			readLock.lock();
			stamp = lock.readLock();
		}
		return stamp;
	}

	/**
	 * Releases a read or write lock for a given file.
	 *
	 * @param name     File to lock
	 * @param stamp    the Stamp representing the lock owner (returned from lockFile)
	 * @param forWrite True if a write lock is requested, else false
	 * @throws NoSuchFileException          If the file doesn't exist
	 * @throws IllegalMonitorStateException if the stamp specified is not (or is no longer) valid
	 */
	@Override
	public void unLockFile(String name, long stamp, boolean forWrite) throws RemoteException, NoSuchFileException, IllegalMonitorStateException {

            
		if(!fileExists(name)){
			throw new NoSuchFileException(name);
		}

                
		if (forWrite) {
			System.out.println("write lock released for " + name);
			lock.unlockWrite(stamp);
		}
		else {
			lock.unlockRead(stamp);
			readLock.unlock();
		}
	}

	/**
	 * Notifies the server that a cache client is shutting down (and hence no longer will be involved in writes)
	 *
	 * @param hostname   The hostname of the client that is disconnecting (same hostname specified when it registered)
	 * @param portNumber The port number of the client that is disconnecting (same port number specified when it registered)
	 * @throws RemoteException
	 */
	@Override
	public void cacheDisconnect(String hostname, int portNumber) throws RemoteException {

		writeLock.lock();
		try {
			String key = hostname.concat(String.valueOf(portNumber));
			System.out.println(key + " disconnected");
			clientCache.remove(key);
		}finally {
			writeLock.unlock();
		}
	}

	/**
	 * Request a new transaction ID to represent a new, client-managed transaction
	 *
	 * @return Transaction organizer-provided ID that will be used in the future to commit or abort this transaction
	 */
	@Override
	public long startNewTransaction() throws RemoteException {
            this.incTransaction();
		return this.transactionID;
	}

	/**
	 * Broadcast to all replicas (and make updates locally as necessary on the server) that a transaction should be committed
	 * You must not allow a client to register or depart during a commit.
	 *
	 * @param xid transaction ID to be committed (from startNewTransaction)
	 * @throws IOException in case of any underlying IOException upon commit
	 */
	@Override
	public void issueCommitTransaction(long xid) throws RemoteException, IOException {

            readLock.lock();
            try{
            for(IFileReplica filerep : clientCache.values()){
                
                filerep.commitTransaction(xid);
            }

                
            }
            
            finally{
                readLock.unlock();
            }
                
            
	}

	/**
	 * Broadcast to all replicas (and make updates locally as necessary on the server) that a transaction should be aborted
	 * You must not allow a client to register or depart during an abort.
	 *
	 * @param xid transaction ID to be committed (from startNewTransaction)
	 */
	@Override
	public void issueAbortTransaction(long xid) throws RemoteException {

            readLock.lock();
            try{
            for(IFileReplica filerep : clientCache.values()){
                filerep.abortTransaction(xid);
            }}
            
            finally{
            readLock.unlock();
            }
	}
	public boolean fileExists(String file) {

            readLock.lock();
            try{
         //   long stamp = lock.tryOptimisticRead();
		Set<String> fileList = serverFiles.keySet();
		for (String fileObj : fileList) { //Find file from list of tagged files
			if (fileObj.equals(file)) {
				return true;
			}
		}
            }
            
            finally{
                readLock.unlock();
            }
             /*   if(!lock.validate(stamp)){
                    stamp = lock.readLock();
                    try{
                        fileList = serverFiles.keySet();
		for (String fileObj : fileList) { //Find file from list of tagged files
			if (fileObj.equals(file)) {
				return true;
			}
		}
                    } finally{
                        lock.unlockRead(stamp);
                    }
                }*/

		return false;

	}
}
