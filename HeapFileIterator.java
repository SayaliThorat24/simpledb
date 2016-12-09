package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
	
	int id;
	int numPages;
	TransactionId tid;
	boolean isOpen;
	int currentPage;
	Iterator<Tuple> tupleItr;
	Tuple t;

	public HeapFileIterator(int id, int numPages, TransactionId tid) {
		this.id = id;
        this.numPages = numPages;
        this.tid = tid;
        isOpen = false;
	}

	private Iterator<Tuple> pageIterator(int pageNumber) throws DbException,
      TransactionAbortedException {
			HeapPageId heapPgId = new HeapPageId(id, pageNumber);
			HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPgId, Permissions.READ_WRITE);
			return page.iterator();
  }
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		isOpen = true;
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if(!isOpen)
			return false;
		else{	
			return(currentPage < numPages);
		}
	}
	
	public Tuple getNextTuple() throws DbException, TransactionAbortedException{
		while(hasNext()){
			if(tupleItr.hasNext()){
				return tupleItr.next();
			}
			currentPage++;
			tupleItr = pageIterator(currentPage);
		}
		return null;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		if(!isOpen)
			throw  new NoSuchElementException();
		Tuple temp = t;
		if(hasNext() && t!=null){
			t = getNextTuple();
				
		}
		return temp;
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		currentPage = 0;
		tupleItr = pageIterator(currentPage);
	}

	@Override
	public void close() {
		isOpen = false;
		
	}

	
}
