package simpledb;

import java.io.*;
import java.sql.Savepoint;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	/**
	 * The File associated with this HeapFile.
	 */
	protected File file;

	/**
	 * The TupleDesc associated with this HeapFile.
	 */
	protected TupleDesc td;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	public HeapFile(File f, TupleDesc td) {
		this.file = f;
		this.td = td;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return file;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		return file.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}

	RandomAccessFile rfile;

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		int offset = BufferPool.PAGE_SIZE * pid.pageno();
		byte[] data = new byte[BufferPool.PAGE_SIZE];
		HeapPage page = null;
		try {
			rfile = new RandomAccessFile(getFile(), "rw");
			rfile.seek(offset);
			rfile.read(data);
			HeapPageId heapPageId = new HeapPageId(pid.getTableId(),
					pid.pageno());
			page = new HeapPage(heapPageId, data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return page;

	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for assignment1
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		 int numP =  (int)file.length() / BufferPool.PAGE_SIZE;
		if(file.length() % BufferPool.PAGE_SIZE > 0){
			numP++;
		}
		return numP;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for assignment1
		return null;
	}

	// see DbFile.java for javadocs
	/*
	 * @author Sayali Thorat 
	 */
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException {
		PageId pid = t.getRecordId().getPageId();
		Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
		((HeapPage)page).deleteTuple(t);
		return page;
	}

	// see DbFile.java for javadocs
	/*
	 * public DbFileIterator iterator(TransactionId tid) { return new
	 * HeapFileIterator(getId(), numPages(), tid); //throw new
	 * UnsupportedOperationException("Implement this"); }
	 */

	public DbFileIterator iterator(final TransactionId tid) {
		// some code goes here
		return new DbFileIterator() {

			Iterator<Tuple> i = null;

			@Override
			public void open() throws DbException, TransactionAbortedException {
				LinkedList<Tuple> l = new LinkedList<Tuple>();
				for (int p = 0; p < numPages(); p++) {
					try {
						Iterator<Tuple> i = ((HeapPage) Database
								.getBufferPool().getPage(tid, new HeapPageId(getId(), p), Permissions.READ_WRITE)).iterator();
						while (i.hasNext())
							l.add(i.next());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				i = l.iterator();
			}

			@Override
			public boolean hasNext() throws DbException,
					TransactionAbortedException {
				return i != null && i.hasNext();
			}

			@Override
			public Tuple next() throws DbException,
					TransactionAbortedException, NoSuchElementException {
				return i.next();
			}

			@Override
			public void rewind() throws DbException,
					TransactionAbortedException {
				close();
				open();
			}

			@Override
			public void close() {
			}

		};
	}
}
