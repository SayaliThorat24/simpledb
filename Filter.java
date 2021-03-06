package simpledb;  

import static org.junit.Assert.assertTrue;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends AbstractDbIterator {
	
	/**
	 * The child operator.
	 */
	DbIterator child;
	
	/**
	 * The predicate.
	 */
	Predicate p;

    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
    	this.child = child;	
    	this.p = p;
    }

    public TupleDesc getTupleDesc() {
    	return child.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
    	child.open();
    }

    public void close() {
    	//super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	child.rewind();   
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     * @author Sayali Thorat
     */
    protected Tuple readNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
    	Tuple tuple;
        while(child.hasNext()){
        	tuple = child.next();
        	if(tuple!=null && p.filter(tuple))
        		return tuple;
        }
        return null;
    }
}
