package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * An {@code Aggregate} operator computes an aggregate value (e.g., sum, avg, max, min) over a single column, grouped by a
 * single column.
 */
public class Aggregate extends AbstractDbIterator {

	DbIterator child;
	DbIterator agg1;
	Aggregator agg;
	int afield;
	Op aop;
	int gfield;
	
	
	
	/**
	 * Constructs an {@code Aggregate}.
	 *
	 * Implementation hint: depending on the type of afield, you will want to construct an {@code IntAggregator} or
	 * {@code StringAggregator} to help you with your implementation of {@code readNext()}.
	 * 
	 *
	 * @param child
	 *            the {@code DbIterator} that provides {@code Tuple}s.
	 * @param afield
	 *            the column over which we are computing an aggregate.
	 * @param gfield
	 *            the column over which we are grouping the result, or -1 if there is no grouping
	 * @param aop
	 *            the {@code Aggregator} operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		
		Type gbfield;
		this.child = child;
		this.gfield = gfield;
		
		if(gfield == Aggregator.NO_GROUPING){
			gbfield = null;
		}else{
			gbfield = child.getTupleDesc().getType(gfield);
		}
		
		Type getType = child.getTupleDesc().getType(afield);
		
		
		if(getType == Type.INT_TYPE){
			this.agg = new IntAggregator(gfield , gbfield, afield, aop);
		}else{
			this.agg = new StringAggregator(gfield , gbfield, afield, aop);
		}
		

	}

	public static String aggName(Aggregator.Op aop) {
		switch (aop) {
		case MIN:
			return "min";
		case MAX:
			return "max";
		case AVG:
			return "avg";
		case SUM:
			return "sum";
		case COUNT:
			return "count";
		}
		return "";
	}

	public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
		// some code goes here
		child.open();
		
		while(child.hasNext()){
			agg.merge(child.next());
		}
		
		agg1 = agg.iterator();
		agg1.open();
		
		//super.open();
		
	}

	/**
	 * Returns the next {@code Tuple}. If there is a group by field, then the first field is the field by which we are
	 * grouping, and the second field is the result of computing the aggregate, If there is no group by field, then the
	 * result tuple should contain one field representing the result of the aggregate. Should return {@code null} if
	 * there are no more {@code Tuple}s.
	 */
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		// some code goes here
		try{
		if(agg1.hasNext()){
			return agg1.next();
		}else{
			return null;
		}
		}catch(Exception exp){
			System.out.println("");
		}
		return null;
	}

	protected Tuple getNext() throws TransactionAbortedException, DbException {
		// some code goes here
		try{
		if(this.child.hasNext()){
			return this.child.next();
		}else{
			return null;
		}
		}catch(Exception exp){
			System.out.println("");
		}
		return null;
	}
	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		child.rewind();
		agg1.rewind();
	}

	/**
	 * Returns the {@code TupleDesc} of this {@code Aggregate}. If there is no group by field, this will have one field
	 * - the aggregate column. If there is a group by field, the first field will be the group by field, and the second
	 * will be the aggregate value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * {@code aggName(aop) (child_td.getFieldName(afield))} where {@code aop} and {@code afield} are given in the
	 * constructor, and {@code child_td} is the {@code TupleDesc} of the child iterator.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		
		return agg.iterator().getTupleDesc();
		
	}

	public void close() {
		// some code goes here
		this.child.close();
	}
}
