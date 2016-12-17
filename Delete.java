/**
 * Author: Ahmed Quadri Syed, Ehtesham
 */
package simpledb;  

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

	private DbIterator child;
	private TransactionId t;
	private TupleDesc tdesc;
	private boolean visited = false;
	
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	
    	Type[] typearray;
		String[] stringarray;
    	this.child=child;
    	this.t=t;
    	typearray = new Type[1];
		typearray[0] = Type.INT_TYPE;	
		stringarray = new String[1];
		stringarray[0] = "tuples";
		
		this.tdesc = new TupleDesc(typearray, stringarray);
        // some code goes here
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tdesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	
    }

    public void close() {
        // some code goes here
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	Tuple tup = new Tuple(tdesc);
    	try{
    		
			if(visited == true){
    			return null;
    		}else{
    			
    		visited = true;
    		int i = 0;
    		while(child.hasNext()){
    			Tuple cld = child.next();
    			Database.getBufferPool().deleteTuple(t, cld);
    			i++;
    			
    		}
    		
    		Field fld = new IntField(i);
    		tup.setField(0, fld);
    		
    		}	
    	}catch(Exception exp){
    		exp.printStackTrace();
    	}
        return tup;
    }
}
