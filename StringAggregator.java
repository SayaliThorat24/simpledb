package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.Aggregator.Op;

/**
 * A {@code StringAggregator} computes some aggregate value over a set of {@code StringField}s.
 */
public class StringAggregator implements Aggregator {

	private int gbfield1;
	List<Tuple> list;
	private int afield1;
	private TupleDesc tdesc;
	private Type gbfieldtype;
	private Type gbfieldtype1;
	private Op what1;
	private boolean noGrouping=false;
	
	private String  fldname, groupfld;
	private String fieldName,groupFieldName;
	private Map<Field, Integer> HMap = new HashMap<Field, Integer>();
	private Map<Field,Integer> grpHMap = new HashMap<Field,Integer>();
	
	private Map<Field,Integer> countgrpHMap = new HashMap<Field,Integer>();
	
	
	/**
	 * Constructs a {@code StringAggregator}.
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or {@code NO_GROUPING} if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., {@code Type.INT_TYPE}), or {@code null} if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            aggregation operator to use -- only supports {@code COUNT}
	 * @throws IllegalArgumentException
	 *             if {@code what != COUNT}
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		if(gbfield == NO_GROUPING){
			noGrouping = true;
		}

		gbfield1 = gbfield;
		gbfieldtype1 = gbfieldtype;
		afield1 = afield;
		what1 = what;
		//System.out.println("afield");
		//System.out.println(afield);
		grpHMap = new HashMap<Field, Integer>();
		
		if(gbfieldtype == null){
			Type[] type = { Type.INT_TYPE};
			this.tdesc = new TupleDesc(type);
		}else{
			Type[] type = { gbfieldtype1 , Type.INT_TYPE};
			this.tdesc = new TupleDesc(type);
		}
		
	}

	/**
	 * Merges a new tuple into the aggregate, grouping as indicated in the constructor.
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup) {
		
		fldname = tup.getTupleDesc().getFieldName(this.afield1);
		groupfld= tup.getTupleDesc().getFieldName(this.gbfield1);
		
		
		// some code goes here
		Tuple tup1 = new Tuple(tdesc);
		Tuple tupNew = new Tuple(tdesc);
		
		
		if(gbfieldtype1.equals(null)){  
			tupNew.setField(0, new IntField(1));
		}else{
			tupNew.setField(0, tup.getField(gbfield1));
			tupNew.setField(1, new IntField(1));
		}
		
			
		int Val;
		Field fld = tupNew.getField(0);
	
			
			if(grpHMap.containsKey(fld)){
				Val = grpHMap.get(fld);
				Val++;

				grpHMap.put(fld,Val);
			}else{
				grpHMap.put(fld, 1);
			}
			
			
		}		


	

	/**
	 * Creates a {@code DbIterator} over group aggregate results.
	 *
	 * @return a {@code DbIterator} whose tuples are the pair ({@code groupVal}, {@code aggregateVal}) if using group,
	 *         or a single ({@code aggregateVal}) if no grouping. The aggregateVal is determined by the type of
	 *         aggregate specified in the constructor.
	 
	public DbIterator iterator() {
		// some code goes here
		ArrayList<Tuple> tup = new ArrayList<Tuple>();
		TupleDesc tDesc = this.getTupleDesc;
		
		
		throw new UnsupportedOperationException("implement me");
	}
	*/
	
	public DbIterator iterator() throws UnsupportedOperationException {
		ArrayList<Tuple> itr = new ArrayList<Tuple>();
		TupleDesc tDesc;
		Type[] typearray;
		String[] stringarray;
		//String fName = tDesc.getFieldName(afield);
		
		
		
		if(gbfield1 == NO_GROUPING){
			typearray = new Type[1];
			typearray[0] = Type.INT_TYPE;
			stringarray = new String[1];
			stringarray[0] = fieldName;
			tDesc = new TupleDesc(typearray, stringarray);
			
			for (Field fld : grpHMap.keySet()){
				int grpno = grpHMap.get(fld);
				Tuple tup = new Tuple(tDesc);
				tup.setField(0, new IntField(grpno));
				itr.add(tup);
			}
		}else{		
			
			typearray = new Type[2];
			typearray[0] = gbfieldtype1;
			typearray[1] = Type.INT_TYPE;
			stringarray = new String[2];
			stringarray[0] = groupfld;
			stringarray[1] = fldname;
			
			System.out.println(typearray[0]);
			
			tDesc = new TupleDesc(typearray, stringarray);
			

			for(String name:tdesc.names ){
				System.out.println(name);
			}
			for(Type name:tDesc.types ){
				System.out.println(name);
			}
			for (Field fld : grpHMap.keySet()){
				int grpno = grpHMap.get(fld);
				Tuple tup = new Tuple(tDesc);
				tup.setField(0, fld);
				tup.setField(1, new IntField(grpno));
				itr.add(tup);
			}
		}
		System.out.println("tdesc.names");
		for(String name:tdesc.names ){
			System.out.println(name);
		}
		for(Type name:tdesc.types ){
			System.out.println(name);
		}
		
		
		return new TupleIterator(tDesc, itr);
		//throw new UnsupportedOperationException("implement me");
	}
	
}
