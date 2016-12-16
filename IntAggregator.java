package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * An {@code IntAggregator} computes some aggregate value over a set of {@code IntField}s.
 */
public class IntAggregator implements Aggregator {

	
	private int gbfield;
	
	private int afield;
	private String  fldname, groupfld;
	private Type gbfieldtype;
	
	private Op what;
	
	private boolean noGrouping;
	private String fieldName;
	
	private HashMap<Field,Integer> grpHMap = new HashMap<Field,Integer>();
	
	private HashMap<Field,Integer> countgrpHMap = new HashMap<Field,Integer>();
	/**
	 * Constructs an {@code Aggregate}.
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or {@code NO_GROUPING} if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., {@code Type.INT_TYPE}), or {@code null} if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            the aggregation operator
	 */

	public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		if(gbfield == NO_GROUPING){
			noGrouping = true;
		}
		
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		
		grpHMap = new HashMap<Field,Integer>();
		countgrpHMap = new HashMap<Field,Integer>();
		
		
		
	}

	/**
	 * Merges a new {@code Tuple} into the aggregate, grouping as indicated in the constructor.
	 * 
	 * @param tup
	 *            the {@code Tuple} containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup) {
		// some code goes here
		
		TupleDesc tDesc = tup.tupledesc;
		fldname = tup.getTupleDesc().getFieldName(this.afield);
		groupfld= tup.getTupleDesc().getFieldName(this.gbfield);
		
		String fName = tDesc.getFieldName(afield);
		Field gbval;
		int aggValue;
		int curValue = 0;
		Op opr = null;
		int count;
		
		
		
		Tuple tup1 = new Tuple(tDesc);
		Tuple tupNew = new Tuple(tDesc);
		
		
		if(gbfieldtype.equals(null)){  
			tupNew.setField(0, new IntField(1));
		}else{
			tupNew.setField(0, tup.getField(gbfield));
			tupNew.setField(1, new IntField(1));
		}
		int Val;
		Field fld = tupNew.getField(0);
		gbval = fld;
		//gbval = tup.getField(gbfield);
		/*
		if(NO_GROUPING == -1){
			gbval = new IntField(Aggregator.NO_GROUPING);
		}else{
			gbval = tup.getField(gbfield);
		}
		*/
		
		int afieldValue = ((IntField)tup.getField(afield)).getValue();
		
		
		if(grpHMap.containsKey(gbval) == false){
			if(what == Op.AVG){
				grpHMap.put(gbval, 0);
				countgrpHMap.put(gbval, 0);
			}else if(what == Op.COUNT){
				grpHMap.put(gbval, 0);
				countgrpHMap.put(gbval, 0);
			}else if(what == Op.SUM){
				grpHMap.put(gbval, 0);
				countgrpHMap.put(gbval, 0);
			}else if(what == Op.MAX){
				countgrpHMap.put(gbval, 0);
				grpHMap.put(gbval,-10000);
			}else if(what == Op.MIN){
				System.out.println(grpHMap.containsKey(gbval));
				countgrpHMap.put(gbval, 0);
				grpHMap.put(gbval,10000);
			}
			
		}else{
			curValue = grpHMap.get(gbval);
		}
		System.out.println("");
		System.out.println(gbval);
		System.out.println(curValue);
		
		curValue = grpHMap.get(gbval);
		int tupCount = countgrpHMap.get(gbval);
		if(what == Op.MIN){

			System.out.println(curValue+">"+afieldValue+"hey");
			if(afieldValue < curValue){
				curValue = afieldValue;
				grpHMap.put(gbval, curValue);
			}
			
		}else if(what == Op.MAX){
			if(curValue < afieldValue){
				//afieldValue = curValue;
				grpHMap.put(gbval, afieldValue);
				
			}
		}else if(what == Op.COUNT){
				curValue++;
				grpHMap.put(gbval, afieldValue);
				
		}else if(what == Op.SUM){
			afieldValue = afieldValue + curValue;
			grpHMap.put(gbval, afieldValue);
			
		}else if(what == Op.AVG){
			
			tupCount++;
			countgrpHMap.put(gbval, tupCount);
			
			afieldValue = afieldValue + curValue;
			grpHMap.put(gbval, afieldValue);
			
	}
			
	}
		

	/**
	 * Creates a {@code DbIterator} over group aggregate results.
	 *
	 * @return a {@code DbIterator} whose tuples are the pair ({@code groupVal}, {@code aggregateVal}) if using group,
	 *         or a single ({@code aggregateVal}) if no grouping. The {@code aggregateVal} is determined by the type of
	 *         aggregate specified in the constructor.
	 */
	public DbIterator iterator() throws UnsupportedOperationException {
		ArrayList<Tuple> itr = new ArrayList<Tuple>();
		TupleDesc tDesc;
		Type[] typearray;
		String[] stringarray;
		//String fName = tDesc.getFieldName(afield);
		if(gbfield == NO_GROUPING){
			typearray = new Type[1];
			typearray[0] = Type.INT_TYPE;
			stringarray = new String[1];
			stringarray[0] = fldname;
			tDesc = new TupleDesc(typearray, stringarray);
			
			for (Field fld : grpHMap.keySet()){
				int grpno = grpHMap.get(fld);
				if (what == Op.AVG) {
					grpno = grpno/countgrpHMap.get(fld);
				}
				Tuple tup = new Tuple(tDesc);
				tup.setField(0, new IntField(grpno));
				itr.add(tup);
			}
		}else{			
			typearray = new Type[2];
			typearray[0] = gbfieldtype;
			typearray[1] = Type.INT_TYPE;
			stringarray = new String[2];
			stringarray[0] = groupfld;
			stringarray[1] = fldname;
			
			tDesc = new TupleDesc(typearray, stringarray);
			
			for (Field fld : grpHMap.keySet()){
				int grpno = grpHMap.get(fld);
				if (what == Op.AVG) {
					grpno = grpno/countgrpHMap.get(fld);
				}
				Tuple tup = new Tuple(tDesc);
				tup.setField(0, fld);
				tup.setField(1, new IntField(grpno));
				itr.add(tup);
			}
		}
		
		return new TupleIterator(tDesc, itr);
		//throw new UnsupportedOperationException("implement me");
	}

}
