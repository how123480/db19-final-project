package org.vanilladb.core.query.planner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.vanilladb.core.query.algebra.ExplainPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProductPlan;
import org.vanilladb.core.query.algebra.ProjectPlan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.SelectScan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.aggfn.*;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;

public class BasicSamplePlanner implements QueryPlanner{

	/**
	 * Creates a query plan as follows. It first takes the product of all tables
	 * and views; it then selects on the predicate; and finally it projects on
	 * the field list.
	 */
	
	public static String booleanArray2Str(Boolean[] b) {
		String s = "";
		int b_size = b.length; 
		for(int i=0;i<b_size;++i) {
			int b_int = b[i] ? 1 : 0;
			s+=String.valueOf(b_int);
		}
		return s;
	}
	public static Boolean[] str2BooleanArray(String str) {
		int str_size = str.length();
		Boolean[] b = new Boolean[str_size];
		
		for(int i=0;i<str_size;++i ) {
			if(str.charAt(i) == '0')
				b[i] = false;
			else if(str.charAt(i) == '1')
				b[i] = true;
		}
		return b;
	}
	
	public int tag_record(QueryData data, Transaction tx,int i) {
		//i mean sql is i-th element in workload Set
		List<Plan> plans = new ArrayList<Plan>();

		boolean hasCf = false;
		boolean hasSf = false;
		Set<String> proField = new HashSet<String>();
		proField.add("RECORD_TAG");
		data.projFields = proField;
		for(AggregationFn af:data.aggregationFn()) {
			if(af.getClass().equals(CountFn.class))
				hasCf = true;
			if(af.getClass().equals(SumFn.class))
				hasSf = true;
		}
		
		for (String tblname : data.tables()) {
			String viewdef = VanillaDb.catalogMgr().getViewDef(tblname, tx);
			if (viewdef != null)
				plans.add(VanillaDb.newPlanner().createQueryPlan(viewdef, tx));
			else
				plans.add(new TablePlan(tblname, tx));
		}
		// Step 2: Create the product of all table plans
		Plan p = plans.remove(0);
		for (Plan nextplan : plans)
			p = new ProductPlan(p, nextplan);
		// Step 3: Add a selection plan for the predicate
		p = new SelectPlan(p, data.pred());
		
		UpdateScan us = (UpdateScan) p.open();
		//p = new ProjectPlan(p, data.projectFields());
		//Scan ps = (Scan)p.open();
		// Step 4: Add a group-by plan if specified
		//SelectScan us = (SelectScan) p.open();
		us.beforeFirst();
		int count = 0;
		while (us.next()) {
			if(hasCf || hasSf) {
				String record_tag = (String)us.getVal("record_tag").asJavaVal();
				Boolean [] b_map = str2BooleanArray(record_tag);
				b_map[i] = true;
				record_tag = booleanArray2Str(b_map);
				Constant c = Constant.newInstance(Type.VARCHAR(200),record_tag.getBytes());
				us.setVal("record_tag",c);
				//TODO:set bit map to 1
			}
			count++;
		}
		//ps.close();
		us.close();
		//VanillaDb.statMgr().countRecordUpdates(data.tableName(), count);
		return count;
	}
	private double getResult(Plan p,QueryData data) {
		Scan s = p.open();
		double value = -0.0;
		s.beforeFirst();
		Set<String> groupField = data.groupFields();
		while(s.next()) {
			for (String field:groupField) {
				value = (Double) s.getVal(field).asJavaVal();
			}
		}
		s.close();
		return value;
		
	}
	
	public double[] sampleQuery(QueryData data, Transaction tx){
		/*
		 * index:
		 * 	0: sample aggn func value
		 *  1: time (ms)
		 */
		double start_time;
		double end_time;
		double [] result = new double[2];
		start_time = (double)TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
		result[0] = getResult(createPlan(data,tx),data);
		end_time = (double)TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
		result[1] = end_time - start_time;
		return result;
	}
	
	@Override
	public Plan createPlan(QueryData data, Transaction tx) {
		// Step 1: Create a plan for each mentioned table or view
				List<Plan> plans = new ArrayList<Plan>();
				for (String tblname : data.tables()) {
					String viewdef = VanillaDb.catalogMgr().getViewDef(tblname, tx);
					if (viewdef != null)
						plans.add(VanillaDb.newPlanner().createQueryPlan(viewdef, tx));
					else {
						if(data.isSample())
							plans.add(new TablePlan("sample "+tblname,tx));
						else 
							plans.add(new TablePlan(tblname, tx));
					}
				}
				// Step 2: Create the product of all table plans
				Plan p = plans.remove(0);
				for (Plan nextplan : plans)
					p = new ProductPlan(p, nextplan);
				// Step 3: Add a selection plan for the predicate
				p = new SelectPlan(p, data.pred());
				// Step 4: Add a group-by plan if specified
				if (data.groupFields() != null) {
					p = new GroupByPlan(p, data.groupFields(), data.aggregationFn(), tx);
				}
				// Step 5: Project onto the specified fields
				p = new ProjectPlan(p, data.projectFields());
				// Step 6: Add a sort plan if specified
				if (data.sortFields() != null)
					p = new SortPlan(p, data.sortFields(), data.sortDirections(), tx);
				// Step 7: Add a explain plan if the query is explain statement
				if (data.isExplain())
					p = new ExplainPlan(p);
				return p;
	}

}
