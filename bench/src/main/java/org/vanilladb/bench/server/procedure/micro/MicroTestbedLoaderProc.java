/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.bench.server.procedure.micro;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.server.param.micro.TestbedLoaderParamHelper;
import org.vanilladb.bench.server.procedure.BasicStoredProcedure;
//import org.vanilladb.bench.server.procedure.micro.MicroTxnProc.Bucket;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.bench.server.param.micro.Workload;

public class MicroTestbedLoaderProc extends BasicStoredProcedure<TestbedLoaderParamHelper> {
	private static Logger logger = Logger.getLogger(MicroTestbedLoaderProc.class.getName());
	private final static double GAMMA = 0.8;
	private final static double DELTA = 0.3;
	private final static int item_record_size = 100000;
	private final static int sample_size = 50000;
	int workload_size;
	int region_size;
	int [] queryCount;
	Workload wl;
	Map<String,Region> Regions;
	public MicroTestbedLoaderProc() {
		super(new TestbedLoaderParamHelper());
		wl = new Workload();
		workload_size = wl.WorkLoadSize;
		queryCount = new int[workload_size];
		Regions = new HashMap<>();
	}
	
	public class Region{
		private Vector<Integer> recordIds;
		public double alphaJ;
		public int r_id;
		public double[] alphaJQ;
		private int ki;
		//private Strisng record_tag;
		public Region(int r_id) {
			this.r_id = r_id;
			this.recordIds  = new Vector();
			this.alphaJQ = new double[workload_size];
			for(int i=0;i<workload_size;++i)
				this.alphaJQ[i] = 0;
		}
		public int getRId() {return this.r_id;}
		public void addRecord(Integer i_id) {
			recordIds.add(i_id);
		}
		public int getNi() {
			return recordIds.size();
		}
		public void setAlphaJQ(int index,double value) {
			this.alphaJQ[index] = value;
		}
		public void setAlpha(double value) {
			this.alphaJ = value;
		}
		public void setKi(int ki) {
			this.ki = ki;
		}
		public int getKi() {
			return this.ki;
		}
	}

	@Override
	protected void executeSql() {
		
		int []counts;
		if (logger.isLoggable(Level.INFO))
			logger.info("Start loading testbed...");

		// turn off logging set value to speed up loading process
		// TODO: remove this hack code in the future
		RecoveryMgr.enableLogging(false);
		
		dropOldData();
		createSchemas();

		// Generate item records
		generateItems(1, paramHelper.getNumberOfItems());

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading completed. Flush all loading data to disks...");

		// TODO: remove this hack code in the future
		RecoveryMgr.enableLogging(true);

		// Create a checkpoint
		CheckpointTask cpt = new CheckpointTask();
		cpt.createCheckpoint();

		// Delete the log file and create a new one
		VanillaDb.logMgr().removeAndCreateNewLog();
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Loading procedure finished.");
		//TODO:tag_record
		logger.info("Start tag record.");
		queryCount = tag_record();
		logger.info("Start sampling.");
		//TODO:sample
		sample();

	}
	private int[] tag_record() {
		int[] counts = new int[workload_size];
		for(int i=0;i<workload_size;++i) {
			String sql = wl.workload[i];
			counts[i] = tag_record(sql,i);
		}
		return counts;
	}
	private void sample() {
		Scan recordTagScan = sample("SELECT RECORD_TAG, i_id FROM item");
		recordTagScan.beforeFirst();
		int r_id = 1;
		while(recordTagScan.next()) {
			String record_tag = (String) recordTagScan.getVal("record_tag").asJavaVal();
			Integer i_id = (Integer) recordTagScan.getVal("i_id").asJavaVal();
			if(Regions.containsKey(record_tag)) {
				Regions.get(record_tag).addRecord(i_id);
			}else {
				Region temp = new Region(r_id);
				temp.addRecord(i_id);
				Regions.put(record_tag,temp);
				r_id +=1;
			}
		}
		recordTagScan.close();
		
		region_size = Regions.size();
		System.out.println("[MicroTestbedLoad]"+region_size);
		//TODO:calculate the alpha(J,Q)
		calAlphaJQ();
		//TODO:calculate alphaJ
		calAlphaJ();
		//TODO:calculate ki
		calKI();
		//TODO:sample ki records and store in table
		logger.info("Start creating sample table.");
		createSampleTbl();
		logger.info("Finish creating sample table.");
		//TODO:create region table
		createRegionTbl();
		logger.info("Finish creating region table");
		
	}
	private void createRegionTbl() {
		executeUpdate("CREATE TABLE region (r_id INT, ni INT, ki INT)");
		for(Object k: Regions.keySet()) {
			String key = (String)k;
			int r_id = Regions.get(key).r_id;
			int ni = Regions.get(key).getNi();
			int ki = Regions.get(key).getKi();
			executeUpdate("INSERT INTO region(r_id, ni, ki) VALUES (" +r_id + ", " + ni + ","
					+ ki + ")");
		}
	}
	private void createSampleTbl() {
		executeUpdate("CREATE TABLE sample_item (i_id INT, i_im_id INT, i_name VARCHAR(24), "
				+ "i_price DOUBLE, i_data VARCHAR(50),r_id INT)");
		for(Object k:Regions.keySet()) {
			String key = (String)k;
			int ki = Regions.get(key).getKi();
			String sql = "SELECT i_id,i_im_id,i_name,i_price,i_data FROM item WHERE record_tag = '"+key+"'";
			Scan s = executeQuery(sql);
			s.beforeFirst();
			int count =0;
			while(s.next()) {
				if(count>=ki)
					break;
				int iid = (int)s.getVal("i_id").asJavaVal();
				int iimid = (int)s.getVal("i_im_id").asJavaVal();
				String iname = (String)s.getVal("i_name").asJavaVal();
				double iprice =(double)s.getVal("i_price").asJavaVal();
				String idata = (String)s.getVal("i_data").asJavaVal();
				executeUpdate("INSERT INTO sample_item(i_id, i_im_id, i_name, i_price, i_data,r_id) VALUES (" + iid + ", " + iimid + ", '"
						+ iname + "', " + iprice + ", '" + idata + "', "+Regions.get(key).r_id+")");
				count++;
			}
			s.close();
		}
	}
	
	private void calKI() {
		double rootAlpha = 0.0;
		for(Object k : Regions.keySet()) {
			String key = (String)k;
			rootAlpha += Math.pow(Regions.get(key).alphaJ,0.5);
		}
		for(Object k : Regions.keySet()) {
			String key = (String)k;
			double rootAlphaJ = Math.pow(Regions.get(key).alphaJ,0.5);
			int ki = (int) (sample_size*(rootAlphaJ/rootAlpha));
			Regions.get(key).setKi(ki);
			System.out.println("[calKI]"+ki);
		}
	}
	private void calAlphaJ() {
		for(Object k:Regions.keySet()) {
			double alphaJ = 0.0f;
			String key = (String)k;
			double[] alphaJQ = Regions.get(key).alphaJQ;
			for(int i =0;i<workload_size;++i) {
				alphaJ += (double)((double)queryCount[i]/(double)item_record_size)*alphaJQ[i];
			}
			Regions.get(key).setAlpha(alphaJ);
			//System.out.println("[calAlphaJ] alphaJ: "+alphaJ);
		}
	}
	private void calAlphaJQ() {
		for(int i=0;i<workload_size;++i) {
			double ru_sum =0.0;
			double lu_sum =0.0;
			double rd_sum = 0.0;
			double ld_sum = 0.0;
			for(Object k:Regions.keySet()) {
				String key = (String)k;
				int ni = Regions.get(key).getNi();
				if(key.charAt(i) == '1') {
					ru_sum += Math.pow(ni,2)*DELTA*(1-DELTA);
					rd_sum += ni*DELTA;
				}else if(key.charAt(i) == '0'){
					lu_sum += Math.pow(ni,2)*GAMMA*(1-GAMMA);
					ld_sum += ni*GAMMA;
				}
			}
			double alphaJQ =(ru_sum+lu_sum)/(Math.pow((rd_sum+ld_sum),2));
			//System.out.println("[calAlphaJQ] alphaJQ: "+alphaJQ);
			for(Object k:Regions.keySet()) {
				String key = (String)k;
				if(key.charAt(i) == '1') {
					Regions.get(key).setAlphaJQ(i,alphaJQ);
				}
			}
		}
	}
	private void dropOldData() {
		// TODO: Implement this
		if (logger.isLoggable(Level.WARNING))
			logger.warning("Dropping is skipped.");
	}
	
	private void createSchemas() {
		if (logger.isLoggable(Level.FINE))
			logger.info("Create tables...");
		
		for (String cmd : paramHelper.getTableSchemas())
			executeUpdate(cmd);
		
		if (logger.isLoggable(Level.FINE))
			logger.info("Create indexes...");

		for (String cmd : paramHelper.getIndexSchemas())
			executeUpdate(cmd);
		
		if (logger.isLoggable(Level.FINE))
			logger.info("Finish creating schemas.");
	}

	private void generateItems(int startIId, int endIId) {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populating items from i_id=" + startIId + " to i_id=" + endIId);

		int iid, iimid;
		String iname, idata;
		double iprice;
		String sql;
		Workload wl = new Workload();
		for (int i = startIId; i <= endIId; i++) {
			iid = i;

			// Deterministic value generation by item id
			iimid = iid % (TpccConstants.MAX_IM - TpccConstants.MIN_IM) + TpccConstants.MIN_IM;
			iname = String.format("%0" + TpccConstants.MIN_I_NAME + "d", iid);
			iprice = (iid % (int) (TpccConstants.MAX_PRICE - TpccConstants.MIN_PRICE)) + TpccConstants.MIN_PRICE;
			idata = String.format("%0" + TpccConstants.MIN_I_DATA + "d", iid);
			sql = "INSERT INTO item(i_id, i_im_id, i_name, i_price, i_data,RECORD_TAG) VALUES (" + iid + ", " + iimid + ", '"
					+ iname + "', " + iprice + ", '" + idata + "','"+ wl.init_str+"')";
			executeUpdate(sql);
		}

		if (logger.isLoggable(Level.FINE))
			logger.info("Populating items completed.");
	}
}
