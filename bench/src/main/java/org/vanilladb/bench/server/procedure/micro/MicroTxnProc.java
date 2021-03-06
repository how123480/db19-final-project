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
import org.vanilladb.bench.server.param.micro.MicroTxnProcParamHelper;
import org.vanilladb.bench.server.param.micro.Workload;
import org.vanilladb.bench.server.procedure.BasicStoredProcedure;
import org.vanilladb.core.query.algebra.Scan;

public class MicroTxnProc extends BasicStoredProcedure<MicroTxnProcParamHelper> {
	Workload wl;
	public MicroTxnProc() {
		super(new MicroTxnProcParamHelper());
		wl = new Workload();
	}

	@Override
	protected void executeSql() {
		
		for(int i=0;i<wl.QuerySize;++i) {
			double[] ori_value = sampleQuery(wl.query[i]);
			double[] sample_value = sampleQuery("sample "+wl.query[i]);
			
			double error = Math.abs((ori_value[0]-sample_value[0]))/ori_value[0];
			System.out.println("--------------------------------------------------------");
			System.out.println("value of original query : " + ori_value[0]);
			System.out.println("original query respone time : " + ori_value[1]+" (ms)");
			System.out.println("value of sample query : " + sample_value[0]);
			System.out.println("sample query respone time : " + sample_value[1]+" (ms)");
			System.out.println("error : "+error+"(%)");
			System.out.println("--------------------------------------------------------");
		}
		/*
		// SELECT
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
			Scan s = executeQuery("SELECT i_name, i_price FROM item WHERE i_id = " + iid);
			s.beforeFirst();
			if (s.next()) {
				String name = (String) s.getVal("i_name").asJavaVal();
				double price = (Double) s.getVal("i_price").asJavaVal();

				paramHelper.setItemName(name, idx);
				paramHelper.setItemPrice(price, idx);
			} else
				throw new RuntimeException("Cloud not find item record with i_id = " + iid);

			s.close();
		}
		
		// UPDATE
		for (int idx = 0; idx < paramHelper.getWriteCount(); idx++) {
			int iid = paramHelper.getWriteItemId(idx);
			double newPrice = paramHelper.getNewItemPrice(idx);
			executeUpdate("UPDATE item SET i_price = " + newPrice + " WHERE i_id =" + iid);
		}
		*/
		
	}
}
