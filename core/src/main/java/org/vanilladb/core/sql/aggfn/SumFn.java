/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
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
package org.vanilladb.core.sql.aggfn;

import static org.vanilladb.core.sql.Type.DOUBLE;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.Regions;
/**
 * The <em>sum</em> aggregation function.
 */
public class SumFn extends AggregationFn {
	private String fldName;
	private Constant val;
	private boolean isSample = false;

	public SumFn(String fldName) {
		this.fldName = fldName;
	}
	public void setSample() {
		isSample = true;
	}

	@Override
	public void processFirst(Record rec) {
		if(isSample) {
			int r_id = (int)rec.getVal("r_id").asJavaVal();
			int ki = VanillaDb.getRegions().getKibyId(r_id);
			int ni = VanillaDb.getRegions().getNibyId(r_id);
			DoubleConstant dKi = new DoubleConstant((double)ki);
			DoubleConstant dNi = new DoubleConstant((double)ni);
			Constant c = rec.getVal(fldName);
			this.val = c.castTo(DOUBLE).mul(dNi).div(dKi);
		}else {
			Constant c = rec.getVal(fldName);
			this.val = c.castTo(DOUBLE);
		}
	}

	@Override
	public void processNext(Record rec) {
		if(isSample) {
			int r_id = (int)rec.getVal("r_id").asJavaVal();
			int ki = VanillaDb.getRegions().getKibyId(r_id);
			int ni = VanillaDb.getRegions().getNibyId(r_id);
			DoubleConstant dKi = new DoubleConstant((double)ki);
			DoubleConstant dNi = new DoubleConstant((double)ni);
			Constant newval = rec.getVal(fldName).castTo(DOUBLE).mul(dNi).div(dKi);
			val = val.add(newval);
		}else {
			Constant newval = rec.getVal(fldName);
			val = val.add(newval);
		}
	}

	@Override
	public String argumentFieldName() {
		return fldName;
	}

	@Override
	public String fieldName() {
		return "sumof" + fldName;
	}

	@Override
	public Constant value() {
		return val;
	}

	@Override
	public Type fieldType() {
		return DOUBLE;
	}

	@Override
	public boolean isArgumentTypeDependent() {
		return false;
	}

	@Override
	public int hashCode() {
		return fieldName().hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (this == other)
			return true;

		if (!(other.getClass().equals(SumFn.class)))
			return false;

		SumFn otherSumFn = (SumFn) other;
		if (!fldName.equals(otherSumFn.fldName))
			return false;

		return true;
	}
}
