package org.vanilladb.core.server;

import java.util.Vector;

public class Regions {
	
	public class R{
		
		public int r_id;
		public int ni;
		public int ki;
		public R(int r_id,int ni,int ki) {
			this.r_id = r_id;
			this.ni = ni;
			this.ki = ki;
		}
	}
	Vector<R> regions;
	public Regions() {
		regions = new Vector<R>();
	}
	public void addRegion(int r_id,int ni,int ki) {
		R r = new R(r_id,ni,ki);
		regions.add(r);
	}
	public int getNibyId(int r_id) {
		for(R r:regions) {
			if(r.r_id == r_id) 
				return r.ni;
		}
		return -1;
	}
	public int getKibyId(int r_id) {
		for(R r:regions) {
			if(r.r_id == r_id) 
				return r.ki;
		}
		return -1;
	}
}
