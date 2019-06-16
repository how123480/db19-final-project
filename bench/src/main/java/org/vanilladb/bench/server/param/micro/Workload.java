package org.vanilladb.bench.server.param.micro;

import java.util.Arrays;

public class Workload {
	public static final int WorkLoadSize = 18;
	public static final int IntersectSize = 9;
	public static final int QuerySize = 18;
	public static String init_str;
	public static byte[] toBytes(Boolean[] input) {
	    byte[] toReturn = new byte[input.length / 8];
	    for (int entry = 0; entry < toReturn.length; entry++) {
	        for (int bit = 0; bit < 8; bit++) {
	            if (input[entry * 8 + bit]) {
	                toReturn[entry] |= (128 >> bit);
	            }
	        }
	    }
	    return toReturn;
	} 
	public static Boolean [] bytesToBooleans(byte [] bytes){
	    Boolean [] bools = new Boolean[bytes.length * 8];
	    byte [] pos = new byte[]{(byte)0x80, 0x40, 0x20, 0x10, 0x8, 0x4, 0x2, 0x1};

	    for(int i = 0; i < bytes.length; i++){
	        for(int j = i * 8, k = 0; k < 8; j++, k++){
	            bools[j] = (bytes[i] & pos[k]) != 0;
	        }
	    }
	    return bools;
	}
	public static String byteArrayToStr(byte[] byteArray) {
	    if (byteArray == null) {
	        return null;
	    }
	    String str = new String(byteArray);
	    return str;
	}
	public static String booleanArray2Str(Boolean[] b) {
		String s = "";
		int b_size = b.length; 
		for(int i=0;i<b_size;++i) {
			int b_int = b[i] ? 1 : 0;
			s+=String.valueOf(b_int);
		}
		return s;
	}
	
	public static Boolean[] init_vect;
	
	public Workload() {
		init_vect = new Boolean[WorkLoadSize];
		Arrays.fill(init_vect, Boolean.FALSE);
		this.init_str = booleanArray2Str(init_vect);
	}
	
	public static final String[] workload = {
			"SELECT COUNT(i_im_id) FROM item WHERE i_im_id < 4967 and i_im_id > 1611",
			"SELECT COUNT(i_id) FROM item WHERE i_id < 4036 and i_id > 3626",
			"SELECT SUM(i_im_id) FROM item WHERE i_im_id < 7233 and i_im_id > 6239",
			"SELECT SUM(i_price) FROM item WHERE i_price < 32 and i_price > 27",
			"SELECT COUNT(i_price) FROM item WHERE i_price < 16 and i_price > 8",
			"SELECT COUNT(i_price) FROM item WHERE i_price < 99 and i_price > 76",
			"SELECT SUM(i_im_id) FROM item WHERE i_im_id < 506 and i_im_id > 72",
			"SELECT COUNT(i_im_id) FROM item WHERE i_im_id < 3316 and i_im_id > 2646",
			"SELECT SUM(i_id) FROM item WHERE i_id < 138 and i_id > 96",
			"SELECT COUNT(i_price) FROM item WHERE i_price < 8 and i_price > 8",
			"SELECT COUNT(i_im_id) FROM item WHERE i_im_id < 9127 and i_im_id > 7523",
			"SELECT SUM(i_id) FROM item WHERE i_id < 21417 and i_id > 13349",
			"SELECT SUM(i_id) FROM item WHERE i_id < 72004 and i_id > 15570",
			"SELECT SUM(i_im_id) FROM item WHERE i_im_id < 8615 and i_im_id > 3136",
			"SELECT COUNT(i_price) FROM item WHERE i_price < 31 and i_price > 14",
			"SELECT COUNT(i_im_id) FROM item WHERE i_im_id < 9830 and i_im_id > 5076",
			"SELECT COUNT(i_id) FROM item WHERE i_id < 27945 and i_id > 22824",
			"SELECT SUM(i_im_id) FROM item WHERE i_im_id < 5234 and i_im_id > 1515"
	};
	public static final String[] query = {
			"SELECT COUNT(i_im_id) FROM item WHERE i_im_id < 4967 and i_im_id > 1611",
			"SELECT COUNT(i_id) FROM item WHERE i_id < 4036 and i_id > 3626",
			"SELECT SUM(i_im_id) FROM item WHERE i_im_id < 7233 and i_im_id > 6239",
			"SELECT SUM(i_price) FROM item WHERE i_price < 32 and i_price > 27",
			"SELECT COUNT(i_price) FROM item WHERE i_price < 16 and i_price > 8",
			"SELECT COUNT(i_price) FROM item WHERE i_price < 99 and i_price > 76",
			"SELECT SUM(i_im_id) FROM item WHERE i_im_id < 506 and i_im_id > 72",
			"SELECT COUNT(i_im_id) FROM item WHERE i_im_id < 3316 and i_im_id > 2646",
			"SELECT SUM(i_id) FROM item WHERE i_id < 138 and i_id > 96",
			"SELECT SUM(i_im_id) FROM item WHERE i_im_id < 894 and i_im_id > 664",
			"SELECT COUNT(i_id) FROM item WHERE i_id < 25588 and i_id > 14458",
			"SELECT COUNT(i_price) FROM item WHERE i_price < 68 and i_price > 2",
			"SELECT COUNT(i_price) FROM item WHERE i_price < 46 and i_price > 19",
			"SELECT SUM(i_im_id) FROM item WHERE i_im_id < 6978 and i_im_id > 277",
			"SELECT COUNT(i_id) FROM item WHERE i_id < 19342 and i_id > 7598",
			"SELECT COUNT(i_im_id) FROM item WHERE i_im_id < 8539 and i_im_id > 5248",
			"SELECT SUM(i_price) FROM item WHERE i_price < 65 and i_price > 42",
			"SELECT COUNT(i_price) FROM item WHERE i_price < 28 and i_price > 3"

	};
}
