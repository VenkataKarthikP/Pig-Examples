package com.cloudwick.Pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class PigMask extends EvalFunc<String>{

	@Override
	public String exec(Tuple arg0) throws IOException {
		// TODO Auto-generated method stub
		
		return new StringBuilder((String) arg0.get(0)).reverse().toString();
		
	}

}
