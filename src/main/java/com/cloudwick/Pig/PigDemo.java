/**
 * 
 */
package com.cloudwick.Pig;

import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * @author Venkata Karthik
 *
 */
public class PigDemo extends FilterFunc {

	@Override
	public Boolean exec(Tuple arg0) throws IOException {
		// TODO Auto-generated method stub
		Object o = arg0.get(0);
		int i= Integer.parseInt((String) o);
		if(i>5)
		return true;
		else
		return false;
	}

	
	
}
