package com.cloudwick.Pig;


import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;


public class GeoLocationUDF extends EvalFunc<String> {
	
	DatabaseReader reader;
	
	public GeoLocationUDF(String fileURI) throws IOException{
	reader = new DatabaseReader.Builder(new File(fileURI)).build();
	}
	
	@Override
	public String exec(Tuple input) throws IOException {
		
		try {
			CityResponse response = reader.city(InetAddress.getByName((String)input.get(0)));
			String city = response.getCity().getName();
			String country = response.getCountry().getName();
			StringBuilder result = new StringBuilder();		
			if(city != null && city.length()!=0)
				{
					result.append(city);
					result.append(" ,");
				}
			if(country != null && country.length()!=0)
				result.append(country);
			String finalResult = result.toString();
			return finalResult.length() == 0 ?  input.get(0)+"No mapping" :  finalResult;
		} catch (GeoIp2Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

}
