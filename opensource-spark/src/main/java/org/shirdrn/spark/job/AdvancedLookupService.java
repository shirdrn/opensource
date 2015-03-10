package org.shirdrn.spark.job;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.shirdrn.spark.job.maxmind.Country;
import org.shirdrn.spark.job.maxmind.LookupService;

import org.xbill.DNS.Address;

/**
 * Extends the implementation provided by 'maxmind', by using
 * DnsJava open source project which includes some useful tools, 
 * such as DNS parser, etc.
 * 
 * Here, DnsJava toolkit can detect multiple IP addresses related a domain, from 
 * which we can acquire better speed and performance. 
 * 
 * @author Yanjun
 */
public class AdvancedLookupService extends LookupService {

	private final Country UNKNOWN_COUNTRY = new Country("--", "N/A");
	
	public AdvancedLookupService(File databaseFile, int options) throws IOException{
		super(databaseFile, options);
	}

	@Override
	public Country getCountry(String ipAddress) {
		return getCountries(ipAddress).get(0);
	}
	
	public List<Country> getCountries(String ipAddress) {
		List<Country> countries = new ArrayList<Country>();
		InetAddress[] ipAddresses;
		try {
			ipAddresses = Address.getAllByName(ipAddress);
			for(InetAddress addr : ipAddresses) {
				Country country = getCountry(bytesToLong(addr.getAddress()));
				countries.add(country);
			}
		} catch (Exception e) {
//			e.printStackTrace();
		} finally {
			if(countries.isEmpty()) {
				countries.add(UNKNOWN_COUNTRY);
			}
		}
		return countries;
	}
	
	/**
     * Returns the long version of an IP address given an InetAddress object.
     * @param address the InetAddress.
     * @return the long form of the IP address.
     */
    private static long bytesToLong(byte [] address) {
        long ipnum = 0;
        for (int i = 0; i < 4; ++i) {
            long y = address[i];
            if (y < 0) {
                y+= 256;
            }
            ipnum += y << ((3-i)*8);
        }
        return ipnum;
    }

}
