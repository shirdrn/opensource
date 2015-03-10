package org.shirdrn.hadoop.dataset;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.shirdrn.hadoop.dataset.CountryCodeSet;

public class TestCountryCodeSet {

	@Test
	public void getSize() {
		assertEquals(245, 
				CountryCodeSet.newInstance().getCountryCodes().size());
	}
}
