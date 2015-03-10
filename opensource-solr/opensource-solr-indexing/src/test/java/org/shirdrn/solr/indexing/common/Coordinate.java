package org.shirdrn.solr.indexing.common;

public class Coordinate {
	
	double latitude;
	double longitude;
	
	public Coordinate() {
		super();
	}
	
	public Coordinate(double latitude, double longitude) {
		super();
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}
}