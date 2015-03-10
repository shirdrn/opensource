package org.shirdrn.solr.indexing.common;

public class Record {
	long id;
	String area;
	int buildingType;
	String category;
	int temperature;
	String code;
	double latitude;
	double longitude;
	String when;
	int floor;
	
	public Record() {
		super();
	}
	
	public Record(long id, String area, int buildingType, String category,
			int temperature, String code, double latitude,
			double longitude, String when) {
		super();
		this.id = id;
		this.area = area;
		this.buildingType = buildingType;
		this.category = category;
		this.temperature = temperature;
		this.code = code;
		this.latitude = latitude;
		this.longitude = longitude;
		this.when = when;
	}
	
	public int getFloor() {
		return floor;
	}

	public void setFloor(int floor) {
		this.floor = floor;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(id).append(",")
		.append(area).append(",")
		.append(buildingType).append(",")
		.append(category).append(",")
		.append(temperature).append(",")
		.append(floor).append(",")
		.append(code).append(",")
		.append(latitude).append(",")
		.append(longitude).append(",")
		.append(when);
		return sb.toString();
	}

}