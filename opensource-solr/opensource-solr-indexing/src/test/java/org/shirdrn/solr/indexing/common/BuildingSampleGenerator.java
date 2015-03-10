package org.shirdrn.solr.indexing.common;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class BuildingSampleGenerator {

	private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	private Random random = new Random();
	
	static String[] areas = {
		"北京", "上海", "深圳", "广州", "天津", "重庆","成都", 
		"银川", "沈阳", "大连", "吉林", "郑州", "徐州", "兰州",
		"东京", "纽约", "贵州", "长春", "大连", "武汉","南京", 
		"海口", "太原", "济南", "日照", "菏泽", "包头", "松原"
	};
	
	long pre = 0L;
	long current = 0L;
	public synchronized long genId() {
		current = System.nanoTime();
		if(current == pre) {
			try {
				Thread.sleep(0, 1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			current = System.nanoTime();
			pre = current;
		}
		return current;
	}
	
	public String genArea() {
		return areas[random.nextInt(areas.length)];
	}
	
	private int maxLatitude = 90;
	private int maxLongitude = 180;
	
	public Coordinate genCoordinate() {
		int beforeDot = random.nextInt(maxLatitude);
		double afterDot = random.nextDouble();
		double lat = beforeDot + afterDot;
		
		beforeDot = random.nextInt(maxLongitude);
		afterDot = random.nextDouble();
		double lon = beforeDot + afterDot;
		
		return new Coordinate(lat, lon);
	}
	
	private Random random1 = new Random(System.currentTimeMillis());
	private Random random2 = new Random(2 * System.currentTimeMillis());
	public int genFloors() {
		return 1 + random1.nextInt(50) + random2.nextInt(50);
	}
	
	static int[] signs = {-1, 1};
	public int genTemperature() {
		return signs[random.nextInt(2)] * random.nextInt(81);
	}
	
	static String[] codes = {"A", "B", "C", "D", "E", "F", "G", "H", "I", 
		"J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", 
		"W", "X", "Y", "Z"};
	public String genCode() {
		return codes[random.nextInt(codes.length)];
	}
	
	static int[] types = {0, 1, 2, 3};
	public int genBuildingType() {
		return types[random.nextInt(types.length)];
	}
	
	static String[] categories = {
		"办公建筑", "教育建筑", "商业建筑", "文教建筑", "医卫建筑",
		"住宅", "宿舍", "公寓", "工业建筑"};
	public String genBuildingCategory() {
		return categories[random.nextInt(categories.length)];
	}
	
	public void generate(String file, int count, boolean appendHeading) throws IOException {
		BufferedWriter w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"));
		if(appendHeading) {
			w.write("id,area,building_type,category,temperature,floor,code,latitude,longitude,when");
			w.newLine();
		}
		
		
		for(int i=0; i<count; i++) {
			String when = df.format(new Date());
			
			StringBuffer sb = new StringBuffer();
			sb.append(genId()).append(",")
				.append("\"").append(genArea()).append("\"").append(",")
				.append(genBuildingType()).append(",")
				.append("\"").append(genBuildingCategory()).append("\"").append(",")
				.append(genTemperature()).append(",")
				.append(genFloors()).append(",")
				.append("\"").append(genCode()).append("\"").append(",");
			Coordinate coord = genCoordinate();
			sb.append(coord.latitude).append(",")
				.append(coord.longitude).append(",")
				.append("\"").append(when).append("\"");
			w.write(sb.toString());
			w.newLine();
		}
		w.close();
		System.out.println("Finished: file=" + file);
	}
	
	public static void main(String[] args) throws Exception {
		BuildingSampleGenerator gen = new BuildingSampleGenerator();
		String file = "E:\\Develop\\eclipse-jee-kepler\\workspace\\building_files";
		for(int i=0; i<=9; i++) {
			String f = new String(file + "_5w_0" + i + ".csv");
			gen.generate(f, 50000, false);
		}
	}

}
