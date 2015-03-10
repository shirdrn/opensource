package org.shirdrn.hadoop.join.reduceside;

public enum JoinSide {

	LEFT(0),
	RIGHT(1);
	
	private final int code;
	private static final int FIRST_CODE = values()[0].code;
	
	public int getCode() {
		return code;
	}
	
	private JoinSide(int code) {
		this.code = code;
	}
	
	public static JoinSide valueOf(int code) {
		final int current = (code & 0xff) - FIRST_CODE;
		return current < 0 || current >= values().length ? null : values()[current];
	}
	
	
}
