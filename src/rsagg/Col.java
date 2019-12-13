package rsagg;

import java.sql.Types;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/*
 * Class representing one column and its data.
 */
public class Col<T> {
	
	// VARIABLES ----------------------------------------------------- //
	
	public final int type, index;
	
	public final String typeName, name;
	
	private List<T> dataAgg = new ArrayList<>();
	
	private String[] dataStr;
	
	private Object[] dataObj;
	
	private byte[] dataByte;
	
	private short[] dataShort;
	
	private int[] dataInt;
	
	private long[] dataLong;
	
	private float[] dataFloat;
	
	private double[] dataDouble;
	
	private boolean[] dataBoolean;
	
	private LocalTime[] dataTime;
	
	// CONSTRUCTOR --------------------------------------------------- //
	
	public Col(int type, String typeName, int index, String name) {
		this.type = type;
		this.typeName = typeName;
		
		this.index = index;
		this.name = name;
	}
	
	// PUBLIC -------------------------------------------------------- //
	
	public String[] getStr() {
		return dataStr;
	}
	
	public Object[] getObj() {
		return dataObj;
	}
	
	public byte[] getByte() {
		return dataByte;
	}
	
	public short[] getShort() {
		return dataShort;
	}
	
	public int[] getInt() {
		return dataInt;
	}
	
	public long[] getLong() {
		return dataLong;
	}
	
	public float[] getFloat() {
		return dataFloat;
	}
	
	public double[] getDouble() {
		return dataDouble;
	}
	
	public boolean[] getBoolean() {
		return dataBoolean;
	}
	
	public LocalTime[] getTime() {
		return dataTime;
	}
	
	// PROTECTED ----------------------------------------------------- //
	
	/*
	 * Add an Object to the ArrayList.
	 */
	protected void add(T e) {
		dataAgg.add(e);
	}
	
	/*
	 * Convert ArrayList with Objects to primitive arrays where possible.
	 */
	protected void convertToPrimitive() {
		switch (type) {
			case Types.CHAR:
			case Types.NCHAR:
			case Types.VARCHAR:
			case Types.NVARCHAR:
			case Types.LONGVARCHAR:
			case Types.LONGNVARCHAR:
				dataStr = dataAgg.toArray(new String[dataAgg.size()]);
				break;
			case Types.OTHER:
				dataObj = dataAgg.toArray(new Object[dataAgg.size()]);
				break;
			case Types.TINYINT:
				dataByte = new byte[dataAgg.size()];
				for (int i = 0; i < dataByte.length; i++) {
					dataByte[i] = (byte) dataAgg.get(i);
				}
				break;
			case Types.SMALLINT:
				dataShort = new short[dataAgg.size()];
				for (int i = 0; i < dataShort.length; i++) {
					dataShort[i] = (short) dataAgg.get(i);
				}
				break;
			case Types.INTEGER:
				dataInt = dataAgg.stream().mapToInt(i -> (int) i).toArray();
				break;
			case Types.DATE:
			case Types.TIMESTAMP:
			case Types.BIGINT:
				dataLong = dataAgg.stream().mapToLong(i -> (long) i).toArray();
				break;
			case Types.REAL:
				dataFloat = new float[dataAgg.size()];
				for (int i = 0; i < dataFloat.length; i++) {
					dataFloat[i] = (float) dataAgg.get(i);
				}
				break;
			case Types.FLOAT:
			case Types.DOUBLE:
				dataDouble = dataAgg.stream().mapToDouble(i -> (double) i).toArray();
				break;
			case Types.BOOLEAN:
				dataBoolean = new boolean[dataAgg.size()];
				for (int i = 0; i < dataBoolean.length; i++) {
					dataBoolean[i] = (boolean) dataAgg.get(i);
				}
				break;
			case Types.TIME:
				dataTime = dataAgg.toArray(new LocalTime[dataAgg.size()]);
				break;
		}
		
		dataAgg = null;
	}
	
}