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
	
	// if true, use get[DataType]Obj() instead of get[DataType]()
	public boolean nullable = false;
	
	private List<T> dataAgg = new ArrayList<>();
	
	private String[] dataStr;
	
	private Object[] dataObj;
	
	private byte[] dataByte;
	
	private Byte[] dataByteObj;
	
	private short[] dataShort;
	
	private Short[] dataShortObj;
	
	private int[] dataInt;
	
	private Integer[] dataIntObj;
	
	private long[] dataLong;
	
	private Long[] dataLongObj;
	
	private float[] dataFloat;
	
	private Float[] dataFloatObj;
	
	private double[] dataDouble;
	
	private boolean[] dataBoolean;
	
	private Boolean[] dataBooleanObj;
	
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
	
	public Byte[] getByteObj() {
		return dataByteObj;
	}
	
	public short[] getShort() {
		return dataShort;
	}
	
	public Short[] getShortObj() {
		return dataShortObj;
	}
	
	public int[] getInt() {
		return dataInt;
	}
	
	public Integer[] getIntObj() {
		return dataIntObj;
	}
	
	public long[] getLong() {
		return dataLong;
	}
	
	public Long[] getLongObj() {
		return dataLongObj;
	}
	
	public float[] getFloat() {
		return dataFloat;
	}
	
	public Float[] getFloatObj() {
		return dataFloatObj;
	}
	
	public double[] getDouble() {
		return dataDouble;
	}
	
	public boolean[] getBoolean() {
		return dataBoolean;
	}
	
	public Boolean[] getBooleanObj() {
		return dataBooleanObj;
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
				if (nullable) {
					dataByteObj = dataAgg.toArray(new Byte[dataAgg.size()]);
				} else {
					dataByte = new byte[dataAgg.size()];
					for (int i = 0; i < dataByte.length; i++) {
						dataByte[i] = (byte) dataAgg.get(i);
					}
				}
				break;
			case Types.SMALLINT:
				if (nullable) {
					dataShortObj = dataAgg.toArray(new Short[dataAgg.size()]);
				} else {
					dataShort = new short[dataAgg.size()];
					for (int i = 0; i < dataShort.length; i++) {
						dataShort[i] = (short) dataAgg.get(i);
					}
				}
				break;
			case Types.INTEGER:
				if (nullable) {
					dataIntObj = dataAgg.toArray(new Integer[dataAgg.size()]);
				} else {
					dataInt = dataAgg.stream().mapToInt(i -> (int) i).toArray();
				}
				break;
			case Types.DATE:
			case Types.TIMESTAMP:
			case Types.BIGINT:
				if (nullable) {
					dataLongObj = dataAgg.toArray(new Long[dataAgg.size()]);
				} else {
					dataLong = dataAgg.stream().mapToLong(i -> (long) i).toArray();
				}
				break;
			case Types.REAL:
				if (nullable) {
					dataFloatObj = dataAgg.toArray(new Float[dataAgg.size()]);
				} else {
					dataFloat = new float[dataAgg.size()];
					for (int i = 0; i < dataFloat.length; i++) {
						dataFloat[i] = (float) dataAgg.get(i);
					}
				}
				break;
			case Types.FLOAT:
			case Types.DOUBLE:
				dataDouble = dataAgg.stream().mapToDouble(i -> (double) i).toArray();
				break;
			case Types.BOOLEAN:
				if (nullable) {
					dataBooleanObj = dataAgg.toArray(new Boolean[dataAgg.size()]);
				} else {
					dataBoolean = new boolean[dataAgg.size()];
					for (int i = 0; i < dataBoolean.length; i++) {
						dataBoolean[i] = (boolean) dataAgg.get(i);
					}
				}
				break;
			case Types.TIME:
				dataTime = dataAgg.toArray(new LocalTime[dataAgg.size()]);
				break;
		}
		
		dataAgg = null;
	}
	
}