package rsagg;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

/*
 * A simple class that aggregates SQL ResultSets by column. This can
 * improve speed for example when the data needs to be passed to Python.
 * The data types work for H2, but may not work for other drivers (e.g.,
 * LocalTime may not be supported). Use Col.get...() to access column
 * data. This is faster than row-by-row access, even when the data is
 * not converted to ndarrays by Jpype (e.g., Strings).
 * 
 * Notes:
 * - returns primitive Java types where possible
 * - unless a column contains NULL:
 *   - double: NULL is replaced with Numpy's NaN for doubles
 *   - all other primitives: object wrapper arrays rather than primitive
 *     arrays are returned (e.g., getIntObj() -> Integer[]). This is sig.
 *     slower than even row-wise iteration, which automatically switches
 *     to double. Either replace NULL values with non-NULL numeric values
 *     or cast to double.
 */
public class RsAgg {
	
	// VARIABLES ----------------------------------------------------- //
	
	@SuppressWarnings("rawtypes")
	public final Col[] cols;
	
	private ResultSet rs;
	
	private ResultSetMetaData meta;
	
	// CONSTRUCTOR --------------------------------------------------- //
	
	public RsAgg(ResultSet rs) throws SQLException {
		this.rs = rs;
		meta = rs.getMetaData();
		cols = new Col[meta.getColumnCount()];
	}
	
	// PUBLIC -------------------------------------------------------- //
	
	@SuppressWarnings("rawtypes")
	public void agg() throws SQLException {
		try {
			initCols();
			
			if (rs.next()) {
				readRow();
			}
			
			while (rs.next()) {
				readRow();
			}
			
			for (Col col : cols) {
				col.convertToPrimitive();
			}
		} finally {
			rs.close();
			rs = null;
			meta = null;
		}
	}
	
	// PRIVATE ------------------------------------------------------- //
	
	private void initCols() throws SQLException {
		for (int i = 1; i <= cols.length; i++) {
			switch (meta.getColumnType(i)) {
				case Types.CHAR:
				case Types.NCHAR:
				case Types.VARCHAR:
				case Types.NVARCHAR:
				case Types.LONGVARCHAR:
				case Types.LONGNVARCHAR:
					cols[i - 1] = new Col<String>(meta.getColumnType(i),
							meta.getColumnTypeName(i),
							i,
							meta.getColumnLabel(i));
					break;
				case Types.OTHER:
					cols[i - 1] = new Col<Object>(meta.getColumnType(i),
							meta.getColumnTypeName(i),
							i,
							meta.getColumnLabel(i));
					break;
				case Types.TINYINT:
					cols[i - 1] = new Col<Byte>(meta.getColumnType(i),
							meta.getColumnTypeName(i),
							i,
							meta.getColumnLabel(i));
					break;
				case Types.SMALLINT:
					cols[i - 1] = new Col<Short>(meta.getColumnType(i),
							meta.getColumnTypeName(i),
							i,
							meta.getColumnLabel(i));
					break;
				case Types.INTEGER:
					cols[i - 1] = new Col<Integer>(meta.getColumnType(i),
							meta.getColumnTypeName(i),
							i,
							meta.getColumnLabel(i));
					break;
				case Types.DATE:
				case Types.TIMESTAMP:
				case Types.BIGINT:
					cols[i - 1] = new Col<Long>(meta.getColumnType(i),
							meta.getColumnTypeName(i),
							i,
							meta.getColumnLabel(i));
					break;
				case Types.REAL:
					cols[i - 1] = new Col<Float>(meta.getColumnType(i),
							meta.getColumnTypeName(i),
							i,
							meta.getColumnLabel(i));
					break;
				case Types.FLOAT:
				case Types.DOUBLE:
					cols[i - 1] = new Col<Double>(meta.getColumnType(i),
							meta.getColumnTypeName(i),
							i,
							meta.getColumnLabel(i));
					break;
				case Types.BOOLEAN:
					cols[i - 1] = new Col<Boolean>(meta.getColumnType(i),
							meta.getColumnTypeName(i),
							i,
							meta.getColumnLabel(i));
					break;
				case Types.TIME:
					cols[i - 1] = new Col<LocalTime>(meta.getColumnType(i),
							meta.getColumnTypeName(i),
							i,
							meta.getColumnLabel(i));
					break;
				default:
					throw new RuntimeException(String.format("column=%d unsupported data type %s",
							i,
							meta.getColumnType(i)));
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private void readRow() throws SQLException {
		for (int i = 1; i <= cols.length; i++) {
			switch (cols[i - 1].type) {
				case Types.CHAR:
				case Types.NCHAR:
				case Types.VARCHAR:
				case Types.NVARCHAR:
				case Types.LONGVARCHAR:
				case Types.LONGNVARCHAR:
					cols[i - 1].add(rs.getString(i));
					break;
				case Types.OTHER:
					cols[i - 1].add(rs.getObject(i));
					break;
				case Types.TINYINT:
					byte val_byte = rs.getByte(i);
					if (rs.wasNull()) {
						cols[i - 1].nullable = true;
						cols[i - 1].add(null);
					} else {
						cols[i - 1].add(val_byte);
					}
					break;
				case Types.SMALLINT:
					short val_short = rs.getShort(i);
					if (rs.wasNull()) {
						cols[i - 1].nullable = true;
						cols[i - 1].add(null);
					} else {
						cols[i - 1].add(val_short);
					}
					break;
				case Types.INTEGER:
					int val_int = rs.getInt(i);
					if (rs.wasNull()) {
						cols[i - 1].nullable = true;
						cols[i - 1].add(null);
					} else {
						cols[i - 1].add(val_int);
					}
					break;
				case Types.BIGINT:
					long val_long = rs.getLong(i);
					if (rs.wasNull()) {
						cols[i - 1].nullable = true;
						cols[i - 1].add(null);
					} else {
						cols[i - 1].add(val_long);
					}
					break;
				case Types.REAL:
					float val_float = rs.getFloat(i);
					if (rs.wasNull()) {
						cols[i - 1].nullable = true;
						cols[i - 1].add(null);
					} else {
						cols[i - 1].add(val_float);
					}
					break;
				case Types.FLOAT:
				case Types.DOUBLE:
					double val_double = rs.getDouble(i);
					if (rs.wasNull()) {
						// save as Numpy's NaN value for doubles
						cols[i - 1].add(Double.longBitsToDouble(0x7ff8000000000000L));
					} else {
						cols[i - 1].add(val_double);
					}
					break;
				case Types.BOOLEAN:
					boolean val_bool = rs.getBoolean(i);
					if (rs.wasNull()) {
						cols[i - 1].nullable = true;
						cols[i - 1].add(null);
					} else {
						cols[i - 1].add(val_bool);
					}
					break;
				case Types.TIME:
					// passed as is: there is no matching type on the Python/Numpy side.
					cols[i - 1].add(rs.getObject(i, LocalTime.class));
					break;
				case Types.DATE:
					LocalDate val_date = rs.getObject(i, LocalDate.class);
					if (rs.wasNull()) {
						cols[i - 1].nullable = true;
						cols[i - 1].add(null);
					} else {
						// passed as epoch days since 1970 (long), which is very fast and
						// can easily be parsed by Numpy.datetime64[D]
						cols[i - 1].add(val_date.toEpochDay());
					}
					break;
				case Types.TIMESTAMP:
					LocalDateTime val_timestamp = rs.getObject(i, LocalDateTime.class);
					if (rs.wasNull()) {
						cols[i - 1].nullable = true;
						cols[i - 1].add(null);
					} else {
						// passed as epoch seconds since 1970 (long), which is very fast and
						// can easily be parsed by Numpy.datetime64[s]
						// numpty.datetime64 is in UTC
						cols[i - 1].add(val_timestamp.toEpochSecond(ZoneOffset.UTC));
					}
					break;
			}
		}
	}
	
}