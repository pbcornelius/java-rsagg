# java-rsagg
Java Column-wise ResultSet Aggregation

A simple tool that aggregates SQL ResultSets by column. This can improve speed for example when the data needs to be passed to Python, as data types are usually the same per column. The data types work for H2, but may not work for other drivers (e.g., `LocalTime` may not be supported). Create an `RsAgg` instance with a given `ResultSet`, use `RsAgg.agg()` to start the column aggregation, after which you can use `Col.get...()` to access column data by data type. This is faster than row-by-row access, even when the data is not converted to `ndarrays` by Jpype (e.g., `String`s).

#### Use from Python

```
cursor.execute('SELECT * FROM test')
rsagg = jpype.JClass("rsagg.RsAgg")(cursor._rs)


try:
    rsagg.agg()
except java.lang.Exception as ex:
    print(str(ex))
    print(ex.stacktrace())

df = pd.DataFrame(columns=[col.name for col in rsagg.cols])
for col in rsagg.cols:
    df[col.name] = datadict[col.typeName](col)
```