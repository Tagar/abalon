# abalon
Various utility functions for Hadoop, Spark etc

<h1 id="abalon.spark.sparkutils">abalon.spark.sparkutils</h1>

<h2 id="abalon.spark.sparkutils.sparkutils_init">sparkutils_init</h2>

```python
sparkutils_init(i_spark, i_debug=False)
```

Initialize module-level variables

:param i_spark: an object of pyspark.sql.session.SparkSession
:param i_debug: debug output of the below functions?


<h2 id="abalon.spark.sparkutils.file_to_df">file_to_df</h2>

```python
file_to_df(df_name, file_path, header=True, delimiter='|', inferSchema=True, cache=False)
```

Reads in a delimited file and sets up a Spark dataframe 

:param df_name: registers this dataframe as a tempTable/view for SQL access;
                  important: it also registers a global variable under that name
:param file_path: path to a file; local files have to have 'file://' prefix; for HDFS files prefix is 'hdfs://' (optional, as hdfs is default)
:param header: boolean - file has a header record? 
:param inferSchema: boolean - infer data types from data? (requires one extra pass over data)
:param delimiter: one character
:param cache: cache this dataframe?


<h2 id="abalon.spark.sparkutils.HDFSwriteString">HDFSwriteString</h2>

```python
HDFSwriteString(dst_file, content, overwrite=True)
```

Creates an HDFS file with given content.
Notice this is usable only for small (metadata like) files.

:param dst_file: destination HDFS file to write to
:param content: string to be written to the file
:param overwrite: overwrite target file?

<h2 id="abalon.spark.sparkutils.dataframeToHDFSfile">dataframeToHDFSfile</h2>

```python
dataframeToHDFSfile(dataframe, dst_file, overwrite=False, header='true', delimiter=',', quoteMode='MINIMAL')
```

dataframeToHDFSfile() saves a dataframe as a delimited file.
It is faster than using dataframe.coalesce(1).write.option('header', 'true').csv(dst_file)
as it doesn't require dataframe to be repartitioned/coalesced before writing.
dataframeToTextFile() uses copyMerge() with HDFS API to merge files. 

:param dataframe: source dataframe 
:param dst_file: destination file to merge file to
:param overwrite: overwrite destination file if already exists? 
:param header: produce header record? Note: the the header record isn't written by Spark,
           but by this function instead to workaround having header records in each part file.
:param delimiter: delimiter character 
:param quoteMode: https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/QuoteMode.html 

<h2 id="abalon.spark.sparkutils.sql_to_df">sql_to_df</h2>

```python
sql_to_df(df_name, sql, cache=False)
```

Runs an sql query and sets up a Spark dataframe 

:param df_name: registers this dataframe as a tempTable/view for SQL access;
                  important: it also registers a global variable under that name
:param sql: Spark SQL query to runs
:param cache: cache this dataframe?

<h2 id="abalon.spark.sparkutils.HDFScopyMerge">HDFScopyMerge</h2>

```python
HDFScopyMerge(src_dir, dst_file, overwrite=False, deleteSource=False)
```

copyMerge() merges files from an HDFS directory to an HDFS files. 
File names are sorted in alphabetical order for merge order.
Inspired by https://hadoop.apache.org/docs/r2.7.1/api/src-html/org/apache/hadoop/fs/FileUtil.html`line.382`

:param src_dir: source directoy to get files from 
:param dst_file: destination file to merge file to
:param overwrite: overwrite destination file if already exists? 
:param deleteSource: drop source directory after merge is complete

