# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


###########################################################################################################
# module variables:

debug = False

(spark, sc) = (None, None)                  # SparkSession and SparkContext

(hadoop, conf, fs) = (None, None, None)     # see desciption below in sparkutils_init()


import __main__ as main_ns                  # import main namespace

###########################################################################################################

sparkutils_init_complete = False
spark_var_name = 'spark'

def sparkutils_init (i_spark=None, i_debug=False):
    '''
    Initialize module-level variables

    :param i_spark: an object of pyspark.sql.session.SparkSession
    :param i_debug: debug output of the below functions?
    '''

    global sparkutils_init_complete
    if sparkutils_init_complete: return

    global spark, debug

    if i_spark:
        spark = i_spark
    else:
        # get spark session 'spark' variable from main namespace instead
        try:
            exec('spark = main_ns.' + spark_var_name, globals())
        except AttributeError:
            print("Variable '{}' not found in main namespace".format(spark_var_name))
            raise

    debug = i_debug

    from pyspark.sql.session import SparkSession
    if not isinstance(spark, SparkSession):
        raise TypeError("'{}' variable should be of type SparkSession, got {}".format(spark_var_name, type(spark)))

    global sc
    global hadoop, conf, fs

    sc = spark.sparkContext

    hadoop = sc._jvm.org.apache.hadoop      # Get a reference to org.apache.hadoop through py4j interface

    # Create Configuration object
    # see - https://hadoop.apache.org/docs/r2.6.4/api/org/apache/hadoop/conf/Configuration.html
    conf = hadoop.conf.Configuration()

    # get FileSystem object
    # see - https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html#get(org.apache.hadoop.conf.Configuration)
    fs = hadoop.fs.FileSystem.get(conf)

    sparkutils_init_complete = True


def get_spark ():

    sparkutils_init()
    return spark


###########################################################################################################


from pyspark.sql.types import LongType, StructField, StructType


###
def dfPartitionSampler(df, percent_sample=5):
    '''
        Fast sampler. Great for huge datasets with lots of partitions.
        Return first X partitions from original dataframe df,
        while preserving partitioning and order of data.

        :param df: source dataframe
        :param percent_sample: e.g., 5 means first 5% of partitions will be returned only
        :returns: new dataframe
    '''

    sparkutils_init()

    full_partitions_num = df.rdd.getNumPartitions()
    sample_patitions_num = int(full_partitions_num * percent_sample / 100)

    def partition_sampler(partitionIndex, iterator):
        if partitionIndex < sample_patitions_num:
            return iterator
        else:
            return iter(())

    rdd = df.rdd.mapPartitionsWithIndex(partition_sampler, preservesPartitioning=True)

    return spark.createDataFrame(rdd, df.schema)


###
def dfZipWithIndex(df, offset=1, colName="rowId"):
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''

    sparkutils_init()

    new_schema = StructType(
        [StructField(colName, LongType(), True)]  # new added field in front
        + df.schema.fields  # previous schema
    )

    zipped_rdd = df.rdd.zipWithIndex()

    new_rdd = zipped_rdd.map(lambda (row, rowId): ([rowId + offset] + list(row)))

    return spark.createDataFrame(new_rdd, new_schema)


###
def file_to_df(df_name, file_path, delimiter='|', quote='"', escape='\\'
               , header=True, inferSchema=True, columns=None
               , where_clause=None, partitions_sampled_percent=None
               , zipWithIndex=None
               , partitions=None
               , partition_by=None, sort_by=None, cluster_by=None
               , cache=False
               ):
    '''
        Reads in a delimited file and sets up a Spark dataframe

        :param df_name: registers this dataframe as a tempTable/view for SQL access;
                          important: it also registers a global variable under that name
        :param path: path to a file; local files have to have 'file://' prefix;
                     for HDFS files prefix is 'hdfs://' (optional, as hdfs is default)
        :param header: boolean - file has a header record?
        :param inferSchema: boolean - infer data types from data? (requires one extra pass over data)
        :param columns: list - optional list of column names (column names can also be taken from header record)
        :param delimiter: one character
        :param cache: boolean - cache this dataframe?
        :param zipWithIndex: new column name to be assigned by zipWithIndex()
        :param where_clause: filtering (before zipWithIndex/repartition/order by/cluster by etc is applied)
        :param partitions_sampled_percent: if specified, will be passed as percent_sample to dfPartitionSampler()
        :param partitions: number - if specified, will repartition to that number
        :param partition_by: if partitions is specified, this parameter controls which columns to partition by
        :param sort_by: if specified, sort by that key within partitions (no shuffling)
        :param cluster_by: string - list of columns (comma separated) to run CLUSTER BY on
        :param quote: character - by default the quote character is ", but can be set to any character.
                      Delimiters inside quotes are ignored; set to '\0' to disable quoting
        :param escape: character - by default the escape character is \, but can be set to any character.
                      Escaped quote characters are ignored
    '''

    sparkutils_init()

    df = (spark.read.format('csv')
          .option('header', header)
          .option('delimiter', delimiter)
          .option('inferSchema', inferSchema)
          .option('quote', quote)
          .option('escape', escape)
          .load(file_path)
          )

    if columns:
        df = df.toDF(*columns)

    if where_clause:
        before_where_clause = df_name + '_before_where_clause'
        df.registerTempTable(before_where_clause)
        df = spark.sql("SELECT * FROM " + before_where_clause + " WHERE " + where_clause)
        spark.catalog.dropTempView(before_where_clause)

    if partitions_sampled_percent:
        df = dfPartitionSampler(df, partitions_sampled_percent)

    if zipWithIndex:
        # zipWithIndex has to happen before repartition() or any other shuffling
        df = dfZipWithIndex(df, colName=zipWithIndex)

    if partitions:
        if partition_by:
            df = df.repartition(partitions, partition_by)
        else:
            df = df.repartition(partitions)

    if sort_by:
        df = df.sortWithinPartitions(sort_by, ascending=True)

    if cluster_by:
        # "CLUSTER BY" is equivalent to repartition(x, cols) + sortWithinPartitions(cols)

        before_cluster_by = df_name + '_before_cluster_by'
        df.registerTempTable(before_cluster_by)
        df = spark.sql("SELECT * FROM " + before_cluster_by + " CLUSTER BY " + cluster_by)
        spark.catalog.dropTempView(before_cluster_by)

    if cache:
        df = df.cache()

    df.registerTempTable(df_name)
    exec('main_ns.{} = df'.format(df_name))

    return df


###
def sql_to_df(df_name, sql, cache=False, partitions=None, partition_by=None, sort_by=None):
    '''
        Runs an sql query and sets up a Spark dataframe

        :param df_name: registers this dataframe as a tempTable/view for SQL access;
                          important: it also registers a global variable under that name
        :param sql: Spark SQL query to runs
        :param partitions: number - if specified, will repartition to that number
        :param partition_by: if partitions is specified, this parameter controls which columns to partition by
        :param sort_by: if specified, sort by that key within partitions (no shuffling)
        :param cache: cache this dataframe?
    '''

    sparkutils_init()

    df = spark.sql(sql)

    if partitions:
        if partition_by:
            df = df.repartition(partitions, partition_by)
        else:
            df = df.repartition(partitions)

    if sort_by:
        df = df.sortWithinPartitions(sort_by, ascending=True)

    if cache:
        df = df.cache()

    df.registerTempTable(df_name)
    exec('main_ns.{} = df'.format(df_name))

    return df


###########################################################################################################
## Basic wrappers around certain hadoop.fs.FileSystem API calls:
## https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/fs/FileSystem.html

def hdfs_exists (file_path):
    '''
    Returns True if HDFS file exists

    :param file_path: file patch
    :return: boolean
    '''

    sparkutils_init()

    # https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/fs/FileSystem.html#exists(org.apache.hadoop.fs.Path)
    return fs.exists(hadoop.fs.Path(file_path))


def hdfs_drop (file_path, recursive=True):
    '''
    Drop HDFS file/dir

    :param file_path: HDFS file/directory path
    :param recursive: drop subdirectories too
    '''

    sparkutils_init()

    # https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/fs/FileSystem.html#delete(org.apache.hadoop.fs.Path,%20boolean)
    return fs.delete(hadoop.fs.Path(file_path), recursive)


def hdfs_rename (src_name, dst_name):
    '''
    Renames src file to dst file name

    :param src_name: source name
    :param dst_name: target name
    '''

    sparkutils_init()

    # https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/fs/FileSystem.html#rename(org.apache.hadoop.fs.Path,%20org.apache.hadoop.fs.Path)
    return fs.rename(hadoop.fs.Path(src_name), hadoop.fs.Path(dst_name))


def hdfs_file_size (file_path):
    '''
    Returns file size of an HDFS file exists

    :param file_path: file patch
    :return: file size
    '''

    sparkutils_init()

    # See https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/fs/ContentSummary.html
    #   - getLength()
    return fs.getContentSummary(hadoop.fs.Path(file_path)).getLength()



###########################################################################################################

def HDFScopyMerge (src_dir, dst_file, overwrite=False, deleteSource=False):
    
    """
    copyMerge() merges files from an HDFS directory to an HDFS files. 
    File names are sorted in alphabetical order for merge order.
    Inspired by https://hadoop.apache.org/docs/r2.7.1/api/src-html/org/apache/hadoop/fs/FileUtil.html#line.382

    :param src_dir: source directoy to get files from 
    :param dst_file: destination file to merge file to
    :param overwrite: overwrite destination file if already exists? this would also overwrite temp file if exists
    :param deleteSource: drop source directory after merge is complete
    """

    sparkutils_init()

    def debug_print (message):
        if debug:
            print("HDFScopyMerge(): " + message)

    # check files that will be merged
    files = []
    for f in fs.listStatus(hadoop.fs.Path(src_dir)):
        if f.isFile():
            files.append(f.getPath())
    if not files:
        raise ValueError("Source directory {} is empty".format(src_dir))
    # determine order of files in which they will be written:
    files.sort(key=lambda f: str(f))

    if overwrite and hdfs_exists(dst_file):
        hdfs_drop(dst_file)
        debug_print("Target file {} dropped".format(dst_file))

    # use temp file for the duration of the merge operation
    dst_file_tmp = "{}.IN_PROGRESS.tmp".format(dst_file)

    # dst_permission = hadoop.fs.permission.FsPermission.valueOf(permission)      # , permission='-rw-r-----'
    out_stream = fs.create(hadoop.fs.Path(dst_file_tmp), overwrite)

    try:
        # loop over files in alphabetical order and append them one by one to the target file
        for filename in files:
            debug_print("Appending file {} into {}".format(filename, dst_file_tmp))

            in_stream = fs.open(filename)   # InputStream object
            try:
                hadoop.io.IOUtils.copyBytes(in_stream, out_stream, conf, False)     # False means don't close out_stream
            finally:
                in_stream.close()
    finally:
        out_stream.close()

    if deleteSource:
        hdfs_drop(src_dir)
        debug_print("Source directory {} removed.".format(src_dir))

    try:
        hdfs_rename(dst_file_tmp, dst_file)
        debug_print("Temp file renamed to {}".format(dst_file))
    except:
        hdfs_drop(dst_file_tmp)  # drop temp file if we can't rename it to target name
        raise


def HDFSwriteString (dst_file, content, overwrite=True, appendEOL=True):
    
    """
    Creates an HDFS file with given content.
    Notice this is usable only for small (metadata like) files.

    :param dst_file: destination HDFS file to write to
    :param content: string to be written to the file
    :param overwrite: overwrite target file?
    :param appendEOL: append new line character?
    """

    sparkutils_init()

    out_stream = fs.create(hadoop.fs.Path(dst_file), overwrite)

    if appendEOL:
        content += "\n"

    try:
        out_stream.write(bytearray(content))
    finally:
        out_stream.close()


def dataframeToHDFSfile (dataframe, dst_file, overwrite=False
                         , header=True, delimiter=','
                         , quoteMode='MINIMAL'
                         , quote='"', escape='\\'
                         , compression='none'
                         ):

    """
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
    :param quote: character - by default the quote character is ", but can be set to any character.
                  Delimiters inside quotes are ignored; set to '\0' to disable quoting
    :param escape: character - by default the escape character is \, but can be set to any character.
                  Escaped quote characters are ignored
    :param quoteMode: https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/QuoteMode.html
    :param compression: compression codec to use when saving to file. This can be one of the known case-insensitive
                        shorten names (none, bzip2, gzip, lz4, snappy and deflate).

    """

    sparkutils_init()

    if not overwrite and hdfs_exists(dst_file):
        raise ValueError("Target file {} already exists and Overwrite is not requested".format(dst_file))

    dst_dir = dst_file + '.tmpdir'

    (dataframe
        .write
        .option('header', False)    # always save without header as if there are multiple partitions,
                                    # each datafile will have a header - not good.
        .option('delimiter', delimiter)
        .option('quoteMode', quoteMode)
        .option('quote', quote)
        .option('escape', escape)
        .option('compression', compression)
        .mode('overwrite')     # temp directory will always be overwritten
        .csv(dst_dir)
        )

    if header:
        # we will create a separate file with just a header record
        header_record = delimiter.join(dataframe.columns)
        header_filename = "{}/--00_header.csv".format(dst_dir)  # have to make sure header filename is 1st in
                                                                # alphabetical order
        HDFSwriteString(header_filename, header_record)

    HDFScopyMerge(dst_dir, dst_file, overwrite, deleteSource=True)


