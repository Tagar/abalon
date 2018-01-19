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


def sparkutils_init (i_spark, i_debug=False):
    '''
    Initialize module-level variables

    :param i_spark: an object of pyspark.sql.session.SparkSession
    :param i_debug: debug output of the below functions?
    '''

    global spark, debug
    (spark, debug) = (i_spark, i_debug)

    from pyspark.sql.session import SparkSession
    if not isinstance(spark, SparkSession):
        raise TypeError("spark parameter should be of type SparkSession")

    global sc
    sc = spark.sparkContext

    global hadoop, conf, fs

    hadoop = sc._jvm.org.apache.hadoop      # Get a reference to org.apache.hadoop through py4j object

    # Create Configuration object
    # see - https://hadoop.apache.org/docs/r2.6.4/api/org/apache/hadoop/conf/Configuration.html
    conf = hadoop.conf.Configuration()

    # get FileSystem object
    # see - https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html#get(org.apache.hadoop.conf.Configuration)
    fs = hadoop.fs.FileSystem.get(conf)


###########################################################################################################


from pyspark.sql.types import LongType, StructField, StructType

###
def dfZipWithIndex(df, offset=1, colName="rowId"):
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''

    new_schema = StructType(
        [StructField(colName, LongType(), True)]  # new added field in front
        + df.schema.fields  # previous schema
    )

    zipped_rdd = df.rdd.zipWithIndex()

    new_rdd = zipped_rdd.map(lambda (row, rowId): ([rowId + offset] + list(row)))

    return spark.createDataFrame(new_rdd, new_schema)

###
def file_to_df(df_name, file_path, header=True, delimiter='|', inferSchema=True, columns=None
               , cache=False, zipWithIndex=None, partitions=None, cluster_by=None
               , quote='"', escape='\\'
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
        :param partitions: number - if specified, will repartition to that number
        :param cluster_by: string - list of columns (comma separated) to run CLUSTER BY on
        :param quote: character - by default the quote character is ", but can be set to any character.
                      Delimiters inside quotes are ignored; set to '\0' to disable quoting
        :param escape: character - by default the escape character is \, but can be set to any character.
                      Escaped quote characters are ignored
    '''

    df = (sqlc.read.format('csv')
          .option('header', header)
          .option('delimiter', delimiter)
          .option('inferSchema', inferSchema)
          .option('quote', quote)
          .option('escape', escape)
          .load(file_path)
          )

    if columns:
        df = df.toDF(*columns)

    if zipWithIndex:
        # zipWithIndex has to happen before repartition()
        df = dfZipWithIndex(df, colName=zipWithIndex)

    if partitions:
        df = df.repartition(partitions)

    if cluster_by:
        before_cluster_by = df_name + '_before_cluster_by'
        df.registerTempTable(before_cluster_by)
        df = sqlc.sql("SELECT * FROM " + before_cluster_by + " CLUSTER BY " + cluster_by)
        spark.catalog.dropTempView(before_cluster_by)

    if cache:
        df = df.cache()

    df.registerTempTable(df_name)
    main_ns.globals()[df_name] = df

    return df

###
def sql_to_df(df_name, sql, cache=False, partitions=None):
    '''
        Runs an sql query and sets up a Spark dataframe

        :param df_name: registers this dataframe as a tempTable/view for SQL access;
                          important: it also registers a global variable under that name
        :param sql: Spark SQL query to runs
        :param partitions: number - if specified, will repartition to that number
        :param cache: cache this dataframe?
    '''

    df = sqlc.sql(sql)

    if partitions:
        df = df.repartition(partitions)

    if cache:
        df = df.cache()

    df.registerTempTable(df_name)
    main_ns.globals()[df_name] = df

    return df


###########################################################################################################
## below three are no implemented yet

def HDFSfileExists (file_path):
    '''
    Returns True if HDFS file exists

    :param file_path: file patch
    :return: boolean
    '''
    return True

def dropHDFSfile (file_path):
    '''
    Drop HDFS file

    :param file_path: HDFS file patch
    '''

def renameHDFSfile (src_name, dst_name):
    '''
    Renames src file to dst file name

    :param src_name: source name
    :param dst_name: target name
    '''

###########################################################################################################

def HDFScopyMerge (src_dir, dst_file, overwrite=False, deleteSource=False):
    
    """
    copyMerge() merges files from an HDFS directory to an HDFS files. 
    File names are sorted in alphabetical order for merge order.
    Inspired by https://hadoop.apache.org/docs/r2.7.1/api/src-html/org/apache/hadoop/fs/FileUtil.html#line.382

    :param src_dir: source directoy to get files from 
    :param dst_file: destination file to merge file to
    :param overwrite: overwrite destination file if already exists? 
    :param deleteSource: drop source directory after merge is complete
    """

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

    if not overwrite and HDFSfileExists(dst_file):
        dropHDFSfile(dst_file)

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
        fs.delete(hadoop.fs.Path(src_dir), True)    # True=recursive
        debug_print("Source directory {} removed.".format(src_dir))

    try:
        renameHDFSfile(dst_file_tmp, dst_file)
        debug_print("Temp file renamed to {}".format(dst_file))
    except:
        dropHDFSfile(dst_file_tmp)  # drop temp file if we can't rename it to target name
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

    out_stream = fs.create(hadoop.fs.Path(dst_file), overwrite)

    if appendEOL:
        content += "\n"

    try:
        out_stream.write(bytearray(content))
    finally:
        out_stream.close()


def dataframeToHDFSfile (dataframe, dst_file, overwrite=False
                         , header='true', delimiter=','
                         , quoteMode='MINIMAL'
                         , quote='"', escape='\\'
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
    """

    if not overwrite and HDFSfileExists(dst_file):
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


