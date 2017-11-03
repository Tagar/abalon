

###########################################################################################################
# module variables:

debug = False

(spark, sc) = (None, None)                  # SparkSession and SparkContext

(hadoop, conf, fs) = (None, None, None)     # see desciption below in sparkutils_init()


###########################################################################################################


def sparkutils_init (i_spark, i_debug=False):
    '''
    Initialize module-level variables

    :param i_spark: an object of pyspark.sql.session.SparkSession
    :param i_debug: debug output of the below functions?
    '''

    from pyspark.sql.session import SparkSession
    if not isinstance(spark, pyspark.sql.session.SparkSession):
        raise TypeError("spark parameter should be of type SparkSession")

    global spark, debug
    (spark, debug) = (i_spark, i_debug)

    global sc
    sc = spark.sparkContext

    global hadoop, conf, fs
    hadoop = sc._jvm.org.apache.hadoop      # Get a reference to org.apache.hadoop through py4j object
    conf = hadoop.conf.Configuration()      # Create Configuration object
                                            #   see - shttps://hadoop.apache.org/docs/r2.6.4/api/org/apache/hadoop/conf/Configuration.html
    fs = hadoop.fs.FileSystem.get(conf)     # get FileSystem object
                                            #   see - https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html#get(org.apache.hadoop.conf.Configuration)


###########################################################################################################


def file_to_df (df_name, file_path, header=True, delimiter='|', inferSchema=True, cache=False):

    """
        Reads in a delimited file and sets up a Spark dataframe 
        
        :param df_name: registers this dataframe as a tempTable/view for SQL access;
                          important: it also registers a global variable under that name
        :param file_path: path to a file; local files have to have 'file://' prefix; for HDFS files prefix is 'hdfs://' (optional, as hdfs is default)
        :param header: boolean - file has a header record? 
        :param inferSchema: boolean - infer data types from data? (requires one extra pass over data)
        :param delimiter: one character
        :param cache: cache this dataframe?
    """

    df = ( spark.read.format('csv')
              .option('header', header)
              .option('delimiter', delimiter)
              .option('inferSchema', inferSchema)
              .load(file_path)
        )

    if cache:
        df = df.cache()

    df.createOrReplaceTempView(df_name)
    globals()[df_name] = df


def sql_to_df (df_name, sql, cache=False):

    """
        Runs an sql query and sets up a Spark dataframe 
        
        :param df_name: registers this dataframe as a tempTable/view for SQL access;
                          important: it also registers a global variable under that name
        :param sql: Spark SQL query to runs
        :param cache: cache this dataframe?
    """

    df = spark.sql(sql)

    if cache:
        df = df.cache()

    df.createOrReplaceTempView(df_name)
    globals()[df_name] = df

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
    files.sort()

    # dst_permission = hadoop.fs.permission.FsPermission.valueOf(permission)      # , permission='-rw-r-----'
    out_stream = fs.create(hadoop.fs.Path(dst_file), overwrite)

    try:
        # loop over files in alphabetical order and append them one by one to the target file
        for filename in files:
            debug_print("Appending file {} into {}".format(filename, dst_file))

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


def HDFSwriteString (dst_file, content, overwrite=True):
    
    """
    Creates an HDFS file with given content.
    Notice this is usable only for small (metadata like) files.

    :param dst_file: destination HDFS file to write to
    :param content: string to be written to the file
    :param overwrite: overwrite target file?
    """

    out_stream = fs.create(hadoop.fs.Path(dst_file), overwrite)
    
    try:
        out_stream.write(bytearray(content))
    finally:
        out_stream.close()


def dataframeToHDFSfile (dataframe, dst_file, overwrite=False,
                            header='true', delimiter=',', quoteMode='MINIMAL'):

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
    :param quoteMode: https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/QuoteMode.html 
    """

    dst_dir = dst_file + '.tmpdir'

    (dataframe
        .write
        .option('header', header)
        .option('delimiter', delimiter)
        .option('quoteMode', quoteMode)
        .mode('overwrite')     # temp directory will always be overwritten
        .csv(dst_dir)
        )

    if header:
        # we will create a separate file with just a header record
        header_record = delimiter.join(dataframe.columns)
        header_filename = "{}/__00_header.csv".format(dst_dir)  # have to make sure header filename is 1st in
                                                                # alphabetical order
        HDFSwriteString(header_filename, header_record)

    HDFScopyMerge(dst_dir, dst_file, overwrite, deleteSource=True)


