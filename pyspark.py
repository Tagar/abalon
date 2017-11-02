
debug = False

###########################################################################################################

def file_to_df (df_name, file_path, header=True, delimiter='|', inferSchema=True, cache=False):
    '''
        Reads in a delimited file and sets up a Spark dataframe 
        
        :param df_name: registers this dataframe as a tempTable/view for SQL access;
                          important: it also registers a global variable under that name
        :param path: path to a file; local files have to have 'file://' prefix; for HDFS files prefix is 'hdfs://' (optional, as hdfs is default)
        :param header: boolean - file has a header record? 
        :param inferSchema: boolean - infer data types from data? (requires one extra pass over data)
        :param delimiter: one character
        :param cache: cache this dataframe?
    '''

    df = ( sqlc.read.format('csv')
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
    '''
        Runs an sql query and sets up a Spark dataframe 
        
        :param df_name: registers this dataframe as a tempTable/view for SQL access;
                          important: it also registers a global variable under that name
        :param sql: Spark SQL query to runs
        :param cache: cache this dataframe?
    '''

    df = sqlc.sql(sql)

    if cache:
        df = df.cache()

    df.createOrReplaceTempView(df_name)
    globals()[df_name] = df

###########################################################################################################

def copyMerge (src_dir, dst_file, overwrite=False, deleteSource=False):
    
    '''
    copyMerge() merges files from an HDFS directory to an HDFS files. 
    File names are sorted in alphabetical order for merge order.
    Inspired by https://hadoop.apache.org/docs/r2.7.1/api/src-html/org/apache/hadoop/fs/FileUtil.html#line.382

    :param src_dir: source directoy to get files from 
    :param dst_file: destination file to merge file to
    :param overwrite: overwrite destination file if already exists? 
    :param deleteSource: drop source directory after merge is complete
    '''
    
    def debug_print (message):
        if debug:
            print("copyMerge(): " + message)

    hadoop = sc._jvm.org.apache.hadoop
    conf = hadoop.conf.Configuration()
    fs = hadoop.fs.FileSystem.get(conf)

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
        for file in files:
            debug_print("Appending file {} into {}".format(file, dst_file))

            in_stream = fs.open(file)   # InputStream object
            try:
                hadoop.io.IOUtils.copyBytes(in_stream, out_stream, conf, False)     # False means don't close out_stream
            finally:
                in_stream.close()
    finally:
        out_stream.close()

    if deleteSource:
        fs.delete(hadoop.fs.Path(src_dir), True)    # True=recursive
        debug_print("Source directory {} removed.".format(src_dir))


def writeString (dst_file, content, overwrite=True):
    
    '''
    Creates an HDFS file with given content
        
    :param dst_file: destination file to merge file to
    :param content: string to be written to the file
    :param overwrite: overwrite target file?
    '''
    
    hadoop = sc._jvm.org.apache.hadoop
    conf = hadoop.conf.Configuration()
    fs = hadoop.fs.FileSystem.get(conf)

    out_stream = fs.create(hadoop.fs.Path(dst_file), overwrite)
    
    try:
        out_stream.write(bytearray(content))
    finally:
        out_stream.close()


def dataframeToTextFile (dataframe, dst_file, overwrite=False,
                            header='true', delimiter=',', quoteMode='MINIMAL'):

    '''
    dataframeToTextFile() saves a dataframe as a delimited file. 
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
    '''

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
        writeString(header_filename, header_record)
        
    copyMerge(dst_dir, dst_file, overwrite, deleteSource=True)
    

