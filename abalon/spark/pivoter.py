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

'''
    Pivots a dataframe.

    Shows very good performance on very wide datasets
    (tested on 50 billion rows -> pivoted to 62m records x ~6000 columns).

    Spark's pivot API call has O(M * N) time complexity,
    where M is number of rows and N is number of columns.
    So it's particularly slow for very wide datasets.

    Methods in this class has time complexity close to O(M * log2(P)),
    where P is avg number of populated columns. P always <= N.
    So it's faster than Spark's pivot() for dense datasets too, but
    performance is even better for sparser datasets.
    A small grain of salt here is the implementation is in PySpark
    and not in Scala obviously.

    Dataframe has to have exactly three columns in this order:
    1. index column     (string) - pivot on this column (elements groupped by this column before pivoting)
    2. colname column   (string, has to conform to SQL limitations of SQL column names)
    3. value column     (float)
    (main pivoting logic happens at RDD level)

    Limitations:
    - value column is assumed to be float/double data type
'''


###########################################################################################################

from abalon.spark.sparkutils import get_spark
from pyspark.sql.types import *
import operator

###########################################################################################################


def BasicSparkPivoter (df, all_vars=None):

    '''
    Pivots a dataframe without aggregation.

    Limitations:
    - {index, colname} is a unique/ "PK" for this dataset
        (there is no aggregation happens for value - use AggSparkPivoter instead if this is needed)

    :param df: dataframe to pivot (see expected schema of the df above)
    :param all_vars: list of all distinct values of `colname` column;
            if not specified, datset will be scanned for all possible colnames;
            the only reason it's passed to this function is so you can redefine order of pivoted columns;
    :return: resulting dataframe
    '''

    def simple_merge_two_dicts(x, y, agg_op):
        x.update(y)  # modifies x with y's keys and values & returns None
        return x

    return pivot_df(df, simple_merge_two_dicts, all_vars)


def AggSparkPivoter (df, all_vars=None, agg_op=operator.add):

    '''
    Pivots a dataframe with aggregation.

    :param df: dataframe to pivot (see expected schema of the df above)
    :param all_vars: list of all distinct values of `colname` column;
            if not specified, datset will be scanned for all possible colnames;
            the only reason it's passed to this function is so you can redefine order of pivoted columns;
    :return: resulting dataframe
    '''

    def agg_merge_two_dicts(x, y, agg_op):
        return {k: agg_op(x.get(k, 0.0),
                          y.get(k, 0.0))
                for k in set(x).union(y)
                }

    return pivot_df(df, agg_merge_two_dicts, all_vars, agg_op)


###########################################################################################################

def pivot_df (df, merger_func, all_vars=None, agg_op=None):

    def map_dict_to_denseArray(idx, d):
        yield idx
        for var in all_vars:
            if var in d:
                yield float(d[var])  # assuming all variables can be cast to float/double
            else:
                yield None  # this is what makes array 'dense'.. even non-existent vars are represented with nulls

    spark = get_spark()

    # assuming 2nd column is index column
    idx_col = df.columns[1]

    if not all_vars:
        # get list of variables from the dataset:
        all_vars = sorted([row[0] for row in df.select(idx_col).distinct().collect()])

    pivoted_rdd = (df.rdd
        .map(lambda (idx, k, v): (idx, {k: v}))  # convert k,v to a 1-element dict
        .reduceByKey(lambda x,y: merger_func(x, y, agg_op))  # merge into a single dict for all vars for this idx
        .map(lambda (idx, d): list(map_dict_to_denseArray(idx, d)))
                                        # create final rdd with dense array of all variables
    )

    fields =  [StructField(idx_col,    StringType(), False)]
    fields += [StructField(field_name, DoubleType(), True) for field_name in all_vars]

    schema = StructType(fields)

    pivoted_df = spark.createDataFrame(pivoted_rdd, schema)
    return pivoted_df


