
# FIXME:
from .sparkutils import sparkutils_init
if 'spark' in globals():
    # assume global variable 'spark' references SparkSession
    sparkutils_init(globals()['spark'])

