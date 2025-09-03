import hashlib
import json
import pickle
from snowflake.ml.feature_store import FeatureView
from snowflake.snowpark import DataFrame

def version_featureview(feature_view:FeatureView, keys:list=[]) -> str:

    """Computes an md5 hash of a feature view's query and the key/value of given metadata fields. 
    This hash can be used as the version when registering the feature view.
    By default, the function only includes the query in the feature view definition. 
    Include additional keys that "matter" to the versioned definition in the keys argument."""

    allow = [
        'name','entities','timestamp_col','desc','feature_desc',
        'refresh_freq','database','schema','initialize','warehouse',
        'refresh_mode','refresh_mode_reason','owner','cluster_by'
    ]
    bad_keys = list(set(keys) - set(allow))
    if bad_keys:
        raise KeyError(str(bad_keys) + " are not allowable keys for versioning")
    to_hash = keys + ['query']
    data = {k:str(getattr(feature_view, "_"+k)) for k in to_hash}
    return hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest().upper()

def version_data(df:DataFrame) -> str:
    
    """Computes an md5 hash on the data itself to be used as a dataset version."""

    return hashlib.md5(str(df.select_expr("HASH_AGG(*)")).encode('utf-8')).hexdigest().upper()

def version_model(model) -> str:

    """Computes an md5 hash of the pickled model object to be used as a model version. 
    V_ is prepended to the hash to meet model registry version name requirements."""

    return "V_"+hashlib.md5(pickle.dumps(model)).hexdigest().upper()