import numpy as np
import pandas as pd
import pandas.api.types as pd_types
import xxhash
import traceback


class Hasher:
    def has_changed(self, name: str, value: object):
        pass

    def update_hash(self, name: str, value: object):
        pass


class HasherImpl(Hasher):
    identity = lambda x: x

    dispatch = {
        int: identity,
        bool: identity,
        float: identity,
        type(None): hash,
        str: hash
    }

    def __init__(self):
        self._hashes = dict()

    def has_changed(self, name: str, value: object):
        try:
            if name not in self._hashes:
                return True
            h = HasherImpl.hash(value)
            return self._hashes[name] != h
        except TypeError as e:
            # print(f"an error occurred when hashing {name}: {e}")
            # traceback.print_exc()
            return True

    def update_hash(self, name: str, value: object):
        try:
            self._hashes[name] = HasherImpl.hash(value)
            # print(f"hashing {name} as {self._hashes[name]}")
        except TypeError as e:
            # print(f"an error occurred when hashing {name}: {e}")
            # traceback.print_exc()
            pass

    @staticmethod
    def hash(value: object) -> object:
        hash_fun = HasherImpl.dispatch.get(type(value))
        if hash_fun is not None:
            return hash_fun(value)
        raise TypeError(f"unsupported type: {type(value)}")

    @staticmethod
    def hash_df(df: pd.DataFrame):
        xx = xxhash.xxh3_64()
        xx.update(df.columns.values)
        xx.update(df.index.values)
        for c in df.columns:
            na = df[c].values
            if pd_types.is_categorical_dtype(df[c].dtype):
                na = df[c].factorize()[0]

            if na is not None:
                if not na.flags.contiguous:
                    raise TypeError("not C-contiguous column in dataframe")
                xx.update(na.data)

        h = xx.digest()
        return h.hex()

    dispatch[pd.DataFrame] = hash_df.__func__

    @staticmethod
    def hash_np_array(arr: np.ndarray):
        if arr.dtype == object:
            raise TypeError(f"dtype {arr.dtype} is not supported")
        try:
            xx = xxhash.xxh3_64()
            xx.update(arr)
            h = xx.digest()
            return h.hex()
        except Exception as e:
            raise TypeError(f"failed to hash {np.ndarray} due to {str(e)}")

    dispatch[np.ndarray] = hash_np_array.__func__
