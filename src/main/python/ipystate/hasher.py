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
    def __init__(self):
        self._hashes = dict()

    def has_changed(self, name: str, value: object):
        try:
            if name not in self._hashes:
                return True
            h = HasherImpl.hash(value)
            return self._hashes[name] != h
        except TypeError as e:
            print(f"an error occurred when hashing {name}: {e}")
            traceback.print_exc()
            return True

    def update_hash(self, name: str, value: object):
        try:
            self._hashes[name] = Hasher.hash(value)
            print(f"hashing {name} as {self._hashes[name]}")
        except TypeError as e:
            print(f"an error occurred when hashing {name}: {e}")
            traceback.print_exc()

    @staticmethod
    def hash(value: object) -> object:
        for primitive in [bool, str, int, float, np.dtype]:
            if isinstance(value, primitive):
                return hash(value)

        if isinstance(value, pd.DataFrame):
            return HasherImpl.hash_df(value)
        raise TypeError(f"unsupported type: {type(value)}")

    @staticmethod
    def hash_df(df):
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
