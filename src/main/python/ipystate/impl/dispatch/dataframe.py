import cloudpickle

from ipystate.impl.dispatch.dispatcher import Dispatcher
import pandas as pd
import pyarrow as pa


class _DataFrameParquetAdapter:
    """
    Handles limitations of parquet format
    """

    PREFIX = 'pq_converted'
    PREFIX_INT = PREFIX + ':' + 'int' + ':'
    PREFIX_TUPLE = PREFIX + ':' + 'tuple' + ':'

    @staticmethod
    def _all_str(cols) -> bool:
        return all(isinstance(c, str) for c in cols)

    @staticmethod
    def _is_pq_converted(cols) -> bool:
        for c in cols:
            if isinstance(c, str) and c.startswith(_DataFrameParquetAdapter.PREFIX):
                return True
        return False

    @staticmethod
    def _is_multi_index(cols) -> bool:
        return all(isinstance(c, str) and c.startswith(_DataFrameParquetAdapter.PREFIX_TUPLE) for c in cols)

    @staticmethod
    def prepare_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
        df1 = df.copy(deep=False)
        if not _DataFrameParquetAdapter._all_str(df1):
            cols = []
            for c in df.columns:
                if isinstance(c, int):
                    typed_name = _DataFrameParquetAdapter.PREFIX_INT + str(c)
                elif isinstance(c, tuple):
                    typed_name = _DataFrameParquetAdapter.PREFIX_TUPLE + repr(c)
                else:
                    typed_name = str(c)
                cols.append(typed_name)
            df1.columns = cols
        return df1

    @staticmethod
    def restore_after_parquet(df: pd.DataFrame) -> pd.DataFrame:
        df1 = df
        if _DataFrameParquetAdapter._is_pq_converted(df.columns):
            cols = []
            for c in df.columns:
                try:
                    if isinstance(c, str) and c.startswith(_DataFrameParquetAdapter.PREFIX_INT):
                        restored = int(c[len(_DataFrameParquetAdapter.PREFIX_INT):])
                    elif isinstance(c, str) and c.startswith(_DataFrameParquetAdapter.PREFIX_TUPLE):
                        restored = eval(c[len(_DataFrameParquetAdapter.PREFIX_TUPLE):])
                    else:
                        restored = c
                    cols.append(restored)
                except:
                    cols.append(c)

            if _DataFrameParquetAdapter._is_multi_index(df.columns):
                df1.columns = pd.MultiIndex.from_tuples(cols)
            else:
                df1.columns = cols
        return df1


class DataframeDispatcher(Dispatcher):
    @staticmethod
    def _pickle_reduce(df):
        reduce = getattr(df, "__reduce_ex__", None)
        return reduce(cloudpickle.DEFAULT_PROTOCOL)

    @staticmethod
    def _create_df(bb) -> pd.DataFrame:
        br = pa.BufferReader(bb)
        df = pd.read_parquet(br, engine='pyarrow')
        return _DataFrameParquetAdapter.restore_after_parquet(df)

    @staticmethod
    def _reduce_dataframe(df):
        try:
            buffer_os = pa.BufferOutputStream()
            df1 = _DataFrameParquetAdapter.prepare_for_parquet(df)
            df1.to_parquet(buffer_os, engine='pyarrow')
            buffer = buffer_os.getvalue()
            return DataframeDispatcher._create_df, (buffer,)
        except:
            return DataframeDispatcher._pickle_reduce(df)

    def register(self, dispatch):
        dispatch[pd.DataFrame] = self._reduce_dataframe
