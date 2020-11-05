from ipystate.impl.dispatch.dispatcher import Dispatcher
import pandas as pd
import pyarrow as pa


class _DataFrameParquetAdapter:
    """
    Handles limitations of parquet format
    """

    PREFIX = 'pq_converted'
    PREFIX_INT = PREFIX + ':' + 'int' + ':'

    @staticmethod
    def _all_str(cols) -> bool:
        for c in cols:
            if type(c) is not str:
                return False
        return True

    @staticmethod
    def _is_pq_converted(cols) -> bool:
        for c in cols:
            if isinstance(c, str) and c.startswith(_DataFrameParquetAdapter.PREFIX):
                return True
        return False

    @staticmethod
    def prepare_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
        df1 = df.copy(deep=False)
        if not _DataFrameParquetAdapter._all_str(df1):
            cols = []
            for c in df.columns:
                typed_name = str(c)
                if isinstance(c, int):
                    typed_name = _DataFrameParquetAdapter.PREFIX_INT + str(c)
                cols.append(typed_name)
            df1.columns = cols
        return df1

    @staticmethod
    def restore_after_parquet(df: pd.DataFrame) -> pd.DataFrame:
        df1 = df
        if _DataFrameParquetAdapter._is_pq_converted(df.columns):
            cols = []
            for c in df.columns:
                if isinstance(c, str) and c.startswith(_DataFrameParquetAdapter.PREFIX_INT):
                    try:
                        ci = int(c[len(_DataFrameParquetAdapter.PREFIX_INT):])
                        cols.append(ci)
                    except:
                        cols.append(c)
                else:
                    cols.append(c)
            df1.columns = cols
        return df1


class DataframeDispatcher(Dispatcher):
    @staticmethod
    def _reduce_dataframe(df):
        def create_df(bb) -> pd.DataFrame:
            br = pa.BufferReader(bb)
            df_ = pd.read_parquet(br, engine='pyarrow')
            return _DataFrameParquetAdapter.restore_after_parquet(df_)

        buffer_os = pa.BufferOutputStream()
        df1 = _DataFrameParquetAdapter.prepare_for_parquet(df)
        df1.to_parquet(buffer_os, engine='pyarrow')
        buffer = buffer_os.getvalue()
        return create_df, (buffer,)

    def register(self, dispatch):
        dispatch[pd.DataFrame] = self._reduce_dataframe
