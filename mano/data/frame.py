import numpy as np
import pandas as pd


@pd.api.extensions.register_dataframe_accessor("utils")
class DataframeAccessor:

    def __init__(self, df):
        self._validate(df)
        self.df = df

    def _validate(self, df):
        if not isinstance(df, pd.DataFrame):
            raise ValueError('Fluor is expecting a pandas dataframe, and not a {}'.format(type(df)))

    def summary(self, width=120, minimal=True):
        DataFrameAnalyzer(width, minimal).summary(self.df)

    def to_categoricals(self):
        for c in self.df.columns:
            if pd.api.types.is_object_dtype(self.df[c]):
                n = self.df[c].nunique()
                if n < 0.5 * len(self.df):
                    self.df[c] = pd.Categorical(self.df[c])
        return self.df

    def downcast_int_columns(self):
        for c in self.df.columns:
            if pd.api.types.is_integer_dtype(self.df[c]):
                self.df[c] = downcast_int(self.df[c])
        return self.df


def downcast_int(series):
    int_types = [np.uint8, np.int8, np.uint16, np.int16, np.uint32, np.int32, np.uint64, np.int64]
    if not pd.api.types.is_integer_dtype(series):
        return series
    vmin = series.min()
    vmax = series.max()
    for int_type in int_types:
        if np.iinfo(int_type).min <= vmin and vmax <= np.iinfo(int_type).max:
            return series.astype(int_type)
    return series


class DataFrameAnalyzer:

    def __init__(self, width=120, minimal=True):
        self.width = width
        self.minimal = minimal

    def _build(self, df):
        self.rows, self.columns = df.shape
        self.space_to_display_number_of_rows = len(str(self.rows))
        self.column_name_width = max([len(str(c)) for c in df.columns])

        self.header = '{}column | {}nulls | {}uniques | type           | description{}'.format(
            ' ' * max(self.column_name_width - 6, 0),
            ' ' * max(self.space_to_display_number_of_rows - 5, 0),
            ' ' * max(self.space_to_display_number_of_rows - 7, 0),
            ' ' * self.width
        )[:self.width]

        self.row_format = '%{}s | %{}d | %{}d | %14s | %s'.format(
            max(self.column_name_width, 6),
            max(self.space_to_display_number_of_rows, 5),
            max(self.space_to_display_number_of_rows, 7)
        )

        self.description_width = len(self.header.split('|')[-1]) - 1

    def summary(self, df):
        self._build(df)
        print('{:,} rows x {:,} columns'.format(self.rows, self.columns))
        print(self.header)
        print('-' * self.width)
        for column in df.columns:
            column_name = str(column)
            n_nulls = df[column].isnull().sum()
            n_unique = df[column].nunique()
            description = self._get_description(df[column])
            row = self.row_format % (column_name, n_nulls, n_unique, df[column].dtype, description)
            print(row[:self.width])

    def _get_description(self, series):
        if pd.api.types.is_float_dtype(series):
            return self._get_float_description(series)
        elif pd.api.types.is_datetime64_any_dtype(series):
            return self._get_date_description(series)
        else:
            return self._get_category_description(series)

    def _get_float_description(self, series):
        smin = series.min()
        smax = series.max()
        smed = series.median()
        description = '[min={:.1f}, q50%={:.1f}, max={:.1f}]'.format(smin, smed, smax)
        if len(description) > self.description_width:
            description = 'q50%={:.1f}'.format(smed)
        if len(description) > self.description_width:
            description = ''
        return description

    def _get_date_description(self, series):
        dmin = series.min()
        dmax = series.max()
        description = '[{} to {}]'.format(dmin, dmax)
        if len(description) > self.description_width:
            description = ''
        return description

    def _get_category_description(self, series):
        description = ''
        values = series.value_counts()
        for i, v in values.items():
            value_description = '{} ({:.0%})'.format(i, v / self.rows)
            if description == '':
                if len(value_description) <= self.description_width:
                    description = value_description
                else:
                    break
            else:
                if len(description) + len(value_description) <= self.description_width - 2:
                    description += ', ' + value_description
                else:
                    break
        return description