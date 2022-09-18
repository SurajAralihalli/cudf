# Copyright (c) 2019-2022, NVIDIA CORPORATION.

import textwrap

import cupy as cp
import numpy as np
import pandas as pd
import pytest
from hypothesis import given, settings, strategies as st

import cudf
from cudf.core._compat import PANDAS_GE_110
from cudf.testing import _utils as utils
from cudf.utils.dtypes import np_dtypes_to_pandas_dtypes

repr_categories = [
    "uint16",
    "int64",
    "float64",
    "str",
    "category",
    "datetime64[ns]",
]


@pytest.mark.parametrize("dtype", repr_categories)
@pytest.mark.parametrize("nrows", [0, 5, 10])
def test_null_series(nrows, dtype):
    size = 5
    sr = cudf.Series(np.random.randint(1, 9, size)).astype(dtype)
    sr[np.random.choice([False, True], size=size)] = None
    if dtype != "category" and cudf.dtype(dtype).kind in {"u", "i"}:
        ps = pd.Series(
            sr._column.data_array_view.copy_to_host(),
            dtype=np_dtypes_to_pandas_dtypes.get(
                cudf.dtype(dtype), cudf.dtype(dtype)
            ),
        )
        ps[sr.isnull().to_pandas()] = pd.NA
    else:
        ps = sr.to_pandas()

    pd.options.display.max_rows = int(nrows)
    psrepr = repr(ps)
    psrepr = psrepr.replace("NaN", "<NA>")
    psrepr = psrepr.replace("NaT", "<NA>")
    psrepr = psrepr.replace("None", "<NA>")
    if "UInt" in psrepr:
        psrepr = psrepr.replace("UInt", "uint")
    elif "Int" in psrepr:
        psrepr = psrepr.replace("Int", "int")
    assert psrepr.split() == repr(sr).split()
    pd.reset_option("display.max_rows")


dtype_categories = [
    "float32",
    "float64",
    "datetime64[ns]",
    "str",
    "category",
]


@pytest.mark.parametrize("ncols", [1, 2, 3, 4, 5, 10])
def test_null_dataframe(ncols):
    size = 20
    gdf = cudf.DataFrame()
    for idx, dtype in enumerate(dtype_categories):
        sr = cudf.Series(np.random.randint(0, 128, size)).astype(dtype)
        sr[np.random.choice([False, True], size=size)] = None
        gdf[dtype] = sr
    pdf = gdf.to_pandas()
    pd.options.display.max_columns = int(ncols)
    pdf_repr = (
        repr(pdf)
        .replace("NaN", "<NA>")
        .replace("NaT", "<NA>")
        .replace("None", "<NA>")
    )
    assert pdf_repr.split() == repr(gdf).split()
    pd.reset_option("display.max_columns")

def _assert_date_series_repr(ps, gs):
    """
    This is a utility function to compare pandas & cudf
    datetime series repr's.
    The repr's differ in the way text is spaced:

    >>> s = cudf.Series([100, 200, 300], dtype='datetime64[ns]')
    >>> s
    0    1970-01-01 00:00:00.000000100
    1    1970-01-01 00:00:00.000000200
    2    1970-01-01 00:00:00.000000300
    dtype: datetime64[ns]

    >>> s.to_pandas()
    0   1970-01-01 00:00:00.000000100
    1   1970-01-01 00:00:00.000000200
    2   1970-01-01 00:00:00.000000300
    dtype: datetime64[ns]
    """
    expected_list = repr(ps).split("\n")
    actual_list = repr(gs).split("\n")

    new_actual_list = []
    for text in actual_list:
        if not text.startswith(" "):
            new_actual_list.append(text.replace("  ", " ", 1))
        else:
            if "..." in text:
                new_actual_list.append(" ".join(text.rsplit("  ", 1)))
            else:
                new_actual_list.append(text)
    # import pdb;pdb.set_trace()
    assert expected_list == new_actual_list

@pytest.mark.parametrize("dtype", repr_categories)
@pytest.mark.parametrize("nrows", [None, 0, 1, 2, 9, 10, 11, 19, 20, 21])
def test_full_series(nrows, dtype):
    np.random.seed(0)
    size = 20
    ps = pd.Series(np.random.randint(0, 100, size)).astype(dtype)
    sr = cudf.from_pandas(ps)
    pd.options.display.max_rows = nrows

    if cudf.api.types.is_datetime_dtype(dtype):
        _assert_date_series_repr(ps , sr)
    else:
        assert repr(ps) == repr(sr)
    pd.reset_option("display.max_rows")


@pytest.mark.parametrize("nrows", [5, 10, 15])
@pytest.mark.parametrize("ncols", [5, 10, 15])
@pytest.mark.parametrize("size", [20, 21])
@pytest.mark.parametrize("dtype", sorted(list(set(repr_categories) - {"datetime64[ns]"})))
def test_full_dataframe_20(dtype, size, nrows, ncols):
    pdf = pd.DataFrame(
        {idx: np.random.randint(0, 100, size) for idx in range(size)}
    ).astype(dtype)
    gdf = cudf.from_pandas(pdf)

    with pd.option_context(
        "display.max_rows", int(nrows), "display.max_columns", int(ncols)
    ):
        assert repr(pdf) == repr(gdf)
        assert pdf._repr_html_() == gdf._repr_html_()
        assert pdf._repr_latex_() == gdf._repr_latex_()


@pytest.mark.parametrize("nrows,ncols,data,expected_repr",[
    (
        5,
        5, 
    {idx: np.arange(0, 100, 5) for idx in range(20)}, 
    textwrap.dedent(
        """
                               0                              1   ...  \\
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000  ...   
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005  ...   
..                            ...                            ...  ...   
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090  ...   
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095  ...   

                               18                             19  
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000  
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005  
..                            ...                            ...  
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090  
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095  

[20 rows x 20 columns]
"""
    )),
    (5, 15, 
    {idx: np.arange(0, 100, 5) for idx in range(20)}, 
    textwrap.dedent(
        """
                               0                              1   \\
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005   
..                            ...                            ...   
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095   

                               2                              3   \\
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005   
..                            ...                            ...   
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095   

                               4                              5   \\
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005   
..                            ...                            ...   
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095   

                               6   ...                             13  \\
0   1970-01-01 00:00:00.000000000  ...  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  ...  1970-01-01 00:00:00.000000005   
..                            ...  ...                            ...   
18  1970-01-01 00:00:00.000000090  ...  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  ...  1970-01-01 00:00:00.000000095   

                               14                             15  \\
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005   
..                            ...                            ...   
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095   

                               16                             17  \\
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005   
..                            ...                            ...   
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095   

                               18                             19  
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000  
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005  
..                            ...                            ...  
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090  
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095  

[20 rows x 20 columns]
"""
    )),
    (15, 15, 
    {idx: np.arange(0, 100, 5) for idx in range(20)}, 
    textwrap.dedent(
        """
                               0                              1   \\
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005   
2   1970-01-01 00:00:00.000000010  1970-01-01 00:00:00.000000010   
3   1970-01-01 00:00:00.000000015  1970-01-01 00:00:00.000000015   
4   1970-01-01 00:00:00.000000020  1970-01-01 00:00:00.000000020   
..                            ...                            ...   
15  1970-01-01 00:00:00.000000075  1970-01-01 00:00:00.000000075   
16  1970-01-01 00:00:00.000000080  1970-01-01 00:00:00.000000080   
17  1970-01-01 00:00:00.000000085  1970-01-01 00:00:00.000000085   
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095   

                               2                              3   \\
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005   
2   1970-01-01 00:00:00.000000010  1970-01-01 00:00:00.000000010   
3   1970-01-01 00:00:00.000000015  1970-01-01 00:00:00.000000015   
4   1970-01-01 00:00:00.000000020  1970-01-01 00:00:00.000000020   
..                            ...                            ...   
15  1970-01-01 00:00:00.000000075  1970-01-01 00:00:00.000000075   
16  1970-01-01 00:00:00.000000080  1970-01-01 00:00:00.000000080   
17  1970-01-01 00:00:00.000000085  1970-01-01 00:00:00.000000085   
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095   

                               4                              5   \\
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005   
2   1970-01-01 00:00:00.000000010  1970-01-01 00:00:00.000000010   
3   1970-01-01 00:00:00.000000015  1970-01-01 00:00:00.000000015   
4   1970-01-01 00:00:00.000000020  1970-01-01 00:00:00.000000020   
..                            ...                            ...   
15  1970-01-01 00:00:00.000000075  1970-01-01 00:00:00.000000075   
16  1970-01-01 00:00:00.000000080  1970-01-01 00:00:00.000000080   
17  1970-01-01 00:00:00.000000085  1970-01-01 00:00:00.000000085   
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095   

                               6   ...                             13  \\
0   1970-01-01 00:00:00.000000000  ...  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  ...  1970-01-01 00:00:00.000000005   
2   1970-01-01 00:00:00.000000010  ...  1970-01-01 00:00:00.000000010   
3   1970-01-01 00:00:00.000000015  ...  1970-01-01 00:00:00.000000015   
4   1970-01-01 00:00:00.000000020  ...  1970-01-01 00:00:00.000000020   
..                            ...  ...                            ...   
15  1970-01-01 00:00:00.000000075  ...  1970-01-01 00:00:00.000000075   
16  1970-01-01 00:00:00.000000080  ...  1970-01-01 00:00:00.000000080   
17  1970-01-01 00:00:00.000000085  ...  1970-01-01 00:00:00.000000085   
18  1970-01-01 00:00:00.000000090  ...  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  ...  1970-01-01 00:00:00.000000095   

                               14                             15  \\
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005   
2   1970-01-01 00:00:00.000000010  1970-01-01 00:00:00.000000010   
3   1970-01-01 00:00:00.000000015  1970-01-01 00:00:00.000000015   
4   1970-01-01 00:00:00.000000020  1970-01-01 00:00:00.000000020   
..                            ...                            ...   
15  1970-01-01 00:00:00.000000075  1970-01-01 00:00:00.000000075   
16  1970-01-01 00:00:00.000000080  1970-01-01 00:00:00.000000080   
17  1970-01-01 00:00:00.000000085  1970-01-01 00:00:00.000000085   
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095   

                               16                             17  \\
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000   
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005   
2   1970-01-01 00:00:00.000000010  1970-01-01 00:00:00.000000010   
3   1970-01-01 00:00:00.000000015  1970-01-01 00:00:00.000000015   
4   1970-01-01 00:00:00.000000020  1970-01-01 00:00:00.000000020   
..                            ...                            ...   
15  1970-01-01 00:00:00.000000075  1970-01-01 00:00:00.000000075   
16  1970-01-01 00:00:00.000000080  1970-01-01 00:00:00.000000080   
17  1970-01-01 00:00:00.000000085  1970-01-01 00:00:00.000000085   
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090   
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095   

                               18                             19  
0   1970-01-01 00:00:00.000000000  1970-01-01 00:00:00.000000000  
1   1970-01-01 00:00:00.000000005  1970-01-01 00:00:00.000000005  
2   1970-01-01 00:00:00.000000010  1970-01-01 00:00:00.000000010  
3   1970-01-01 00:00:00.000000015  1970-01-01 00:00:00.000000015  
4   1970-01-01 00:00:00.000000020  1970-01-01 00:00:00.000000020  
..                            ...                            ...  
15  1970-01-01 00:00:00.000000075  1970-01-01 00:00:00.000000075  
16  1970-01-01 00:00:00.000000080  1970-01-01 00:00:00.000000080  
17  1970-01-01 00:00:00.000000085  1970-01-01 00:00:00.000000085  
18  1970-01-01 00:00:00.000000090  1970-01-01 00:00:00.000000090  
19  1970-01-01 00:00:00.000000095  1970-01-01 00:00:00.000000095  

[20 rows x 20 columns]
"""
    ),
        )
])
def test_full_datetime_dataframe(nrows, ncols, data, expected_repr):
    pdf = pd.DataFrame(
        data    
    ).astype('datetime64[ns]')
    gdf = cudf.from_pandas(pdf)
    

    with pd.option_context(
        "display.max_rows", int(nrows), "display.max_columns", int(ncols)
    ):
        assert expected_repr.split() == repr(gdf).split()
        assert pdf._repr_html_() == gdf._repr_html_()
        assert pdf._repr_latex_() == gdf._repr_latex_()


@given(
    st.lists(
        st.integers(-9223372036854775808, 9223372036854775807),
        min_size=1,
        max_size=10000,
    )
)
@settings(deadline=None)
def test_integer_dataframe(x):
    gdf = cudf.DataFrame({"x": x})
    pdf = gdf.to_pandas()
    pd.options.display.max_columns = 1
    assert repr(gdf) == repr(pdf)
    assert repr(gdf.T) == repr(pdf.T)
    pd.reset_option("display.max_columns")


@given(
    st.lists(
        st.integers(-9223372036854775808, 9223372036854775807), max_size=10000
    )
)
@settings(deadline=None)
def test_integer_series(x):
    sr = cudf.Series(x)
    ps = pd.Series(data=x)

    assert repr(sr) == repr(ps)


@given(st.lists(st.floats()))
@settings(deadline=None)
def test_float_dataframe(x):
    gdf = cudf.DataFrame({"x": cudf.Series(x, nan_as_null=False)})
    pdf = gdf.to_pandas()
    assert repr(gdf) == repr(pdf)


@given(st.lists(st.floats()))
@settings(deadline=None)
def test_float_series(x):
    sr = cudf.Series(x, nan_as_null=False)
    ps = pd.Series(data=x)
    assert repr(sr) == repr(ps)


@pytest.fixture
def mixed_pdf():
    pdf = pd.DataFrame()
    pdf["Integer"] = np.array([2345, 11987, 9027, 9027])
    pdf["Date"] = np.array(
        ["18/04/1995", "14/07/1994", "07/06/2006", "16/09/2005"]
    )
    pdf["Float"] = np.array([9.001, 8.343, 6, 2.781])
    pdf["Integer2"] = np.array([2345, 106, 2088, 789277])
    pdf["Category"] = np.array(["M", "F", "F", "F"])
    pdf["String"] = np.array(["Alpha", "Beta", "Gamma", "Delta"])
    pdf["Boolean"] = np.array([True, False, True, False])
    return pdf


@pytest.fixture
def mixed_gdf(mixed_pdf):
    return cudf.from_pandas(mixed_pdf)


def test_mixed_dataframe(mixed_pdf, mixed_gdf):
    assert repr(mixed_gdf) == repr(mixed_pdf)


def test_mixed_series(mixed_pdf, mixed_gdf):
    for col in mixed_gdf.columns:
        assert repr(mixed_gdf[col]) == repr(mixed_pdf[col])


def test_MI():
    gdf = cudf.DataFrame(
        {
            "a": np.random.randint(0, 4, 10),
            "b": np.random.randint(0, 4, 10),
            "c": np.random.randint(0, 4, 10),
        }
    )
    levels = [["a", "b", "c", "d"], ["w", "x", "y", "z"], ["m", "n"]]
    codes = cudf.DataFrame(
        {
            "a": [0, 0, 0, 0, 1, 1, 2, 2, 3, 3],
            "b": [0, 1, 2, 3, 0, 1, 2, 3, 0, 1],
            "c": [0, 1, 0, 1, 0, 1, 0, 1, 0, 1],
        }
    )
    pd.options.display.max_rows = 999
    pd.options.display.max_columns = 0
    gdf = gdf.set_index(cudf.MultiIndex(levels=levels, codes=codes))
    pdf = gdf.to_pandas()
    assert repr(gdf) == repr(pdf)
    assert repr(gdf.index) == repr(pdf.index)
    assert repr(gdf.T) == repr(pdf.T)
    pd.reset_option("display.max_rows")
    pd.reset_option("display.max_columns")


@pytest.mark.parametrize("nrows", [0, 1, 3, 5, 10])
@pytest.mark.parametrize("ncols", [0, 1, 2, 3])
def test_groupby_MI(nrows, ncols):
    gdf = cudf.DataFrame(
        {"a": np.arange(10), "b": np.arange(10), "c": np.arange(10)}
    )
    pdf = gdf.to_pandas()
    gdg = gdf.groupby(["a", "b"], sort=True).count()
    pdg = pdf.groupby(["a", "b"], sort=True).count()
    pd.options.display.max_rows = nrows
    pd.options.display.max_columns = ncols
    assert repr(gdg) == repr(pdg)
    assert repr(gdg.index) == repr(pdg.index)
    assert repr(gdg.T) == repr(pdg.T)
    pd.reset_option("display.max_rows")
    pd.reset_option("display.max_columns")


@pytest.mark.parametrize("dtype", utils.NUMERIC_TYPES)
@pytest.mark.parametrize("length", [0, 1, 10, 100, 1000])
def test_generic_index(length, dtype):
    psr = pd.Series(
        range(length),
        index=np.random.randint(0, high=100, size=length).astype(dtype),
        dtype="float64" if length == 0 else None,
    )
    gsr = cudf.Series.from_pandas(psr)

    assert repr(psr.index) == repr(gsr.index)


@pytest.mark.parametrize(
    "gdf",
    [
        cudf.DataFrame({"a": range(10000)}),
        cudf.DataFrame({"a": range(10000), "b": range(10000)}),
        cudf.DataFrame({"a": range(20), "b": range(20)}),
        cudf.DataFrame(
            {
                "a": range(20),
                "b": range(20),
                "c": ["abc", "def", "xyz", "def", "pqr"] * 4,
            }
        ),
        cudf.DataFrame(index=[1, 2, 3]),
        cudf.DataFrame(index=range(10000)),
        cudf.DataFrame(columns=["a", "b", "c", "d"]),
        cudf.DataFrame(columns=["a"], index=range(10000)),
        cudf.DataFrame(columns=["a", "col2", "...col n"], index=range(10000)),
        cudf.DataFrame(index=cudf.Series(range(10000)).astype("str")),
        cudf.DataFrame(
            columns=["a", "b", "c", "d"],
            index=cudf.Series(range(10000)).astype("str"),
        ),
    ],
)
@pytest.mark.parametrize(
    "slice",
    [
        slice(2500, 5000),
        slice(2500, 2501),
        slice(5000),
        slice(1, 10),
        slice(10, 20),
        slice(15, 2400),
    ],
)
@pytest.mark.parametrize("max_seq_items", [1, 10, 60, 10000, None])
@pytest.mark.parametrize("max_rows", [1, 10, 60, 10000, None])
def test_dataframe_sliced(gdf, slice, max_seq_items, max_rows):
    pd.options.display.max_seq_items = max_seq_items
    pd.options.display.max_rows = max_rows
    pdf = gdf.to_pandas()

    sliced_gdf = gdf[slice]
    sliced_pdf = pdf[slice]

    expected_repr = repr(sliced_pdf).replace("None", "<NA>")
    actual_repr = repr(sliced_gdf)

    assert expected_repr == actual_repr
    pd.reset_option("display.max_rows")
    pd.reset_option("display.max_seq_items")


@pytest.mark.parametrize(
    "index,expected_repr",
    [
        (
            cudf.Index([1, 2, 3, None]),
            "Int64Index([1, 2, 3, <NA>], dtype='int64')",
        ),
        (
            cudf.Index([None, 2.2, 3.324342, None]),
            "Float64Index([<NA>, 2.2, 3.324342, <NA>], dtype='float64')",
        ),
        (
            cudf.Index([None, None, None], name="hello"),
            "StringIndex([None None None], dtype='object', name='hello')",
        ),
        (
            cudf.Index([None, None, None], dtype="float", name="hello"),
            "Float64Index([<NA>, <NA>, <NA>], dtype='float64', name='hello')",
        ),
        (
            cudf.Index([None], dtype="float64", name="hello"),
            "Float64Index([<NA>], dtype='float64', name='hello')",
        ),
        (
            cudf.Index([None], dtype="int8", name="hello"),
            "Int8Index([<NA>], dtype='int8', name='hello')",
        ),
        (
            cudf.Index([None] * 50, dtype="object"),
            "StringIndex([None None None None None None None None "
            "None None None None None None\n None None None None None None "
            "None None None None None None None None\n None None None None "
            "None None None None None None None None None None\n None None "
            "None None None None None None], dtype='object')",
        ),
        (
            cudf.Index([None] * 20, dtype="uint32"),
            "UInt32Index([<NA>, <NA>, <NA>, <NA>, <NA>, <NA>, <NA>, <NA>, "
            "<NA>,\n       <NA>, <NA>, <NA>, <NA>, <NA>, <NA>, <NA>, <NA>, "
            "<NA>,\n       <NA>, <NA>],\n      dtype='uint32')",
        ),
        (
            cudf.Index(
                [None, 111, 22, 33, None, 23, 34, 2343, None], dtype="int16"
            ),
            "Int16Index([<NA>, 111, 22, 33, <NA>, 23, 34, 2343, <NA>], "
            "dtype='int16')",
        ),
        (
            cudf.Index([1, 2, 3, None], dtype="category"),
            "CategoricalIndex([1, 2, 3, <NA>], categories=[1, 2, 3], "
            "ordered=False, dtype='category')",
        ),
        (
            cudf.Index([None, None], dtype="category"),
            "CategoricalIndex([<NA>, <NA>], categories=[], ordered=False, "
            "dtype='category')",
        ),
        (
            cudf.Index(np.array([10, 20, 30, None], dtype="datetime64[ns]")),
            "DatetimeIndex([1970-01-01 00:00:00.000000010, "
            "1970-01-01 00:00:00.000000020,"
            "\n       1970-01-01 00:00:00.000000030, <NA>],\n      "
            "dtype='datetime64[ns]')",
        ),
        (
            cudf.Index(np.array([10, 20, 30, None], dtype="datetime64[s]")),
            "DatetimeIndex([1970-01-01 00:00:10, "
            "1970-01-01 00:00:20, 1970-01-01 00:00:30,\n"
            "       <NA>],\n      dtype='datetime64[s]')",
        ),
        (
            cudf.Index(np.array([10, 20, 30, None], dtype="datetime64[us]")),
            "DatetimeIndex([1970-01-01 00:00:00.000010, "
            "1970-01-01 00:00:00.000020,\n       "
            "1970-01-01 00:00:00.000030, <NA>],\n      "
            "dtype='datetime64[us]')",
        ),
        (
            cudf.Index(np.array([10, 20, 30, None], dtype="datetime64[ms]")),
            "DatetimeIndex([1970-01-01 00:00:00.010, "
            "1970-01-01 00:00:00.020,\n       "
            "1970-01-01 00:00:00.030, <NA>],\n      "
            "dtype='datetime64[ms]')",
        ),
        (
            cudf.Index(np.array([None] * 10, dtype="datetime64[ms]")),
            "DatetimeIndex([<NA>, <NA>, <NA>, <NA>, <NA>, <NA>, <NA>, <NA>, "
            "<NA>,\n       <NA>],\n      dtype='datetime64[ms]')",
        ),
    ],
)
def test_generic_index_null(index, expected_repr):

    actual_repr = repr(index)

    assert expected_repr == actual_repr


@pytest.mark.parametrize(
    "df,pandas_special_case",
    [
        (pd.DataFrame({"a": [1, 2, 3]}, index=[10, 20, None]), False),
        (
            pd.DataFrame(
                {
                    "a": [1, None, 3],
                    "string_col": ["hello", "world", "rapids"],
                },
                index=[None, "a", "b"],
            ),
            True,
        ),
        (pd.DataFrame([], index=[None, "a", "b"]), False),
        (pd.DataFrame({"aa": [None, None]}, index=[None, None]), False),
        (pd.DataFrame({"aa": [1, 2, 3]}, index=[None, None, None]), False),
        (
            pd.DataFrame(
                {"aa": [None, 2, 3]},
                index=np.array([1, None, None], dtype="datetime64[ns]"),
            ),
            False,
        ),
        (
            pd.DataFrame(
                {"aa": [None, 2, 3]},
                index=np.array([100, None, None], dtype="datetime64[ns]"),
            ),
            False,
        ),
        (
            pd.DataFrame(
                {"aa": [None, None, None]},
                index=np.array([None, None, None], dtype="datetime64[ns]"),
            ),
            False,
        ),
        (
            pd.DataFrame(
                {"aa": [1, None, 3]},
                index=np.array([10, 15, None], dtype="datetime64[ns]"),
            ),
            False,
        ),
        (
            pd.DataFrame(
                {"a": [1, 2, None], "v": [10, None, 22], "p": [100, 200, 300]}
            ).set_index(["a", "v"]),
            False,
        ),
        (
            pd.DataFrame(
                {
                    "a": [1, 2, None],
                    "v": ["n", "c", "a"],
                    "p": [None, None, None],
                }
            ).set_index(["a", "v"]),
            False,
        ),
        (
            pd.DataFrame(
                {
                    "a": np.array([1, None, None], dtype="datetime64[ns]"),
                    "v": ["n", "c", "a"],
                    "p": [None, None, None],
                }
            ).set_index(["a", "v"]),
            False,
        ),
    ],
)
def test_dataframe_null_index_repr(df, pandas_special_case):
    pdf = df
    gdf = cudf.from_pandas(pdf)

    expected_repr = (
        repr(pdf)
        .replace("NaN", "<NA>")
        .replace("NaT", "<NA>")
        .replace("None", "<NA>")
    )
    actual_repr = repr(gdf)

    if pandas_special_case:
        # Pandas inconsistently print StringIndex null values
        # as `None` at some places and `NaN` at few other places
        # Whereas cudf is consistent with strings `null` values
        # to be printed as `None` everywhere.
        actual_repr = repr(gdf).replace("None", "<NA>")

    assert expected_repr.split() == actual_repr.split()


@pytest.mark.parametrize(
    "sr,pandas_special_case",
    [
        (pd.Series([1, 2, 3], index=[10, 20, None]), False),
        (pd.Series([1, None, 3], name="a", index=[None, "a", "b"]), True),
        (pd.Series(None, index=[None, "a", "b"], dtype="float"), True),
        (pd.Series([None, None], name="aa", index=[None, None]), False),
        (pd.Series([1, 2, 3], index=[None, None, None]), False),
        (
            pd.Series(
                [None, 2, 3],
                index=np.array([1, None, None], dtype="datetime64[ns]"),
            ),
            False,
        ),
        (
            pd.Series(
                [None, None, None],
                index=np.array([None, None, None], dtype="datetime64[ns]"),
            ),
            False,
        ),
        (
            pd.Series(
                [1, None, 3],
                index=np.array([10, 15, None], dtype="datetime64[ns]"),
            ),
            False,
        ),
        (
            pd.DataFrame(
                {"a": [1, 2, None], "v": [10, None, 22], "p": [100, 200, 300]}
            ).set_index(["a", "v"])["p"],
            False,
        ),
        (
            pd.DataFrame(
                {
                    "a": [1, 2, None],
                    "v": ["n", "c", "a"],
                    "p": [None, None, None],
                }
            ).set_index(["a", "v"])["p"],
            False,
        ),
        (
            pd.DataFrame(
                {
                    "a": np.array([1, None, None], dtype="datetime64[ns]"),
                    "v": ["n", "c", "a"],
                    "p": [None, None, None],
                }
            ).set_index(["a", "v"])["p"],
            False,
        ),
    ],
)
def test_series_null_index_repr(sr, pandas_special_case):
    psr = sr
    gsr = cudf.from_pandas(psr)

    expected_repr = (
        repr(psr)
        .replace("NaN", "<NA>")
        .replace("NaT", "<NA>")
        .replace("None", "<NA>")
    )
    actual_repr = repr(gsr)

    if pandas_special_case:
        # Pandas inconsistently print StringIndex null values
        # as `None` at some places and `NaN` at few other places
        # Whereas cudf is consistent with strings `null` values
        # to be printed as `None` everywhere.
        actual_repr = repr(gsr).replace("None", "<NA>")
    assert expected_repr.split() == actual_repr.split()


@pytest.mark.parametrize(
    "data",
    [
        [1000000, 200000, 3000000],
        [1000000, 200000, None],
        [],
        [None],
        [None, None, None, None, None],
        [12, 12, 22, 343, 4353534, 435342],
        np.array([10, 20, 30, None, 100]),
        cp.asarray([10, 20, 30, 100]),
        [1000000, 200000, 3000000],
        [1000000, 200000, None],
        [1],
        [12, 11, 232, 223432411, 2343241, 234324, 23234],
        [12, 11, 2.32, 2234.32411, 2343.241, 23432.4, 23234],
        [1.321, 1132.324, 23223231.11, 233.41, 0.2434, 332, 323],
        [
            136457654,
            134736784,
            245345345,
            223432411,
            2343241,
            3634548734,
            23234,
        ],
        [12, 11, 2.32, 2234.32411, 2343.241, 23432.4, 23234],
    ],
)
@pytest.mark.parametrize("dtype", ["timedelta64[s]", "timedelta64[us]"])
def test_timedelta_series_s_us_repr(data, dtype):
    if not PANDAS_GE_110:
        pytest.xfail(reason="pandas >= 1.1 requried")
    sr = cudf.Series(data, dtype=dtype)
    psr = sr.to_pandas()

    expected = (
        repr(psr).replace("timedelta64[ns]", dtype).replace("NaT", "<NA>")
    )
    actual = repr(sr)

    assert expected.split() == actual.split()


@pytest.mark.parametrize(
    "ser, expected_repr",
    [
        (
            cudf.Series([], dtype="timedelta64[ns]"),
            textwrap.dedent(
                """
            Series([], dtype: timedelta64[ns])
            """
            ),
        ),
        (
            cudf.Series([], dtype="timedelta64[ms]"),
            textwrap.dedent(
                """
            Series([], dtype: timedelta64[ms])
            """
            ),
        ),
        (
            cudf.Series([1000000, 200000, 3000000], dtype="timedelta64[ns]"),
            textwrap.dedent(
                """
            0    0 days 00:00:00.001000000
            1    0 days 00:00:00.000200000
            2    0 days 00:00:00.003000000
            dtype: timedelta64[ns]
            """
            ),
        ),
        (
            cudf.Series([1000000, 200000, 3000000], dtype="timedelta64[ms]"),
            textwrap.dedent(
                """
            0    0 days 00:16:40
            1    0 days 00:03:20
            2    0 days 00:50:00
            dtype: timedelta64[ms]
            """
            ),
        ),
        (
            cudf.Series([1000000, 200000, None], dtype="timedelta64[ns]"),
            textwrap.dedent(
                """
            0    0 days 00:00:00.001000000
            1    0 days 00:00:00.000200000
            2                         <NA>
            dtype: timedelta64[ns]
            """
            ),
        ),
        (
            cudf.Series([1000000, 200000, None], dtype="timedelta64[ms]"),
            textwrap.dedent(
                """
            0    0 days 00:16:40
            1    0 days 00:03:20
            2               <NA>
            dtype: timedelta64[ms]
            """
            ),
        ),
        (
            cudf.Series(
                [None, None, None, None, None], dtype="timedelta64[ns]"
            ),
            textwrap.dedent(
                """
            0    <NA>
            1    <NA>
            2    <NA>
            3    <NA>
            4    <NA>
            dtype: timedelta64[ns]
            """
            ),
        ),
        (
            cudf.Series(
                [None, None, None, None, None], dtype="timedelta64[ms]"
            ),
            textwrap.dedent(
                """
            0    <NA>
            1    <NA>
            2    <NA>
            3    <NA>
            4    <NA>
            dtype: timedelta64[ms]
            """
            ),
        ),
        (
            cudf.Series(
                [12, 12, 22, 343, 4353534, 435342], dtype="timedelta64[ns]"
            ),
            textwrap.dedent(
                """
            0    0 days 00:00:00.000000012
            1    0 days 00:00:00.000000012
            2    0 days 00:00:00.000000022
            3    0 days 00:00:00.000000343
            4    0 days 00:00:00.004353534
            5    0 days 00:00:00.000435342
            dtype: timedelta64[ns]
            """
            ),
        ),
        (
            cudf.Series(
                [12, 12, 22, 343, 4353534, 435342], dtype="timedelta64[ms]"
            ),
            textwrap.dedent(
                """
            0    0 days 00:00:00.012
            1    0 days 00:00:00.012
            2    0 days 00:00:00.022
            3    0 days 00:00:00.343
            4    0 days 01:12:33.534
            5    0 days 00:07:15.342
            dtype: timedelta64[ms]
            """
            ),
        ),
        (
            cudf.Series(
                [1.321, 1132.324, 23223231.11, 233.41, 0.2434, 332, 323],
                dtype="timedelta64[ns]",
            ),
            textwrap.dedent(
                """
            0    0 days 00:00:00.000000001
            1    0 days 00:00:00.000001132
            2    0 days 00:00:00.023223231
            3    0 days 00:00:00.000000233
            4              0 days 00:00:00
            5    0 days 00:00:00.000000332
            6    0 days 00:00:00.000000323
            dtype: timedelta64[ns]
            """
            ),
        ),
        (
            cudf.Series(
                [1.321, 1132.324, 23223231.11, 233.41, 0.2434, 332, 323],
                dtype="timedelta64[ms]",
            ),
            textwrap.dedent(
                """
            0    0 days 00:00:00.001
            1    0 days 00:00:01.132
            2    0 days 06:27:03.231
            3    0 days 00:00:00.233
            4        0 days 00:00:00
            5    0 days 00:00:00.332
            6    0 days 00:00:00.323
            dtype: timedelta64[ms]
            """
            ),
        ),
        (
            cudf.Series(
                [
                    13645765432432,
                    134736784,
                    245345345,
                    223432411,
                    999992343241,
                    3634548734,
                    23234,
                ],
                dtype="timedelta64[ms]",
            ),
            textwrap.dedent(
                """
            0    157937 days 02:23:52.432
            1         1 days 13:25:36.784
            2         2 days 20:09:05.345
            3         2 days 14:03:52.411
            4     11573 days 23:39:03.241
            5        42 days 01:35:48.734
            6         0 days 00:00:23.234
            dtype: timedelta64[ms]
            """
            ),
        ),
        (
            cudf.Series(
                [
                    13645765432432,
                    134736784,
                    245345345,
                    223432411,
                    999992343241,
                    3634548734,
                    23234,
                ],
                dtype="timedelta64[ns]",
            ),
            textwrap.dedent(
                """
            0    0 days 03:47:25.765432432
            1    0 days 00:00:00.134736784
            2    0 days 00:00:00.245345345
            3    0 days 00:00:00.223432411
            4    0 days 00:16:39.992343241
            5    0 days 00:00:03.634548734
            6    0 days 00:00:00.000023234
            dtype: timedelta64[ns]
            """
            ),
        ),
        (
            cudf.Series(
                [
                    13645765432432,
                    134736784,
                    245345345,
                    223432411,
                    999992343241,
                    3634548734,
                    23234,
                ],
                dtype="timedelta64[ms]",
                name="abc",
            ),
            textwrap.dedent(
                """
            0    157937 days 02:23:52.432
            1         1 days 13:25:36.784
            2         2 days 20:09:05.345
            3         2 days 14:03:52.411
            4     11573 days 23:39:03.241
            5        42 days 01:35:48.734
            6         0 days 00:00:23.234
            Name: abc, dtype: timedelta64[ms]
            """
            ),
        ),
        (
            cudf.Series(
                [
                    13645765432432,
                    134736784,
                    245345345,
                    223432411,
                    999992343241,
                    3634548734,
                    23234,
                ],
                dtype="timedelta64[ns]",
                index=["a", "b", "z", "x", "y", "l", "m"],
                name="hello",
            ),
            textwrap.dedent(
                """
            a    0 days 03:47:25.765432432
            b    0 days 00:00:00.134736784
            z    0 days 00:00:00.245345345
            x    0 days 00:00:00.223432411
            y    0 days 00:16:39.992343241
            l    0 days 00:00:03.634548734
            m    0 days 00:00:00.000023234
            Name: hello, dtype: timedelta64[ns]
            """
            ),
        ),
    ],
)
def test_timedelta_series_ns_ms_repr(ser, expected_repr):
    expected = expected_repr
    actual = repr(ser)

    assert expected.split() == actual.split()


@pytest.mark.parametrize(
    "df,expected_repr",
    [
        (
            cudf.DataFrame(
                {
                    "a": cudf.Series(
                        [1000000, 200000, 3000000], dtype="timedelta64[s]"
                    )
                }
            ),
            textwrap.dedent(
                """
                                  a
                0  11 days 13:46:40
                1   2 days 07:33:20
                2  34 days 17:20:00
                """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": cudf.Series(
                        [
                            136457654,
                            None,
                            245345345,
                            223432411,
                            None,
                            3634548734,
                            23234,
                        ],
                        dtype="timedelta64[s]",
                    ),
                    "b": [10, 11, 22, 33, 44, 55, 66],
                }
            ),
            textwrap.dedent(
                """
                                     a   b
                0   1579 days 08:54:14  10
                1                 <NA>  11
                2   2839 days 15:29:05  22
                3   2586 days 00:33:31  33
                4                 <NA>  44
                5  42066 days 12:52:14  55
                6      0 days 06:27:14  66
                """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": cudf.Series(
                        [
                            136457654,
                            None,
                            245345345,
                            223432411,
                            None,
                            3634548734,
                            23234,
                        ],
                        dtype="timedelta64[s]",
                        index=["a", "b", "c", "d", "e", "f", "g"],
                    )
                }
            ),
            textwrap.dedent(
                """
                                     a
                a   1579 days 08:54:14
                b                 <NA>
                c   2839 days 15:29:05
                d   2586 days 00:33:31
                e                 <NA>
                f  42066 days 12:52:14
                g      0 days 06:27:14
                """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": cudf.Series(
                        [1, 2, 3, 4, 5, 6, 7],
                        index=cudf.Index(
                            [
                                136457654,
                                None,
                                245345345,
                                223432411,
                                None,
                                3634548734,
                                23234,
                            ],
                            dtype="timedelta64[ms]",
                        ),
                    )
                }
            ),
            textwrap.dedent(
                """
                                      a
                1 days 13:54:17.654   1
                <NA>                  2
                2 days 20:09:05.345   3
                2 days 14:03:52.411   4
                <NA>                  5
                42 days 01:35:48.734  6
                0 days 00:00:23.234   7
                """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": cudf.Series(
                        ["a", "f", "q", "e", "w", "e", "t"],
                        index=cudf.Index(
                            [
                                136457654,
                                None,
                                245345345,
                                223432411,
                                None,
                                3634548734,
                                23234,
                            ],
                            dtype="timedelta64[ns]",
                        ),
                    )
                }
            ),
            textwrap.dedent(
                """
                                    a
                0 days 00:00:00.136457654  a
                <NA>                f
                0 days 00:00:00.245345345  q
                0 days 00:00:00.223432411  e
                <NA>                w
                0 days 00:00:03.634548734  e
                0 days 00:00:00.000023234  t
                """
            ),
        ),
    ],
)
def test_timedelta_dataframe_repr(df, expected_repr):
    actual_repr = repr(df)

    assert actual_repr.split() == expected_repr.split()


@pytest.mark.parametrize(
    "index, expected_repr",
    [
        (
            cudf.Index([1000000, 200000, 3000000], dtype="timedelta64[ms]"),
            "TimedeltaIndex(['0 days 00:16:40', "
            "'0 days 00:03:20', '0 days 00:50:00'], "
            "dtype='timedelta64[ms]')",
        ),
        (
            cudf.Index(
                [None, None, None, None, None], dtype="timedelta64[us]"
            ),
            "TimedeltaIndex([<NA>, <NA>, <NA>, <NA>, <NA>], "
            "dtype='timedelta64[us]')",
        ),
        (
            cudf.Index(
                [
                    136457654,
                    None,
                    245345345,
                    223432411,
                    None,
                    3634548734,
                    23234,
                ],
                dtype="timedelta64[us]",
            ),
            "TimedeltaIndex([0 days 00:02:16.457654, <NA>, "
            "0 days 00:04:05.345345, "
            "0 days 00:03:43.432411, <NA>,"
            "       0 days 01:00:34.548734, 0 days 00:00:00.023234],"
            "      dtype='timedelta64[us]')",
        ),
        (
            cudf.Index(
                [
                    136457654,
                    None,
                    245345345,
                    223432411,
                    None,
                    3634548734,
                    23234,
                ],
                dtype="timedelta64[s]",
            ),
            "TimedeltaIndex([1579 days 08:54:14, <NA>, 2839 days 15:29:05,"
            "       2586 days 00:33:31, <NA>, 42066 days 12:52:14, "
            "0 days 06:27:14],"
            "      dtype='timedelta64[s]')",
        ),
    ],
)
def test_timedelta_index_repr(index, expected_repr):
    if not PANDAS_GE_110:
        pytest.xfail(reason="pandas >= 1.1 requried")
    actual_repr = repr(index)

    assert actual_repr.split() == expected_repr.split()


@pytest.mark.parametrize(
    "pmi",
    [
        pd.MultiIndex.from_tuples(
            [(1, "red"), (1, "blue"), (2, "red"), (2, "blue")]
        ),
        pd.MultiIndex.from_tuples(
            [(1, "red"), (1, "blue"), (2, "red"), (2, "blue")] * 10
        ),
        pd.MultiIndex.from_tuples([(1, "red", 102, "sdf")]),
        pd.MultiIndex.from_tuples(
            [
                ("abc", 0.234, 1),
                ("a", -0.34, 0),
                ("ai", 111, 4385798),
                ("rapids", 0, 34534534),
            ],
            names=["alphabets", "floats", "ints"],
        ),
    ],
)
@pytest.mark.parametrize("max_seq_items", [None, 1, 2, 5, 10, 100])
def test_multiindex_repr(pmi, max_seq_items):
    pd.set_option("display.max_seq_items", max_seq_items)
    gmi = cudf.from_pandas(pmi)

    assert repr(gmi) == repr(pmi)
    pd.reset_option("display.max_seq_items")


@pytest.mark.parametrize(
    "gdi, expected_repr",
    [
        (
            cudf.DataFrame(
                {
                    "a": [None, 1, 2, 3],
                    "b": ["abc", None, "xyz", None],
                    "c": [0.345, np.nan, 100, 10],
                }
            )
            .set_index(["a", "b"])
            .index,
            textwrap.dedent(
                """
                MultiIndex([(<NA>, 'abc'),
                            (   1,  <NA>),
                            (   2, 'xyz'),
                            (   3,  <NA>)],
                        names=['a', 'b'])
                """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": cudf.Series([None, np.nan, 2, 3], nan_as_null=False),
                    "b": ["abc", None, "xyz", None],
                    "c": [0.345, np.nan, 100, 10],
                }
            )
            .set_index(["a", "b"])
            .index,
            textwrap.dedent(
                """
            MultiIndex([(<NA>, 'abc'),
                        ( nan,  <NA>),
                        ( 2.0, 'xyz'),
                        ( 3.0,  <NA>)],
                    names=['a', 'b'])
            """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": cudf.Series([None, 1, 2, 3], dtype="datetime64[ns]"),
                    "b": ["abc", None, "xyz", None],
                    "c": [0.345, np.nan, 100, 10],
                }
            )
            .set_index(["a", "b"])
            .index,
            textwrap.dedent(
                """
            MultiIndex([(                         '<NA>', 'abc'),
                        ('1970-01-01 00:00:00.000000001',  <NA>),
                        ('1970-01-01 00:00:00.000000002', 'xyz'),
                        ('1970-01-01 00:00:00.000000003',  <NA>)],
                    names=['a', 'b'])
            """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": cudf.Series([None, 1, 2, 3], dtype="datetime64[ns]"),
                    "b": ["abc", None, "xyz", None],
                    "c": [0.345, np.nan, 100, 10],
                }
            )
            .set_index(["a", "b", "c"])
            .index,
            textwrap.dedent(
                """
                MultiIndex([(                         '<NA>', 'abc', 0.345),
                            ('1970-01-01 00:00:00.000000001',  <NA>,  <NA>),
                            ('1970-01-01 00:00:00.000000002', 'xyz', 100.0),
                            ('1970-01-01 00:00:00.000000003',  <NA>,  10.0)],
                        names=['a', 'b', 'c'])
                """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": ["abc", None, "xyz", None],
                    "b": cudf.Series([None, 1, 2, 3], dtype="timedelta64[ns]"),
                    "c": [0.345, np.nan, 100, 10],
                }
            )
            .set_index(["a", "b", "c"])
            .index,
            textwrap.dedent(
                """
                MultiIndex([('abc',                      '<NA>', 0.345),
                            ( <NA>, '0 days 00:00:00.000000001',  <NA>),
                            ('xyz', '0 days 00:00:00.000000002', 100.0),
                            ( <NA>, '0 days 00:00:00.000000003',  10.0)],
                        names=['a', 'b', 'c'])
                """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": ["abc", None, "xyz", None],
                    "b": cudf.Series([None, 1, 2, 3], dtype="timedelta64[ns]"),
                    "c": [0.345, np.nan, 100, 10],
                }
            )
            .set_index(["c", "a"])
            .index,
            textwrap.dedent(
                """
                MultiIndex([(0.345, 'abc'),
                            ( <NA>,  <NA>),
                            (100.0, 'xyz'),
                            ( 10.0,  <NA>)],
                        names=['c', 'a'])
                """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": [None, None, None, None],
                    "b": cudf.Series(
                        [None, None, None, None], dtype="timedelta64[ns]"
                    ),
                    "c": [0.345, np.nan, 100, 10],
                }
            )
            .set_index(["b", "a"])
            .index,
            textwrap.dedent(
                """
            MultiIndex([('<NA>', <NA>),
                        ('<NA>', <NA>),
                        ('<NA>', <NA>),
                        ('<NA>', <NA>)],
                    names=['b', 'a'])
            """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": [1, 2, None, 3, 5],
                    "b": [
                        "abc",
                        "def, hi, bye",
                        None,
                        ", one, two, three, four",
                        None,
                    ],
                    "c": cudf.Series(
                        [0.3232, np.nan, 1, None, -0.34534], nan_as_null=False
                    ),
                    "d": [None, 100, 2000324, None, None],
                }
            )
            .set_index(["a", "b", "c", "d"])
            .index,
            textwrap.dedent(
                """
    MultiIndex([(   1,                     'abc',   0.3232,    <NA>),
                (   2,            'def, hi, bye',      nan,     100),
                (<NA>,                      <NA>,      1.0, 2000324),
                (   3, ', one, two, three, four',     <NA>,    <NA>),
                (   5,                      <NA>, -0.34534,    <NA>)],
            names=['a', 'b', 'c', 'd'])
    """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": [1, 2, None, 3, 5],
                    "b": [
                        "abc",
                        "def, hi, bye",
                        None,
                        ", one, two, three, four",
                        None,
                    ],
                    "c": cudf.Series(
                        [0.3232, np.nan, 1, None, -0.34534], nan_as_null=False
                    ),
                    "d": [None, 100, 2000324, None, None],
                }
            )
            .set_index(["b", "a", "c", "d"])
            .index,
            textwrap.dedent(
                """
    MultiIndex([(                    'abc',    1,   0.3232,    <NA>),
                (           'def, hi, bye',    2,      nan,     100),
                (                     <NA>, <NA>,      1.0, 2000324),
                (', one, two, three, four',    3,     <NA>,    <NA>),
                (                     <NA>,    5, -0.34534,    <NA>)],
            names=['b', 'a', 'c', 'd'])
    """
            ),
        ),
        (
            cudf.DataFrame(
                {
                    "a": ["(abc", "2", None, "3", "5"],
                    "b": [
                        "abc",
                        "def, hi, bye",
                        None,
                        ", one, two, three, four",
                        None,
                    ],
                    "c": cudf.Series(
                        [0.3232, np.nan, 1, None, -0.34534], nan_as_null=False
                    ),
                    "d": [None, 100, 2000324, None, None],
                }
            )
            .set_index(["a", "b", "c", "d"])
            .index,
            textwrap.dedent(
                """
    MultiIndex([('(abc',                     'abc',   0.3232,    <NA>),
                (   '2',            'def, hi, bye',      nan,     100),
                (  <NA>,                      <NA>,      1.0, 2000324),
                (   '3', ', one, two, three, four',     <NA>,    <NA>),
                (   '5',                      <NA>, -0.34534,    <NA>)],
            names=['a', 'b', 'c', 'd'])
    """
            ),
        ),
    ],
)
def test_multiindex_null_repr(gdi, expected_repr):
    actual_repr = repr(gdi)

    assert actual_repr.split() == expected_repr.split()


def test_categorical_series_with_nan_repr():
    series = cudf.Series(
        [1, 2, np.nan, 10, np.nan, None], nan_as_null=False
    ).astype("category")

    expected_repr = textwrap.dedent(
        """
    0     1.0
    1     2.0
    2     NaN
    3    10.0
    4     NaN
    5    <NA>
    dtype: category
    Categories (4, float64): [1.0, 2.0, 10.0, NaN]
    """
    )

    assert repr(series).split() == expected_repr.split()

    sliced_expected_repr = textwrap.dedent(
        """
        2     NaN
        3    10.0
        4     NaN
        5    <NA>
        dtype: category
        Categories (4, float64): [1.0, 2.0, 10.0, NaN]
        """
    )

    assert repr(series[2:]).split() == sliced_expected_repr.split()


def test_categorical_dataframe_with_nan_repr():
    series = cudf.Series(
        [1, 2, np.nan, 10, np.nan, None], nan_as_null=False
    ).astype("category")
    df = cudf.DataFrame({"a": series})
    expected_repr = textwrap.dedent(
        """
          a
    0   1.0
    1   2.0
    2   NaN
    3  10.0
    4   NaN
    5  <NA>
    """
    )

    assert repr(df).split() == expected_repr.split()


def test_categorical_index_with_nan_repr():
    cat_index = cudf.Index(
        cudf.Series(
            [1, 2, np.nan, 10, np.nan, None], nan_as_null=False
        ).astype("category")
    )

    expected_repr = (
        "CategoricalIndex([1.0, 2.0, NaN, 10.0, NaN, <NA>], "
        "categories=[1.0, 2.0, 10.0, NaN], ordered=False, dtype='category')"
    )

    assert repr(cat_index) == expected_repr

    sliced_expected_repr = (
        "CategoricalIndex([NaN, 10.0, NaN, <NA>], "
        "categories=[1.0, 2.0, 10.0, NaN], ordered=False, dtype='category')"
    )

    assert repr(cat_index[2:]) == sliced_expected_repr


def test_empty_series_name():
    ps = pd.Series([], name="abc", dtype="int")
    gs = cudf.from_pandas(ps)

    assert repr(ps) == repr(gs)


def test_repr_struct_after_concat():
    df = cudf.DataFrame(
        {
            "a": cudf.Series(
                [
                    {"sa": 2056831253},
                    {"sa": -1463792165},
                    {"sa": 1735783038},
                    {"sa": 103774433},
                    {"sa": -1413247520},
                ]
                * 13
            ),
            "b": cudf.Series(
                [
                    {"sa": {"ssa": 1140062029}},
                    None,
                    {"sa": {"ssa": 1998862860}},
                    {"sa": None},
                    {"sa": {"ssa": -395088502}},
                ]
                * 13
            ),
        }
    )
    pdf = df.to_pandas()

    assert repr(df) == repr(pdf)

@pytest.mark.parametrize("ser,expected_repr",[
        (
            cudf.Series(
                [
                    "1969-12-31 23:59:58.001001", "1839-12-24 03:58:56.000826", "1647-05-20 19:25:03.000638"
                ],
                dtype="datetime64[us]",
                index=["a", "b", "z"],
                name="hello",
            ),
            textwrap.dedent(
                """
            a    1969-12-31 23:59:58.001001
            b    1839-12-24 03:58:56.000826
            z    1647-05-20 19:25:03.000638
            Name: hello, dtype: datetime64[us]
            """
            ),
        ),
        (
            cudf.Series(["2499-12-01 01:00:00", "2499-11-01 01:30:00"], dtype="datetime64[s]"),
            textwrap.dedent(
                """
                0    2499-12-01 01:00:00
                1    2499-11-01 01:30:00
                dtype: datetime64[s]
                """
            )
        )
    ],
)
def test_datetime_series_repr(ser, expected_repr):
    expected = expected_repr
    actual = repr(ser)

    assert expected.split() == actual.split()