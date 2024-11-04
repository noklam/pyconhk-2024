# pyconhk-2024

https://duckdb.org/docs/extensions/tpch.html


## Installtion
Install duckdb:
```
brew install duckdb
duckdb
```

export JAVA_HOME=`/usr/libexec/java_home -v 1.8` # Need to run this on M2

Create the TPCH benchmark:
```
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf = 1);
```

You can change the scaling factor to a larger number for heavier workload.

## Why ibis?
- If you are Python User:
- If you are SQL User, write once, run everywhere.
- Efficient testing (Spark in production, duckdb for local test)
- Simply more efficient, and fast

## Datalake, Composable Open Data Stack
- Storage decoupled (Spark, DuckDB, pandas) can all read Parquet.
- Queries can be translated between different engines.


# Reference
- https://duckdb.org/docs/extensions/tpch.html
- https://ibis-project.org/posts/ibis-bench/
- https://github.com/databricks/tpch-dbgen/tree/master/queries
- https://github.com/dotnet/spark/blob/main/benchmark/python/tpch_functional_queries.py


## Other benefits:
ibis:
    Type-checked and validated as you go. No more debugging cryptic database errors; Ibis catches your mistakes right away.
    Easier to write. Pythonic function calls with tab completion in IPython.
    More composable. Break complex queries down into easier-to-digest pieces
    Easier to reuse. Mix and match Ibis snippets to create expressions tailored for your analysis.