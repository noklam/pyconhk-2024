from __future__ import annotations


import ibis
# from ibis.backends.tests.errors import ClickHouseDatabaseError
# from ibis.backends.tests.tpc.conftest import add_date, tpc_test
"""Pricing Summary Report Query (Q1).

The Pricing Summary Report Query provides a summary pricing report for all
lineitems shipped as of a given date.  The  date is  within  60  - 120 days
of  the  greatest  ship  date  contained  in  the database.  The query
lists totals  for extended  price,  discounted  extended price, discounted
extended price  plus  tax,  average  quantity, average extended price,  and
average discount.  These  aggregates  are grouped  by RETURNFLAG  and
LINESTATUS, and  listed  in ascending  order of RETURNFLAG and  LINESTATUS.
A  count  of the  number  of  lineitems in each  group  is included.
"""
import datetime
from dateutil.relativedelta import relativedelta

def add_date(datestr: str, dy: int = 0, dm: int = 0, dd: int = 0) -> ir.DateScalar:
    dt = datetime.date.fromisoformat(datestr)
    dt += relativedelta(years=dy, months=dm, days=dd)
    return ibis.date(dt.isoformat())


con = ibis.connect("duckdb:///Users/Nok_Lam_Chan/conference/pycon2024/tpch.duckdb")
lineitem = con.table("lineitem", database=f"tpch")

t = lineitem
q = t.filter(t.l_shipdate <= add_date("1998-12-01", dd=-90))
discount_price = t.l_extendedprice * (1 - t.l_discount)
charge = discount_price * (1 + t.l_tax)
q = q.group_by(["l_returnflag", "l_linestatus"])
q = q.aggregate(
    sum_qty=t.l_quantity.sum(),
    sum_base_price=t.l_extendedprice.sum(),
    sum_disc_price=discount_price.sum(),
    sum_charge=charge.sum(),
    avg_qty=t.l_quantity.mean(),
    avg_price=t.l_extendedprice.mean(),
    avg_disc=t.l_discount.mean(),
    count_order=lambda t: t.count(),
)
q = q.order_by(["l_returnflag", "l_linestatus"])
print(q)
print(ibis.to_sql(q))