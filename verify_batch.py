"""Compare the persisted batch lake with an independent CSV and Decimal calculation."""
import csv
import json
import math
import re
from collections import defaultdict
from decimal import Decimal, localcontext
from common import TABLES,lake_root,spark_session,save_audit


def normalize(value):
    if value is None or value == '':
        return None
    return re.sub(r'[\t\n\v\f\r \u00A0]+',' ',value).strip(' ')


def main():
    root=lake_root(); silver={};counts={}
    for table in TABLES:
        seen=set();rows=[];raw_count=0
        with (root/'landing'/f'{table}.csv').open(encoding='utf-8-sig',newline='') as f:
            reader=csv.DictReader(f);names=reader.fieldnames
            for row in reader:
                raw_count+=1
                cleaned=tuple(normalize(row[name]) for name in names)
                if cleaned not in seen:
                    seen.add(cleaned);rows.append(dict(zip(names,cleaned)))
        silver[table]=rows;counts[table]={'bronze':raw_count,'silver':len(rows)}
    bio={};invalid=0
    for row in silver['athlete_bio']:
        if row['athlete_id'] in bio:raise ValueError('Duplicate athlete key in oracle')
        try:
            height=Decimal(row['height']);weight=Decimal(row['weight'])
            if not math.isfinite(float(height)) or not math.isfinite(float(weight)):
                raise ValueError('Nonfinite measurement')
        except (ValueError,TypeError,ArithmeticError):invalid+=1;continue
        bio[row['athlete_id']]=(height,weight,row['sex'],row['country_noc'])
    sums=defaultdict(lambda:[Decimal(0),Decimal(0),0]);unknown=invalid_events=0
    all_ids={r['athlete_id'] for r in silver['athlete_bio']}
    for row in silver['athlete_event_results']:
        if row['athlete_id'] not in all_ids:unknown+=1;continue
        if row['athlete_id'] not in bio:invalid_events+=1;continue
        height,weight,sex,country=bio[row['athlete_id']]
        key=(row['sport'],row['medal'] or None,sex,country)
        sums[key][0]+=height;sums[key][1]+=weight;sums[key][2]+=1
    spark=spark_session('FP independent batch verification')
    try:
        for table in TABLES:
            assert spark.read.parquet(str(root/'bronze'/table)).count()==counts[table]['bronze']
            assert spark.read.parquet(str(root/'silver'/table)).count()==counts[table]['silver']
        actual=spark.read.parquet(str(root/'gold'/'avg_stats')).collect()
        assert len(actual)==len(sums)
        for row in actual:
            key=(row.sport,row.medal,row.sex,row.country_noc)
            h,w,n=sums.pop(key)
            assert row.event_count==n
            assert math.isclose(row.avg_height,float(h/n),rel_tol=1e-12,abs_tol=1e-9),(key,row.avg_height,h/n)
            assert math.isclose(row.avg_weight,float(w/n),rel_tol=1e-12,abs_tol=1e-9),(key,row.avg_weight,w/n)
            assert row.timestamp is not None
        assert not sums
        save_audit('independent_verification',{'status':'PASS','table_counts':counts,'groups_checked':len(actual),
                  'invalid_bio_rows':invalid,'unknown_athlete_events':unknown,'events_with_invalid_bio':invalid_events,
                  'joined_event_rows':sum(r.event_count for r in actual),'every_group_and_mean_matches':True,
                  'no_medal_groups':sum(r.medal is None for r in actual),'timestamp_present':True})
    finally:spark.stop()


if __name__=='__main__':main()
