import click
from datetime import datetime
from dateutil.parser import parse as parse_date
import github
import json
import logging
import sqlite3
import sys


log = logging.getLogger('ghstats')

def row_factory(cursor, row):
    """Returns a sqlite row factory that returns a dictionary"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


SCHEMA_REFER = """
CREATE TABLE IF NOT EXISTS repo_refer (
   repo     varchar,
   ingest_date datetime,
   referrer varchar,
   count    integer,
   uniques  integer
)
"""

def load_paths(gh, db, repo, sample_period=5):
    traffic = gh.repos(*(repo.split('/'))).traffic.popular.paths.get()
    dt = datetime.utcnow()

    cursor = db.cursor()
    cursor.execute('''
       select ingest_date
       from repo_paths
       where repo = ?
       order by ingest_date desc limit 1''', (repo,))
    last = cursor.fetchone()

    if last:
        last = parse_date(last['ingest_date'])
        if (dt - last).days < sample_period:
            log.info(
                "Traffic Paths waiting %d more days for next sampling period",
                sample_period - (dt-last).days)
            return

    ingest_date = dt.isoformat()
    records = []
    for t in traffic:
        records.append((repo, ingest_date, t['path'], t['title'], t['count'], t['uniques']))

    log.info("Inserting %d Popular paths for %s", len(records), repo)
    cursor.executemany(
        'insert into repo_paths values (?, ?, ?, ?, ?, ?)', records)


load_paths.schema = """
CREATE TABLE IF NOT EXISTS repo_paths (
   repo     varchar,
   ingest_date datetime,
   path     varchar,
   title    varchar,
   count    integer,
   uniques  integer
)
"""


def load_clones(gh, db, repo):
    traffic = gh.repos(*(repo.split('/'))).traffic.clones.get()
    cursor = db.cursor()
    cursor.execute('''
        select timestamp 
        from repo_clones
        where repo = ?
        order by timestamp desc limit 1''', (repo,))
    last = cursor.fetchone()
    
    records = []
    for t in traffic.get('clones', []):
        if last and t['timestamp'] <= last['timestamp']:
            continue
        records.append((
            repo, t['timestamp'], t['count'], t['uniques']))
    if not records:
        return

    log.info("Inserting %d Clones Days for %s", len(records), repo)
    cursor.executemany(
        'insert into repo_clones values (?, ?, ?, ?)', records)    

load_clones.schema = """
CREATE TABLE IF NOT EXISTS repo_clones (
   repo     varchar,
   timestamp datetime,
   count    integer,
   uniques  integer
)
"""


def load_views(gh, db, repo):
    traffic = gh.repos(*(repo.split('/'))).traffic.views.get()

    cursor = db.cursor()
    cursor.execute('''
        select timestamp 
        from repo_views 
        where repo = ?
        order by timestamp desc limit 1''', (repo,))
    last = cursor.fetchone()
    
    records = []
    for t in traffic.get('views', []):
        if last and t['timestamp'] <= last['timestamp']:
            continue
        records.append((
            repo, t['timestamp'], t['count'], t['uniques']))
    if not records:
        return

    log.info("Inserting %d View Days for %s", len(records), repo)
    cursor.executemany(
        'insert into repo_views values (?, ?, ?, ?)', records)


load_views.schema = """
CREATE TABLE IF NOT EXISTS repo_views (
   repo     varchar,
   timestamp datetime,
   uniques  integer,
   count    integer
)
"""


LOADERS = [load_views, load_clones, load_paths]


@click.command()
@click.option('-t', '--token', envvar='GITHUB_TOKEN')
@click.option(
    '-r', '--repo', multiple=True,
    help='Repo to inspect/collect, format is $user|$org/repo_name, ie. kapilt/cloud-custodian')
@click.option('-f', '--db')
def main(token, repo, db):

    logging.basicConfig(level=logging.INFO)
    hub = github.GitHub(access_token=token)
    
    with sqlite3.connect(db) as rdb:
        rdb.row_factory = row_factory        
        for loader in LOADERS:
            rdb.cursor().execute(loader.schema)
            for r in repo:
                loader(hub, rdb, r)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback, pdb, sys
        traceback.print_exc()
        pdb.post_mortem(sys.exc_info()[-1])

