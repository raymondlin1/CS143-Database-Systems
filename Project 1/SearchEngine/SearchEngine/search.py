#!/usr/bin/python3

import psycopg2
import re
import string
import sys
import base64
import hashlib
import codecs
from psycopg2 import sql

_PUNCTUATION = frozenset(string.punctuation)

def _remove_punc(token):
    """Removes punctuation from start/end of token."""
    i = 0
    j = len(token) - 1
    idone = False
    jdone = False
    while i <= j and not (idone and jdone):
        if token[i] in _PUNCTUATION and not idone:
            i += 1
        else:
            idone = True
        if token[j] in _PUNCTUATION and not jdone:
            j -= 1
        else:
            jdone = True
    return "" if i > j else token[i:(j+1)]

def _get_tokens(query):
    rewritten_query = []
    tokens = re.split('[ \n\r]+', query)
    for token in tokens:
        cleaned_token = _remove_punc(token)
        if cleaned_token:
            if "'" in cleaned_token:
                cleaned_token = cleaned_token.replace("'", "'")
            rewritten_query.append(cleaned_token)
    return rewritten_query



def search(query_type, offset, query):
    """TODO
    Your code will go here. Refer to the specification for projects 1A and 1B.
    But your code should do the following:
    1. Connect to the Postgres database.
    2. Graciously handle any errors that may occur (look into try/except/finally).
    3. Close any database connections when you're done.
    4. Write queries so that they are not vulnerable to SQL injections.
    5. The parameters passed to the search function may need to be changed for 1B. 
    """

    try:
        connection = psycopg2.connect(user="cs143",
                                  password="cs143",
                                  host="localhost",
                                  database="searchengine")
    except psycopg2.Error as e:
        print(e.pgerror)

    try:
        cursor = connection.cursor()
    except psycopg2.Error as e:
        print(e.pgerror)

    qtype = query_type.upper()

    tokens = _get_tokens(query)
    tokens = [t.lower() for t in tokens]
    num_tokens = len(tokens)
    print(tokens)
    
    if num_tokens == 0:
        raise ValueError("No results! Must enter a query!")

    start_clause = sql.SQL("""SELECT song_name, artist_name, Y.page_link FROM (
        SELECT L.song_id, SUM(R.score) as score
        FROM project1.token L
        JOIN project1.tfidf R
        ON L.song_id = R.song_id AND L.token = R.token""")

    where_clause = sql.Composed([sql.SQL(" WHERE L.token = "), sql.Literal(tokens[0])])
    for x in range(1,num_tokens):
        where_clause = where_clause + sql.Composed([sql.SQL(" OR L.token = "), sql.Literal(tokens[x])])

    group_clause = sql.SQL(" GROUP BY L.song_id")

    and_clause = sql.Composed([sql.SQL(" HAVING COUNT(L.song_id) = "), sql.Literal(num_tokens)])

    end_clause = sql.SQL("""
    ) X JOIN project1.song Y ON X.song_id = Y.song_id
        JOIN project1.artist Z ON Y.artist_id = Z.artist_id
        ORDER BY score DESC""")

    if qtype == "AND":
        sql_query = sql.Composed([start_clause, where_clause, group_clause, and_clause, end_clause])
    else:
        sql_query = sql.Composed([start_clause, where_clause, group_clause, end_clause])

    
    print(sql_query.as_string(cursor))
    print('\n')

    """START RAYMOND LIN
    Make a unique materialized view name"""
    view_name = query_type
    
    i = 0
    while i != len(tokens):
        view_name += "_" + tokens[i]
        i += 1

    view_name += "_query"
    print("View name: {view}".format(view = view_name))

    """create a hash for view_name"""
    view_hash = base64.urlsafe_b64encode(view_name.encode('utf-8')).decode('ascii')
    print(view_hash)
    view_int = int.from_bytes(base64.b64decode(view_hash), 'big')
    print(view_int)
    
    chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    view_int = abs(view_int)
    result = ''
    while view_int > 0:
        view_int, remainder = divmod(view_int, 36)
        result = chars[remainder] + result

    view_name = 'mv' + result.lower()
    print(view_name)
    
    """Delete existing views that aren't equal to current view"""
    view_lookup_query = sql.Composed([sql.SQL("SELECT relname FROM pg_class WHERE relname NOT LIKE 'tfidf' AND relname NOT LIKE "),
                                      sql.Literal(view_name),
                                      sql.SQL("AND relkind LIKE 'm';")])
    print(view_lookup_query.as_string(cursor))
    try:
        cursor.execute(view_lookup_query)
    except psycopg2.Error as e:
        print(e.pgerror)

    views = cursor.fetchall()
    for name in views:
        print(name[0])
        if name[0].strip('\)\(') != view_name:
            var = name[0].strip('\)\(')
            delete_query = sql.Composed([sql.SQL("DROP MATERIALIZED VIEW IF EXISTS project1."), sql.SQL(var), sql.SQL(";")])
            print(delete_query.as_string(cursor))
            try:
                cursor.execute(delete_query)
            except psycopg2.Error as e:
                print(e.pgerror)
            
    """Check if materialized view already exists"""
    check_query = sql.Composed([sql.SQL("SELECT 1 FROM pg_class WHERE relname LIKE "), sql.Literal(view_name), sql.SQL(";")])
    print(check_query.as_string(cursor))

    try:
        cursor.execute(check_query)
    except psycopg2.Error as e:
        print(e.pgerror)

    value = cursor.fetchone()
    connection.commit()
    connection.close()

    if value is not None and value[0] == 1:
        print("view was found!")
    else:
        print("view not found - creating one now")
        materialized_query = sql.Composed([sql.SQL("CREATE MATERIALIZED VIEW IF NOT EXISTS project1."), sql.SQL(view_name), sql.SQL(" AS "), sql_query, sql.SQL(";")])
        print(materialized_query.as_string(cursor))
        try:
            connection = psycopg2.connect(user="cs143",
                              password="cs143",
                              host="localhost",
                              database="searchengine")
        except psycopg2.Error as e:
            print(e.pgerror)
        
        try:
            cursor = connection.cursor()
        except psycopg2.Error as e:
            print(e.pgerror)

        try:
            cursor.execute(materialized_query)
        except psycopg2.Error as e:
            print(e.pgerror)

        connection.commit()
        connection.close()
        
    try:
        connection = psycopg2.connect(user="cs143",
                                  password="cs143",
                                  host="localhost",
                                  database="searchengine")
    except psycopg2.Error as e:
        print(e.pgerror)
    
    try:
        cursor = connection.cursor()
    except psycopg2.Error as e:
        print(e.pgerror)

    offset_str = str(offset)
    short_query = sql.Composed([sql.SQL("SELECT * FROM project1."), sql.SQL(view_name), sql.SQL(" LIMIT 20 OFFSET "), sql.SQL(offset_str), sql.SQL(";")])
    print(short_query.as_string(cursor))
    
    try:
        cursor.execute(short_query)
    except psycopg2.Error as e:
        print(e.pgerror)

    rows = []
    row = cursor.fetchall()
    for result in row:
        rows.append(result)

    length_query = sql.Composed([sql.SQL("SELECT COUNT(*) FROM project1."), sql.SQL(view_name), sql.SQL(";")])
    print(length_query.as_string(cursor))
    try:
        cursor.execute(length_query)
    except psycopg2.Error as e:
        print(e.pgerror)

    length = cursor.fetchone()
    rows.append(length)
        
    connection.close()
    return rows

if __name__ == "__main__":
    if len(sys.argv) > 2:
        result = search(sys.argv[1].lower(), sys.argv[2], ' '.join(sys.argv[3:]))
        print(result)
    else:
        print("USAGE: python3 search.py [or|and] term1 term2 ...")

