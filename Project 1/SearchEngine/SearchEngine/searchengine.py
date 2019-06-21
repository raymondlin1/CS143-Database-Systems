#!/usr/bin/python3

from flask import Flask, render_template, request

import search

application = app = Flask(__name__)
app.debug = True

@app.route('/search', methods=["GET"])
def dosearch():
    """
    TODO:
    Use request.args to extract other information
    you may need for pagination.
    """

    query = request.args['query']
    qtype = request.args['query_type']

    """print("Page number: {page}".format(page = request.args.get('page')))"""
    page = request.args.get('page', 1)
    page = int(page)
    offset = (page - 1) * 20

    next = page + 1
    previous = page - 1
    if previous < 1:
        previous = 1
    
    """if len(tokens) > 1:
        for x in range(1, len(query)):
            tokens = tokens + "+{next_tk}".format(next_tk = query[x])"""
    
    url = "/search?query_type={x}&query={y}".format(x = qtype, y = query)


    search_results = search.search(qtype, offset, query)

    length = int(search_results[-1][0])
    del search_results[-1]

    lower_bound = offset + 1
    upper_bound = lower_bound + 19
    if upper_bound > length:
        upper_bound = length

    return render_template('results.html',
            query=query,
            query_type=qtype,
            results=length,
            search_results=search_results,
            x=lower_bound,
            y=upper_bound,
            page=page,
            url=url)

@app.route("/", methods=["GET"])
def index():
    if request.method == "GET":
        pass
    return render_template('index.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0')
