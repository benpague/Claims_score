import pandas as pd
import numpy as np
import time
from elasticsearch import Elasticsearch, exceptions
from flask import Flask, jsonify, request, render_template
import os
import sys
import requests

es = Elasticsearch(host='es')

if es.ping():
    print('connected to ES')

app = Flask(__name__)

@app.route('/')
def index():
    return("PhilHealth Claims Fraud and Abuse Risk Scoring System")

@app.route('/load_data', methods=['POST'])
def load_data_in_es():
    """ creates an index in elasticsearch """
    url = "http://data.sfgov.org/resource/rqzj-sfat.json"
    r = requests.get(url)
    data = r.json()
    print("Loading data in elasticsearch ...")
    for id, truck in enumerate(data):
        res = es.index(index="sfdata", doc_type="truck", id=id, body=truck)
    print("Total trucks loaded: ", len(data))


@app.route('/score/', methods=['GET', 'POST'])
def get_data():
    try:
        data = request.get_json()
        series = data['series']
        grp = data['id']
        hci = data['hci']
        hcp = data['hcp']

        # metric scores
        hci_cv_score = 0
        hci_cp_score = 0
        hci_pv_score = 0
        hci_pp_score = 0
        hcp_cv_score = 0
        hcp_cp_score = 0
        hcp_pv_score = 0
        hcp_pp_score = 0

        # metric importance factor
        m1 = 1
        m2 = 1
        m3 = 1
        m4 = 1
        m5 = 1
        m6 = 1
        m7 = 1
        m8 = 1

        # elasticsearch
        res1 = es.search(index='hci_cv', body={'query': {'match': {'_id': grp}}})
        res2 = es.search(index='hci_cp', body={'query': {'match': {'_id': grp}}})
        res3 = es.search(index='hci_pv', body={'query': {'match': {'_id': grp}}})
        res4 = es.search(index='hci_pp', body={'query': {'match': {'_id': grp}}})
        res5 = es.search(index='hcp_cv', body={'query': {'match': {'_id': grp}}})
        res6 = es.search(index='hcp_cp', body={'query': {'match': {'_id': grp}}})
        res7 = es.search(index='hcp_pv', body={'query': {'match': {'_id': grp}}})
        res8 = es.search(index='hcp_pp', body={'query': {'match': {'_id': grp}}})

        hit1 = res1['hits']['hits'][0]['_source']
        hit2 = res2['hits']['hits'][0]['_source']
        hit3 = res3['hits']['hits'][0]['_source']
        hit4 = res4['hits']['hits'][0]['_source']
        hit5 = res5['hits']['hits'][0]['_source']
        hit6 = res6['hits']['hits'][0]['_source']
        hit7 = res7['hits']['hits'][0]['_source']
        hit8 = res8['hits']['hits'][0]['_source']

        # flag hospital outliers
        if hci in hit1['current outliers']:
            hci_cv_score += 1
        if hci in hit2['current outliers']:
            hci_cp_score += 1
        if hci in hit3['previous outliers']:
            hci_pv_score += 1
        if hci in hit4['previous outliers']:
            hci_pp_score += 1

        # flag doctor outliers
        for i in hcp:
            if i in hit5['current outliers']:
                hcp_cv_score += 1
            if i in hit6['current outliers']:
                hcp_cp_score += 1
            if i in hit7['previous outliers']:
                hcp_pv_score += 1
            if i in hit8['previous outliers']:
                hcp_pp_score += 1
        if hcp_cv_score >= 1:
            hcp_cv_score = 1
        if hcp_cp_score >= 1:
            hcp_cp_score = 1
        if hcp_pv_score >= 1:
            hcp_pv_score = 1
        if hcp_pp_score >= 1:
            hcp_pp_score = 1

        # provider score for potential fraud
        s = (hci_cv_score * m1 + hci_cp_score * m2 + hci_pv_score * m3 + hci_pp_score * m4 + hcp_cv_score * m5 + hcp_cp_score * m6 + hcp_pv_score * m7 + hcp_pp_score * m8) / 8
        return (jsonify(s))
    # log score per series

    except Exception as esc:
        return (jsonify('Disease Group Code Error'))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)


