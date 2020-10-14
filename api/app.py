import pandas as pd
import numpy as np
import time
from elasticsearch import Elasticsearch, exceptions
from flask import Flask, jsonify, request, render_template
import os
import sys
import requests
from datetime import datetime

es = Elasticsearch(host='es01')


app = Flask(__name__)

@app.route('/')
def index():
    return("PhilHealth Claims Fraud and Abuse Risk Scoring System")

@app.route('/insert_data', methods=['POST'])
def insert_data():
    try:
        data = request.get_json()
        index = data['index']
        id = data['id']
        group = data['group']

        body = {
            'group': group,
            'timestamp': datetime.now()
        }
        result = es.index(index=index, id=id, doc_type='group', body=body)
        return (jsonify(result))

    


    except Exception as esc:
        return(jsonify('IO Error'))



@app.route('/score/', methods=['GET', 'POST'])
def get_data():
    try:
        data = request.get_json()

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
        if hci in hit1['Current Outliers']:
            hci_cv_score += 1
        if hci in hit2['Current Outliers']:
            hci_cp_score += 1
        if hci in hit3['Previous Outliers']:
            hci_pv_score += 1
        if hci in hit4['Previous Outliers']:
            hci_pp_score += 1

        # flag doctor outliers
        for i in hcp:
            if i in hit5['Current Outliers']:
                hcp_cv_score += 1
            if i in hit6['Current Outliers']:
                hcp_cp_score += 1
            if i in hit7['Previous Outliers']:
                hcp_pv_score += 1
            if i in hit8['Previous Outliers']:
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
        return(jsonify(s))
        # log score per series

    except Exception as esc:
        return(jsonify(0.00))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)


