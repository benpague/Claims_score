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
    return ("PhilHealth Claims Fraud and Abuse Risk Scoring System")


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
        return (jsonify('IO Error'))


@app.route('/score/', methods=['GET', 'POST'])
def get_data():
    try:
        data = request.get_json()

        grp = data['id']
        hci = data['hci']
        hcp = data['hcp']
        los = data['los']

        # Initialize metric scores to zero
        hci_v_score = 0
        hci_p_score = 0
        hcp_v_score = 0
        hcp_p_score = 0
        los_score = 0
        other_flags_score = 0

        # metric importance factor, 1 by default
        m1 = 1
        m2 = 1
        m3 = 1
        m4 = 1
        m5 = 1
        m6 = 1

        # number of metrics used
        num_metrics = 6

        # query elasticsearch database
        res1 = es.search(index='hci_v', body={'query': {'match': {'_id': grp}}})
        res2 = es.search(index='hci_p', body={'query': {'match': {'_id': grp}}})
        res3 = es.search(index='hcp_v', body={'query': {'match': {'_id': grp}}})
        res4 = es.search(index='hcp_p', body={'query': {'match': {'_id': grp}}})
        res5 = es.search(index='los', body={'query': {'match': {'_id': grp}}})
        res6 = es.search(index='other_flags', body={'query': {'match': {'_id': 'priority_grp'}}})

        hit1 = res1['hits']['hits'][0]['_source']
        hit2 = res2['hits']['hits'][0]['_source']
        hit3 = res3['hits']['hits'][0]['_source']
        hit4 = res4['hits']['hits'][0]['_source']
        hit5 = res5['hits']['hits'][0]['_source']
        hit6 = res6['hits']['hits'][0]['_source']

        # flag hospital outliers
        if hci in hit1['group']['Outliers']:
            hci_v_score += 1
        if hci in hit2['group']['Outliers']:
            hci_p_score += 1

        # flag doctor outliers
        for i in hcp:
            if i in hit3['group']['Outliers']:
                hcp_v_score += 1
            if i in hit4['group']['Outliers']:
                hcp_p_score += 1

        if hcp_v_score >= 1:
            hcp_v_score = 1
        if hcp_p_score >= 1:
            hcp_p_score = 1

        # flag LOS outliers
        if 0 < los < hit5['group']:
            los_score = 1

        # flag other_flags
        if grp in hit6['group']['priority_grp']:
            other_flags_score += 1

        # provider score for potential fraud
        s = (hci_v_score * m1 + hci_p_score * m2 + hcp_v_score * m3 + hcp_p_score * m4 + los_score * m5 + other_flags_score * m6) / num_metrics
        return jsonify(s)




    except Exception as esc:
        return jsonify(esc)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
