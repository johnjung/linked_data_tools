#!/usr/bin/env python
"""Usage:
    query_marklogic <collection>
"""

# e.g.,
# python linked_data_collection_diff.py http://lib.uchicago.edu/digital_collections/maps/chisoc

# TODO-
#   - store collection-level triples on the OCFL side.

import sqlite3, os, requests, rdflib, rdflib.compare, sys
from docopt import docopt
from requests.auth import HTTPBasicAuth

def ocfl_collection_triples(collection):
    return ''

def ocfl_item_triples(ark_identifier):
    return requests.get('http://ark.lib.uchicago.edu/{}/file.ttl'.format(ark_identifier)).text

def ocfl_ark_identifiers(collection):
    conn = sqlite3.connect('/data/s4/jej/ark_data.db')
    c = conn.cursor()

    c.execute('''
    SELECT ark
    FROM arks
    WHERE project='{}'
    '''.format(collection))

    return [r[0] for r in c.fetchall()]

def ocfl_collection_graph(collection):
    graph = rdflib.Graph()
    for i in ocfl_ark_identifiers(collection):
        graph.parse(data=ocfl_item_triples(i), format='n3')
    graph.parse(data=ocfl_collection_triples(collection), format='n3')
    return graph

def marklogic_collection_graph(collection):
    query = '''
    DESCRIBE *
    FROM <{}>
    WHERE {{ 
       ?s ?p ?o
    }}
    '''.format(collection).encode('utf-8')

    triples_str = requests.post(
        auth=HTTPBasicAuth(
            os.environ['MARKLOGIC_LDR_USER'],
            os.environ['MARKLOGIC_LDR_PASSWORD']
        ),  
        data=query,
        headers={
            'Accept': 'application/n-triples',
            'Content-type': 'application/sparql-query' 
        },
        url='http://marklogic.lib.uchicago.edu:8008/v1/graphs/sparql'
    ).content.decode('utf-8')

    return rdflib.Graph().parse(data=triples_str, format='n3')

if __name__=="__main__":
    options = docopt(__doc__)

    m_iso = rdflib.compare.to_isomorphic(
        marklogic_collection_graph(options['<collection>'])
    )
    o_iso = rdflib.compare.to_isomorphic(
        ocfl_collection_graph(options['<collection>'])
    )
    in_both, in_m, in_o = rdflib.compare.graph_diff(m_iso, o_iso)

    print('{} triples common to both.'.format(len(in_both)))
    print('{} triples in marklogic only.'.format(len(in_m)))
    print('{} triples in OCFL only.'.format(len(in_o)))
