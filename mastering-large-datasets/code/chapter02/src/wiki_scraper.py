#
# Retrieving a Wikipedia page's network from its title
#

import json
import networkx as nx
from itertools import chain
from multiprocessing import Pool
from urllib import request, parse


GEPHI_FILENAME = './MyGraph.gexf'

WIKI_URL = """https://en.wikipedia.org/w/api.php?action=query&
prop=links|linkshere&pllimit=500&lhlimit=500&titles={}&
format=json&formatversion=2"""


def link_to_title(link):
  return link["title"]

def clean_if_key(page,key):
    if key in page.keys():
        return map(link_to_title,page[key])
    else: 
        return []

def get_wiki_links(pageTitle):
    safe_title = parse.quote(pageTitle)
    url = WIKI_URL.format(safe_title)
    page = request.urlopen(url).read()
    j = json.loads(page)
    jpage = j["query"]["pages"][0]
    inbound = clean_if_key(jpage,"links")
    outbound = clean_if_key(jpage,"linkshere")
    return {"title": pageTitle,
            "in-links":list(inbound),
            "out-links":list(outbound)}

# Flatten page's inbound and outbound links into one big list
def flatten_network(page):
    return page["in-links"]+page["out-links"]

# Represent each link as an edge between pages
def page_to_edges(page):
    a = [(page['title'],p) for p in page['out-links']]
    b = [(p,page['title']) for p in page['in-links']]
    return a+b

# Flatten our list of edges into single list
def flatten_edge_list(edges):
    return chain.from_iterable(edges)

# Create Gephi file for visualization
def create_gephi(edges, filename=GEPHI_FILENAME):
    G = nx.DiGraph()
    for e in edges:
        G.add_edge(*e)
    nx.readwrite.gexf.write_gexf(G, filename)

def process_wiki_network(title, create_graph=True):
    root = get_wiki_links(title)
    initial_network = flatten_network(root)
    with Pool() as P:
        all_pages = P.map(get_wiki_links, initial_network)
        edges = P.map(page_to_edges, all_pages)
    edges = flatten_edge_list(edges)
    if create_graph:
        create_gephi(edges)


# Updated run as executable
if __name__ == '__main__':
    process_wiki_network('Parallel_computing')