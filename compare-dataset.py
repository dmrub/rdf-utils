#!/usr/bin/env python
# vim: set fileencoding=utf8 :

from __future__ import print_function

import sys
import warnings

import rdflib
from rdfutils import calc_hash_value
import hashlib
import concurrent.futures
import requests
from SPARQLWrapper import SPARQLWrapper, JSON
from concurrent.futures import ThreadPoolExecutor
from six.moves.urllib.parse import urlencode
import argparse

MAX_WORKERS = 5
try:
    import multiprocessing

    MAX_WORKERS = multiprocessing.cpu_count() * 5
except (ImportError, NotImplementedError):
    pass


# https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
# Print iterations progress
def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='#'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
    # Print New Line on Complete
    if iteration == total:
        print()


def progress(count, total, suffix=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()  # As suggested by Rom Ruben


def hash_graph(data, format, hash="sha256"):
    graph = rdflib.Graph()
    graph.parse(data=data, format=format)
    rdf_hash = calc_hash_value(graph)
    if hash != 'none':
        hash_func = hashlib.new(hash)
        hash_func.update(rdf_hash.encode('utf-8'))
        rdf_hash = hash_func.hexdigest()
    return rdf_hash


class CompareTask(object):

    def __init__(self, url1, url2, graph, verify=False):
        self.url1 = url1
        self.url2 = url2
        self.graph = graph
        self.verify = verify
        query = {'graph': graph}
        ue_query = urlencode(query)
        self.get_url1 = url1 + '?' + ue_query
        self.get_url2 = url2 + '?' + ue_query
        self.data1 = None
        self.data2 = None
        self.finished = False
        self.hash1 = None
        self.hash2 = None

    def run(self):
        if self.finished:
            return self

        get_headers = {
            'accept': "text/turtle",
            'cache-control': "no-cache"
        }

        if self.data1 is None:
            response = requests.request("GET", self.get_url1, headers=get_headers, verify=self.verify)
            response.raise_for_status()
            self.data1 = response.content
            self.hash1 = hash_graph(self.data1, format="turtle")

        assert self.hash1 is not None

        if self.data2 is None:
            response = requests.request("GET", self.get_url2, headers=get_headers, verify=self.verify)
            response.raise_for_status()
            self.data2 = response.content
            self.hash2 = hash_graph(self.data2, format="turtle")

        assert self.hash2 is not None

        self.finished = True
        return self


def main():
    import ssl

    ssl._create_default_https_context = ssl._create_unverified_context

    parser = argparse.ArgumentParser(
        description="Compare RDF datasets",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--debug', help='debug mode', action="store_true")
    parser.add_argument('url1', metavar='URL1', help='url of the first dataset')
    parser.add_argument('url2', metavar='URL2', help='url of the second dataset')
    args = parser.parse_args()

    src_url = args.url1
    dest_url = args.url2

    print("Dataset 1:", src_url)
    print("Dataset 2:", dest_url)

    print('Getting graph list from {} ...'.format(src_url))
    src = SPARQLWrapper(src_url + '/sparql')
    src.setQuery("""SELECT DISTINCT ?g
    WHERE {
      GRAPH ?g { ?s ?p ?o }
    }""")
    src.setReturnFormat(JSON)

    qr = src.query().convert()
    graphs = []
    has_default = False
    for result in qr["results"]["bindings"]:
        g = result["g"]["value"]
        graphs.append(g)
        has_default = has_default or g == 'default'

    if not has_default:
        graphs.append('default')

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        l = len(graphs)

        future_to_task = {}
        for i, g in enumerate(graphs):
            progress(i + 1, l, suffix='Prepare tasks')
            task = CompareTask(src_url, dest_url, g, verify=False)
            future = executor.submit(task.run)
            future_to_task[future] = task

        num_trials = 3
        trial = 0

        done_tasks = []
        while future_to_task:
            trial += 1
            repeat_tasks = []
            l = len(future_to_task)
            j = 0
            for future in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[future]
                progress(j, l, suffix='Compare data [%i / %i] in graph %s        ' % (trial, num_trials, task.graph))
                try:
                    done_tasks.append(future.result())
                except Exception as exc:
                    print()
                    print('comparison task for graph %s failed, exception: %s' % (task.graph, exc), file=sys.stderr)
                    repeat_tasks.append(task)
                else:
                    # print('task success: %s' % (data,), file=sys.stderr) # DEBUG
                    pass
                j += 1
            progress(j, l, suffix='Compare data [%i / %i] in graph %s        ' % (trial, num_trials, task.graph))

            print()

            future_to_task.clear()
            if repeat_tasks and trial < num_trials:
                future_to_task = {executor.submit(task.run): task for task in repeat_tasks}

        print()
        result = 0
        if repeat_tasks:
            print('Could not compare data for following graphs:', file=sys.stderr)
            for task in repeat_tasks:
                print(task.graph, file=sys.stderr)
            result = 1
        else:
            print('Successfuly compared all data')
        num_equal = 0
        num_diff = 0
        for task in done_tasks:
            if task.hash1 == task.hash2:
                num_equal += 1
            else:
                num_diff += 1
                print('Data for graph {} have different hashes: {} != {}'.format(task.graph, task.hash1, task.hash2),
                      file=sys.stderr)

        print('Equal graphs:', num_equal)
        print('Different graphs:', num_diff)
        return result


if __name__ == '__main__':
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        sys.exit(main())
