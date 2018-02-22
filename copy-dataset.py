#!/usr/bin/env python
# vim: set fileencoding=utf8 :

from __future__ import print_function

import argparse
import sys
import warnings

import concurrent.futures
import requests
from SPARQLWrapper import SPARQLWrapper, JSON
from concurrent.futures import ThreadPoolExecutor
from six.moves.urllib.parse import urlencode

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


class CopyTask(object):

    def __init__(self, src_url, dest_url, graph, verify=False):
        self.src_url = src_url
        self.dest_url = dest_url
        self.graph = graph
        self.verify = verify
        query = {'graph': graph}
        ue_query = urlencode(query)
        self.get_url = src_url + '?' + ue_query
        self.put_url = dest_url + '?' + ue_query
        self.data = None
        self.finished = False

    def run(self):
        if self.finished:
            return self

        if self.data is None:
            get_headers = {
                'accept': "text/turtle",
                'cache-control': "no-cache"
            }

            response = requests.request("GET", self.get_url, headers=get_headers, verify=self.verify)
            response.raise_for_status()
            self.data = response.content

            # DEBUG
            # print(type(data))
            # with open('out.dat', 'wb') as fd:
            #   fd.write(response.content)
            # print(response.text.encode('utf-8'), file=sys.stderr)

        # try:
        #     response = requests.request("DELETE", put_url, verify=False)
        #     response.raise_for_status()
        # except requests.HTTPError as e:
        #     pass

        put_headers = {
            'content-type': "text/turtle",
            'cache-control': "no-cache"
        }

        response = requests.request("PUT", self.put_url, headers=put_headers, verify=self.verify, data=self.data)
        response.raise_for_status()
        self.finished = True
        # print(get_url, put_url, file=sys.stderr)
        return self


def main():
    import ssl

    ssl._create_default_https_context = ssl._create_unverified_context


    parser = argparse.ArgumentParser(
        description="Copy RDF dataset",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--debug', help='debug mode', action="store_true")
    parser.add_argument('src_url', help='url of the source dataset')
    parser.add_argument('dest_url', help='url of the destination dataset')
    args = parser.parse_args()

    src_url = args.src_url
    dest_url = args.dest_url

    print("Source dataset:", src_url)
    print("Destination dataset:", dest_url)

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

    # get graphs
    l = len(graphs)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        progress(0, l, suffix='Prepare tasks')
        future_to_task = {}
        for i, g in enumerate(graphs):
            task = CopyTask(src_url, dest_url, g, verify=False)
            future = executor.submit(task.run)
            future_to_task[future] = task
            progress(i + 1, l, suffix='Prepare tasks')

        num_trials = 3
        trial = 0

        while future_to_task:
            trial += 1
            repeat_tasks = []
            l = len(future_to_task)
            j = 0
            for future in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[future]
                progress(j, l, suffix='Copy data [%i / %i] in graph %s        ' % (trial, num_trials, task.graph))
                try:
                    future.result()
                except Exception as exc:
                    print()
                    print('copy task for graph %s failed, exception: %s' % (task.graph, exc), file=sys.stderr)
                    repeat_tasks.append(task)
                else:
                    # print('task success: %s' % (data,), file=sys.stderr) # DEBUG
                    pass
                j += 1

            print()

            future_to_task.clear()
            if repeat_tasks and trial < num_trials:
                future_to_task = {executor.submit(task.run): task for task in repeat_tasks}

        print()
        if repeat_tasks:
            print('Could not copy data for following graphs:', file=sys.stderr)
            for task in repeat_tasks:
                print(task.graph, file=sys.stderr)
            return 1
        else:
            print('Successfuly copied all data')
            return 0


if __name__ == '__main__':
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        sys.exit(main())
