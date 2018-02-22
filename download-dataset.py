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
from six.moves.urllib.parse import quote_plus
import os
import os.path

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


class DownloadTask(object):

    def __init__(self, dataset_url, dest_dir, graph, verify=False):
        self.dataset_url = dataset_url
        self.dest_dir = dest_dir
        self.dest_file = os.path.join(dest_dir, quote_plus(graph))
        self.graph = graph
        self.verify = verify
        query = {'graph': graph}
        ue_query = urlencode(query)
        self.get_url = dataset_url + '?' + ue_query
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

            with open(self.dest_file, 'wb') as fd:
                fd.write(self.data)
        self.finished = True
        return self


def main():
    import ssl

    ssl._create_default_https_context = ssl._create_unverified_context

    parser = argparse.ArgumentParser(
        description="Download RDF dataset",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--debug', help='debug mode', action="store_true")
    parser.add_argument('url', help='url of the dataset')
    parser.add_argument('dest_dir', help='destination directory')
    args = parser.parse_args()

    dataset_url = args.url
    dest_dir = args.dest_dir

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    elif not os.path.isdir(dest_dir):
        print('Path', dest_dir, 'exist and is not a directory', file=sys.stderr)
        return 1

    print("Dataset URL:", dataset_url)
    print("Destination directory:", dest_dir)

    print('Getting graph list from {} ...'.format(dataset_url))
    src = SPARQLWrapper(dataset_url + '/sparql')
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
            task = DownloadTask(dataset_url, dest_dir, g, verify=False)
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
                progress(j, l, suffix='Download data [%i / %i] in graph %s        ' % (trial, num_trials, task.graph))
                try:
                    future.result()
                except Exception as exc:
                    print()
                    print('download task for graph %s failed, exception: %s' % (task.graph, exc), file=sys.stderr)
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
            print('Could not download data for following graphs:', file=sys.stderr)
            for task in repeat_tasks:
                print(task.graph, file=sys.stderr)
            return 1
        else:
            print('Successfuly downloaded all data')
            return 0


if __name__ == '__main__':
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        sys.exit(main())
