#!/usr/bin/env python

# pip install jupyterlab
# pip install notebook

# pip install google-api-python-client
# pip install google-api-core
# pip install urllib3[secure] httplib2shim
# pip install python-dateutil

# https://cloud.google.com/dataflow/docs/reference/rest
# https://developers.google.com/resources/api-libraries/documentation/dataflow/v1b3/python/latest/index.html

import os
import json
import socket
import logging
import concurrent.futures
import dateutil.parser
import multiprocessing
import argparse

from dateutil.tz import tzutc

import httplib2shim
httplib2shim.patch()

from googleapiclient.discovery import build
from google.api_core.exceptions import GoogleAPIError
from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest

logging.basicConfig(format='[%(asctime)s %(levelname)s] - %(message)s', level=logging.INFO)

def dataflow_debug(service_jobs, projectId, location, jobId):
    return service_jobs.debug().getConfig(
            projectId=projectId,
            location=location,
            jobId=jobId
    ).execute()


def dataflow_messages(service_jobs, projectId, location, jobId):
    messages, pageToken = list(), None
    
    while True:
        response = service_jobs.messages().list(
                projectId=projectId,    
                location=location,
                jobId=jobId,
                pageSize=1000,
                pageToken=pageToken,
                minimumImportance='JOB_MESSAGE_DEBUG'
        ).execute()
    
        messages.append(response)

        pageToken = response.get('nextPageToken') if 'nextPageToken' in response else None
        if not pageToken: break
    
    return messages


def dataflow_snapshots(service_jobs, projectId, location, jobId):
    return service_jobs.snapshots().list(
            projectId=projectId,
            location=location,
            jobId=jobId
    ).execute()


def dataflow_workItems(service_jobs, projectId, location, jobId):
    return service_jobs.workItems().reportStatus(
            projectId=projectId,
            location=location,
            jobId=jobId
    ).execute()


def dataflow_metrics(service_jobs, projectId, location, jobId):
   return service_jobs.getMetrics(
           projectId=projectId,
           location=location,
           jobId=jobId
    ).execute()


def dataflow_details(service_jobs, projectId, location, jobId):
    return service_jobs.get(
            projectId=projectId,
            location=location,
            jobId=jobId,
            view='JOB_VIEW_ALL'
    ).execute()


def dataflow_job(projectId, location, jobId, page):
    logging.info('page = %s, jobId = %s', page, jobId)

    service = build('dataflow', 'v1b3', cache_discovery=False)
    service_jobs = service.projects().locations().jobs()

    args = (service_jobs, projectId, location, jobId)
    
    functions = {
        'debug' : dataflow_debug,
        'messages' : dataflow_messages,
        'snapshots' : dataflow_snapshots,
        'workItem' : dataflow_workItems,
        'metrics' : dataflow_metrics,
        'details' : dataflow_details,
    }
     
    return { k : v(*args) for k,v in functions.items() }


def dataflow_jobs_filter(jobs, datefrom, dateto):
    if datefrom or dateto:
        early_stop, filtered_jobs= False, list()

        for job in jobs: 
            createTime = dateutil.parser.parse(job.get('createTime'))

            if datefrom and dateto:
                if createTime >= datefrom and createTime <= dateto:
                    filtered_jobs.append(job)

            elif datefrom:
                if createTime >= datefrom:
                    filtered_jobs.append(job)
            
            elif dateto:
                if createTime <= dateto:
                    filtered_jobs.append(job)

            if createTime < datefrom:
                early_stop = True
                break
                    
        return early_stop, filtered_jobs
    else:
        return False, jobs

def dataflow_jobs_write(projectId, location, file, limit=0, datefrom=None, dateto=None):
    pages, pageToken, page_counter = list(), None, 0

    service = build('dataflow', 'v1b3', cache_discovery=False)
    service_jobs = service.projects().locations().jobs()

    pages = []

    while True:
        if limit and page_counter >= limit: break
        
        page_counter += 1

        logging.info('-'*80)
        logging.info('getting jobs from page (%s) token = \'%s\'', page_counter, pageToken)

        response = service_jobs.list(projectId=projectId,
                                     location=location,
                                     view='JOB_VIEW_ALL',
                                     pageToken=pageToken).execute()
      
        jobs = response.get('jobs')
        logging.info('received total jobs = %d', len(jobs))

        early_stop, jobs = dataflow_jobs_filter(jobs, datefrom, dateto)
        logging.info('received total filtered = %d', len(jobs))
        
        response['jobs'] = jobs
        pages.append(response)

        pageToken = response.get('nextPageToken')
        if pageToken or early_stop: break
   
    logging.info('*'*80)
    logging.info('writing %d result pages', page_counter)
    json.dump(pages, file, indent=4)


def dataflow_jobs_read(projectId, location):
    jobs, pageToken, page_counter = list(), None, 0

    while True:
        logging.info('-'*80)

        page_counter += 1
        page_current = '{}'.format(page_counter).zfill(3)

        logging.info('getting jobs from page (%s) token = \'%s\'', page_current, pageToken)

        service = build('dataflow', 'v1b3', cache_discovery=False)
        service_jobs = service.projects().locations().jobs()
        
        response = service_jobs.list(projectId=projectId,
                                     location=location,
                                     view='JOB_VIEW_ALL',
                                     pageToken=pageToken).execute()
      
        jobs = response.get('jobs')
        logging.info('received total jobs = %d', len(jobs))

        with concurrent.futures.ProcessPoolExecutor(max_workers=100) as executor:
            futures = {
                    executor.submit(
                        dataflow_job,
                            projectId,
                            location,
                            job.get('id'),
                            page_current
                    ) : job.get('id') for job in jobs
            }

            responses = [future.result() for future in concurrent.futures.as_completed(futures)]

            filename = 'us-east-1-dataflow-atejeda/dataflow/{}_dataflow_{}.json'.format(
                    projectId,
                    page_current
            )
            logging.info('writing results into %s', filename)
            json.dump(jobs, open(filename, 'w'), indent=4)

        #pageToken = response.get('nextPageToken') if 'nextPageToken' in response else None
        if not pageToken: break

    return jobs

def string_date(arg0):
    return dateutil.parser.parse(arg0).replace(tzinfo=tzutc())

def valid_directory(arg0):
    if os.path.isdir(arg0): return arg0
    raise argparse.ArgumentTypeError('directory doesn\'t exists')

def index_mode(args):
    projectId = args.project
    location = args.location
    file = args.file
    limit = args.limit
    datefrom = args.datefrom
    dateto = args.dateto

    dataflow_jobs_write(projectId, location, file, limit, datefrom, dateto)
    

def full_mode(args):
    pass

if __name__ == '__main__':
    socket.setdefaulttimeout(600)

    parser = argparse.ArgumentParser(description='Get dataflow job info')

    # common arguments
    
    parser.add_argument('--project',
                        help='project id',
                        required=True)

    parser.add_argument('--location',
                        help='location (region)',
                        required=True)

    # index mode arguments

    subparsers = parser.add_subparsers(help='sub-command help')

    index_group = subparsers.add_parser('index', help='index mode help')
    index_group.set_defaults(func=index_mode)
    
    index_group.add_argument('--file',
                        help='the file to write write',
                        type=argparse.FileType('w', encoding='UTF-8'),
                        required=True)
    
    index_group.add_argument('--limit',
                        help='limit the result to N pages',
                        type=int,
                        default=0)

    index_group.add_argument('--datefrom',
                        help='datefrom >= job.createTime',
                        type=string_date,
                        default=None)

    index_group.add_argument('--dateto',
                        help='dateto <= job.createTime',
                        type=string_date,
                        default=None)

    # full mode arguments

    full_group = subparsers.add_parser('full', help='full mode help')
    full_group.set_defaults(func=full_mode)
    
    full_group.add_argument('--output',
                        help='the folder path where to write the results',
                        type=valid_directory,
                        required=True)

    full_group.add_argument('--jobs',
                        help='number of concurrent jobs',
                        type=int,
                        default=multiprocessing.cpu_count())

    # options
    
    args = parser.parse_args()
    args.func(args)
