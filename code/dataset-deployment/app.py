import csv
import json
import logging
import os
import pprint
import requests

from pathlib import Path

from frictionless import Pipeline, Resource, formats, steps, transform
from frictionless.resources import TableResource

DATASET_IDS = 'dataset-ids.csv'
DEPLOYMENTS_IDS = 'dataset-deployments.csv'
CSV_INFO = 'QA_dd_transition-as_data_file.csv'

OUTPUT_FILE = 'dataset-deployment-manifest.csv'

DATA_DIR = '/data/'
OUTPUT_DIR = '/output'


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(filename='/output/log.txt', mode='a'),
        logging.StreamHandler()
    ]
)

def readManifest(row):
    #dataset_id,dd_id,downloads_dd,filter_params_dd,jgofs_object_relpath_dd,jgofs_server,jgofs_url_dd,matches_dataURL,object_id,object_name,object_source,same_object,timestamp_captured,validated,tsv_ok_dd,jgofs_ok_dd,doc_ok_dd,comment_ok_dd,jgofs_url_orig,has_dataset_dataURL,jgofs_url_dataset,jgofs_object_relpath_dataset,version,state,doi,filter_params_dataset,strategy,make_primary,downloads_object,tsv_ok_object,jgofs_ok_object,doc_ok_object,comment_ok_object
    return {
        'dataset_id': row[0],
        'dd_id': row[1],
        'object_name': row[9],
        'tsv_ok_dd': row[14],       # if False, print and continue
        'jgofs_ok_dd': row[15],
        'doc_ok_dd': row[16],
        'comment_ok_dd': row[17],   # if True, add this file as the file description
        'make_primary': row[27],
    }


def makeCSV(source, destination):
    logging.info("Attempting {src} => {dest}".format(src=source, dest=destination));

    try:
        # Read in the TSV file using UTF-8 encoding
        resource = TableResource(path=source, format='tsv', encoding='utf-8')
        #logging.debug(resource.read_rows())

        #Infer the schema so the code knows what the fields are
        resource.infer(stats=True)
        # Force all columns to be interpreted as string
        for i,field in enumerate(resource.schema.fields):
            resource.schema.set_field_type(field.name, 'string')
        #logging.debug(resource.schema)

        # Write the file as CSV with LF line endings
        target = TableResource(path=destination, control=formats.CsvControl(line_terminator="\n"))
        resource.write(target)
    except Exception as e:
        logging.exception(e)
        raise e


#read the dataset-ids file
DATASETS = {}
with open(DATASET_IDS) as datasetfile:
    spreadsheet = csv.reader(datasetfile)
    skipped_headers = False
    for row in spreadsheet:
        if False == skipped_headers:
            skipped_headers = True
            continue
        else:
            if row[0] not in DATASETS:
                DATASETS[row[0]] = {}
            DATASETS[row[0]][row[1]] = {'name': row[2]}

#read the dataset-ids file
DEPLOYMENTS = {}
with open(DEPLOYMENTS_IDS) as ddfile:
    spreadsheet = csv.reader(ddfile)
    skipped_headers = False
    for row in spreadsheet:
        if False == skipped_headers:
            skipped_headers = True
            continue
        elif row[0] not in DEPLOYMENTS:
                DEPLOYMENTS[row[0]] = {'name': row[1]}

NOT_FOUND = {
  'msg': [],
  'dataset': [],
  'dataset-deployment': [],
  'deployment': [],
  'errors': [],
}

final_output_file = "{dir}/{file}".format(dir=OUTPUT_DIR, file=OUTPUT_FILE)
with open(final_output_file, 'w', newline='') as writefile:
    fieldnames = [
        'dataset_id', 
        'dd_id', 
        'object_name', 
        'make_primary', 
        'output_file',
        'description',
    ]
    writer = csv.DictWriter(writefile, fieldnames=fieldnames)
    writer.writeheader()
    
    # read the dataset deployment file
    with open(CSV_INFO) as csvfile:
        spreadsheet = csv.reader(csvfile)
        skipped_headers = False
        for row in spreadsheet:
            data = readManifest(row)
            if False == skipped_headers:
                skipped_headers = True
                continue
            else:

                # Is tsv file OK to use?
                if data['tsv_ok_dd'] == 'False':
                    logging.warning("**** BAD TSV for Dataset: {d}, Dataset-Deployment: {dd}, Object: {o}".format(d=data['dataset_id'], dd=data['dd_id'], o=data['object_name']))
                    continue

                deployment_name = None
                if data['dataset_id'] not in DATASETS:
                    if data['dataset_id'] not in NOT_FOUND['dataset']:
                        NOT_FOUND['dataset'].append(data['dataset_id'])
                        NOT_FOUND['msg'].append('DATASET not found: ' + data['dataset_id'])

                    if data['dd_id'] not in DEPLOYMENTS:
                        if data['dd_id'] not in NOT_FOUND['deployment']:
                            NOT_FOUND['deployment'].append(data['dd_id'])
                            NOT_FOUND['msg'].append('DEPLOYMENT not found: ' + data['dd_id'])
                        continue
                    else:
                        deployment_name = DEPLOYMENTS[data['dd_id']]['name']
                else:
                    if data['dd_id'] not in DATASETS[data['dataset_id']]:
                        if data['dd_id'] not in NOT_FOUND['dataset-deployment']:
                            NOT_FOUND['dataset-deployment'].append(data['dd_id'])
                            NOT_FOUND['msg'].append('DATASET-DEPLOYMENT not found: ' + data['dd_id'] + ' in Dataset: ' + data['dataset_id'])
                        
                        if data['dd_id'] not in DEPLOYMENTS:
                            if data['dd_id'] not in NOT_FOUND['deployment']:
                                NOT_FOUND['deployment'].append(data['dd_id'])
                                NOT_FOUND['msg'].append('DEPLOYMENT not found: ' + data['dd_id'])
                            continue
                        else:
                            deployment_name = DEPLOYMENTS[data['dd_id']]['name']

                    else:
                        deployment_name = DATASETS[data['dataset_id']][data['dd_id']]['name']


                # get the filename
                filename = "{object}_{deployment}.csv".format(object=data['object_name'], deployment=deployment_name)

                # convert to CSV
                logging.info("OBJECT: {o}".format(o=data['object_name']))
                filepath = "{dir}{dataset_id}/dataset_deployment/{dd_id}/{object}.tsv".format(dir=DATA_DIR, dataset_id=data['dataset_id'], dd_id=data['dd_id'], object=data['object_name'])
                
                destination = "{dir}/{dataset}/dataset_deployment/{dd_id}/{dest}".format(dir=OUTPUT_DIR, dataset=data['dataset_id'], dd_id=data['dd_id'], dest=filename)
                makeCSV(source=filepath, destination=destination)
                data['output_file'] = destination

                # figure out the description
                found_comment = False
                if data['comment_ok_dd'] == 'True':
                    commentfile_content = None
                    commentfile = "{dir}{dataset_id}/dataset_deployment/{dd_id}/{object}.datacomment".format(dir=DATA_DIR, dataset_id=data['dataset_id'], dd_id=data['dd_id'], object=data['object_name'])
                    if os.path.exists(commentfile):
                        with open(commentfile, 'r') as comment_f:
                            commentfile_content = comment_f.read()
                        data['description'] = commentfile_content
                        found_comment = True
                    else:
                        logging.warning("*** COMMENT FILE COULD NOT BE FOUND: {f}".format(f=commentfile))
                
                if False == found_comment:
                    logging.warning("NO description for file: {f}".format(f=filepath))
                    data['description'] = ''

                unset(data['tsv_ok_dd'])
                unset(data['jgofs_ok_dd'])
                unset(data['doc_ok_dd'])
                unset(data['comment_ok_dd'])

                writer.writerow(data)
                
"""
for d in NOT_FOUND:
    for item in NOT_FOUND[d]:
        logging.warning(d + ': ' + item)
"""

logging.info('Done!')
