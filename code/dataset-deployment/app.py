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
        'tsv_ok_dd': row[14],
        'jgofs_ok_dd': row[15],
        'doc_ok_dd': row[16],
        'comment_ok_dd': row[17],
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
}

final_output_file = "{dir}/{file}".format(dir=OUTPUT_DIR, file=OUTPUT_FILE)
with open(final_output_file, 'w', newline='') as writefile:
    fieldnames = ['dataset_id', 'dd_id', 'object_name', 'tsv_ok_dd', 'jgofs_ok_dd', 'doc_ok_dd', 'comment_ok_dd', 'make_primary', 'output_file']
    writer = csv.DictWriter(writefile, fieldnames=fieldnames)

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
                destination = "{dir}/{dataset}/{dest}".format(dir=OUTPUT_DIR, dataset=data['dataset_id'], dest=filename)
                makeCSV(source=filepath, destination=destination)
                data['output_file'] = destination
                writer.writerow(data)
                

                """
                # path: _jgofs/output/20230414143635/datasets/data/2291/dataURL/adcp.csv
                json_path = data['path'].replace('_jgofs/output/20230414143635/datasets', '').replace('.csv', '.json')
                if os.path.exists(json_path):
                    with open(json_path) as f:
                        # read information about the TSV that was extracted
                        tsv_data = json.load(f)
                        dataset_id = tsv_data['dataset_id']
                        version = tsv_data['version']
                        logging.info("Dataset: {id}_v{v}".format(id=dataset_id, v=version))
                        drupalwriter.writerow([dataset_id, version, data['path'], data['filename'], data['bytesize'], data['md5'], data['mimetype'], data['aws_job_id'], data['aws_source_bucket'], data['aws_source_path'], data['url']])

                        continue
                        if 'downloads' in tsv_data and 'tsv' in tsv_data['downloads']:
                            status_code = tsv_data['downloads']['tsv']['status_code']
                            path = tsv_data['downloads']['tsv']['path']
                            if 200 == status_code:
                                logging.info("SUCCESS [{code}] {f}".format(code=status_code, f=os.path.basename(path)))
                                lookupDatadocs(path=path, dataset_id=dataset_id, version_id=version)
                            else:
                                logging.info("FAILURE [{code}] {f}".format(code=status_code, f=os.path.basename(path)))
                else:
                    logging.warning("Could not find JSON: {f}".format(f=json_path));
                    continue
                """

for d in NOT_FOUND:
    for item in NOT_FOUND[d]:
        logging.warning(d + ': ' + item)

logging.info('Done!')
