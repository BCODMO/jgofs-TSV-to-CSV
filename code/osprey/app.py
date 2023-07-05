import csv
import json
import logging
import os
import pprint
import requests

from pathlib import Path

from frictionless import Pipeline, Resource, formats, steps, transform
from frictionless.resources import TableResource

CSV_INFO = 'jgofs_0630_final.csv'
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

def buildCsvInfo(row):
    #row,filename,path,mimeType,fileType,job_id,aws_path,Status,Error,DatadocsURL,bytesize,checksum
    return {
        'row_id': row[0],
        'filename': row[1],
        'path': row[2],
        'mimetype': row[3],
        'aws_job_id': row[5],
        'aws_source_bucket': 'bcodmo-files-prod',
        'aws_source_path': row[6],
        'url': row[9],
        'bytesize': row[10],
        'md5': row[11],
    }


def lookupDatadocs(path, dataset_id, version_id):
    res = requests.get("https://www.bco-dmo.org/api/dataset/{id}".format(id=dataset_id))
    dataset = res.json()
    graph_id = dataset['@id'].replace('#graph', '')
    try:
        current_version = dataset['@graph'][0][graph_id]['http://ocean-data.org/schema/versionLabel'][0]['@value']
        if str(current_version) == str(version_id):
            logging.info("Version match: {v}".format(v=current_version))
        else:
            logging.warning("Version mismatch: {v}".format(v=current_version))
        
        # lookup in the CSV
        # datadocs URL
        # filename
        # bytesize
        # checksum
        # aws_job_id
        # aws_object_id
        # aws_source_bucket
        # aws_source_path
        return #the AWS info
    except Exception as e:
        logging.exception(e)

    return None


json_files = Path(DATA_DIR).glob("**/dataURL/*.json")
file_list = list(json_files)
print(file_list)

logging.info("num files is {num_files}".format(num_files=len(file_list)))

with open("{dir}/import_jgofs_06-30-2023.csv".format(dir=OUTPUT_DIR), 'w') as drupal_output:
    drupalwriter = csv.writer(drupal_output)
    fieldnames = ['dataset_id', 'dataset_version', 'tsv_path', 'filename', 'bytesize', 'md5', 'mimetype', 'aws_job_id', 'aws_source_bucket', 'aws_source_path', 'datadocs_url']
    drupalwriter.writerow(fieldnames)

    # read the AWS check-in file
    with open(CSV_INFO) as csvfile:
        spreadsheet = csv.reader(csvfile)
        skipped_headers = False
        for row in spreadsheet:
            data = buildCsvInfo(row)
            if False == skipped_headers:
                skipped_headers = True
                continue
            else:
                # path: _jgofs/output/20230414143635/datasets/data/2291/dataURL/adcp.csv
                json_path = data['path'].replace('_jgofs/output/20230414143635/datasets', '').replace('.csv', '.json')
                if os.path.exists(json_path):
                    with open(json_path) as f:
                        # read information about the TSV that was extracted
                        tsv_data = json.load(f)
                        dataset_id = tsv_data['dataset_id']
                        version = tsv_data['version']
                        logging.info("Dataset: {id}_v{v}".format(id=dataset_id, v=version))
                        drupalwriter.writerow([dataset_id, version, data['path'], data['filename'], data['bytesize'], data['md5'], data['filename'], data['mimetype'], data['aws_job_id'], data['aws_source_bucket'], data['aws_source_path'], data['url']])
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
                    continue

logging.info('Done!')