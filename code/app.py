import logging

from pathlib import Path

from frictionless import Pipeline, Resource, formats, steps, transform
from frictionless.resources import TableResource

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

def makeCSV(source, destination):
    logging.info("{src} => {dest}".format(src=file, dest=destination));

    resource = TableResource(path=source, format='tsv', encoding='utf-8')
    #pprint(resource.read_rows())

    resource.infer(stats=True)
    for i,field in enumerate(resource.schema.fields):
        resource.schema.set_field_type(field.name, 'string')
    #logging.info(resource.schema)

    target = TableResource(path=destination, control=formats.CsvControl(line_terminator="\n"))
    resource.write(target)

    #pipeline = Pipeline(steps=[steps.table_write(path=destination)])
    #logging.info(pipeline)

    #target = resource.transform(pipeline)
    #logging.info(target.read_rows())


files = Path(DATA_DIR).glob("**/dataURL/*.tsv")
file_list = list(files)

logging.info("num files is {num_files}".format(num_files=len(file_list)))

for file in file_list:
    filepath = str(file)
    destination = "{dir}{dest}".format(dir=OUTPUT_DIR, dest=filepath.replace('.tsv', '.csv'))
    makeCSV(source=filepath, destination=destination)
logging.info('Done!')