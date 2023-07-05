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
    logging.info("Attempting {src} => {dest}".format(src=file, dest=destination));

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
