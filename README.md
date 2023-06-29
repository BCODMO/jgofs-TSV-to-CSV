tsv-to-csv

Convert TSV files from JGOFS into CSVs

# Running the script

1. Create a `.env` file in the top-level directory
    ```
    DATA_DIR={absolute path to the top-level data directory to process}
    OUTPUT_DIR={absolute path to the directory to store resulting CSV files}
    ```

2. Execute with: `docker-compose run --rm pipeline`

    - Logging: `log.txt` will be created at the top-level of the OUTPUT_DIR and logged to the console
