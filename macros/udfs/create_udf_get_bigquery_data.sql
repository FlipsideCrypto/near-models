{% macro create_UDF_GET_BIGQUERY_DATA(
    project,
    dataset,
    table,
    gcp_secret_name,
    destination_bucket,
    destination_path,
    start_date,
    end_date
) %}
    {% set sql %}
        CREATE OR REPLACE FUNCTION {{ target.database }}.STREAMLINE.UDF_GET_BIGQUERY_DATA(
            project STRING,
            dataset STRING,
            table STRING,
            gcp_secret_name STRING,
            destination_bucket STRING,
            destination_path STRING,
            start_date STRING DEFAULT NULL,
            end_date STRING DEFAULT NULL
        )
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.8'
        PACKAGES = ('snowflake-snowpark-python', 'google-cloud-bigquery', 'google-cloud-storage')
        IMPORTS = ('@python_packages/python_packages.zip')
        HANDLER = 'export_to_gcs'
        AS
        $$
        from google.cloud import bigquery, storage
        import json
        from google.oauth2 import service_account
        import _snowflake
        import re


        def get_gcp_credentials(secret_name: str):
            """Get GCP credentials from Snowflake secret"""
            try:
                creds_str = _snowflake.get_generic_secret_string(secret_name)
                creds_json = json.loads(creds_str)
                credentials = service_account.Credentials.from_service_account_info(creds_json)
                return credentials
            except Exception as e:
                raise ValueError(f"Failed to get GCP credentials from Snowflake secret: {str(e)}")

        def validate_partition_dates(partition_type, start_date=None, end_date=None):
            """Validate date formats based on partition type"""
            try:
                if not start_date and not end_date:
                    return True

                formats = {
                    'HOUR': (r'^\d{10}$', 'YYYYMMDDHH'),  # 2024010112
                    'DAY': (r'^\d{8}$', 'YYYYMMDD'),      # 20240101
                    'MONTH': (r'^\d{6}$', 'YYYYMM'),      # 202401
                    'YEAR': (r'^\d{4}$', 'YYYY'),         # 2024
                }

                if partition_type not in formats:
                    return True  # Skip validation for other types (like integer range)

                pattern, format_example = formats[partition_type]

                if start_date:
                    if not re.match(pattern, start_date):
                        raise ValueError(f"Invalid start_date format. Expected {format_example} for {partition_type} partitioning, got {start_date}")

                if end_date:
                    if not re.match(pattern, end_date):
                        raise ValueError(f"Invalid end_date format. Expected {format_example} for {partition_type} partitioning, got {end_date}")

                # Additional validation: start_date should be <= end_date if both provided
                if start_date and end_date and start_date > end_date:
                    raise ValueError(f"start_date ({start_date}) cannot be greater than end_date ({end_date})")

                return True

            except Exception as e:
                raise ValueError(f"Date validation error: {str(e)}")

        def get_partition_info(client, project, dataset, table):
            """Get partition information using BigQuery client"""
            table_ref = f"{project}.{dataset}.{table}"
            table = client.get_table(table_ref)

            if table.time_partitioning is None:
                return None, None

            return (
                table.time_partitioning.field,  # partition column
                table.time_partitioning.type_   # partition type (e.g., DAY, MONTH, YEAR)
            )

        def get_gcs_timestamp(storage_client, bucket_name, partition_path):
            """Get timestamp for a specific partition path"""
            try:
                bucket = storage_client.bucket(bucket_name)
                blobs = list(bucket.list_blobs(prefix=partition_path, max_results=1))

                if not blobs:
                    return None

                pattern = r'/(\d{13})_'
                match = re.search(pattern, blobs[0].name)
                return int(match.group(1)) if match else None
            except Exception as e:
                print(f"Error getting GCS timestamp: {str(e)}")
                return None

        def clean_partition_path(storage_client, bucket_name, partition_path):
            """Delete all files in the partition path"""
            try:
                bucket = storage_client.bucket(bucket_name)
                blobs = bucket.list_blobs(prefix=partition_path)
                bucket.delete_blobs(list(blobs))
            except Exception as e:
                print(f"Error cleaning partition path: {str(e)}")

        def get_partitions(client, project, dataset, table, storage_client, destination_bucket, destination_path, start_date=None, end_date=None):
            """Get partition information and compare with GCS"""
            where_clause = f"table_name = '{table}' AND partition_id != '__NULL__'"

            if start_date:
                where_clause += f" AND partition_id >= '{start_date}'"
            if end_date:
                where_clause += f" AND partition_id <= '{end_date}'"

            query = f"""
            SELECT
                partition_id,
                UNIX_MILLIS(last_modified_time) as last_modified_time,
                total_rows,
                total_logical_bytes
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
            WHERE {where_clause}
            ORDER BY partition_id
            """

            query_job = client.query(query)
            bq_partitions = list(query_job.result())

            if not bq_partitions:
                return []

            # Check each partitions modification status
            partitions_to_update = []
            for partition in bq_partitions:
                partition_path = f"{destination_path}/partition_id={partition.partition_id}"
                gcs_timestamp = get_gcs_timestamp(storage_client, destination_bucket, partition_path)

                if gcs_timestamp is None or gcs_timestamp < partition.last_modified_time:
                    partitions_to_update.append(partition)

            return partitions_to_update

        def get_partition_filter(partition_id, partition_type, partition_column):
            """Generate the appropriate partition filter based on partition type"""
            if partition_type == 'DAY':
                return f"{partition_column} = PARSE_DATE('%Y%m%d', '{partition_id}')"
            elif partition_type == 'MONTH':
                # For monthly partitions (YYYYMM)
                return f"""
                    {partition_column} >= PARSE_DATE('%Y%m%d', '{partition_id}01')
                    AND {partition_column} < PARSE_DATE('%Y%m%d',
                        CAST(FORMAT_DATE('%Y%m', DATE_ADD(PARSE_DATE('%Y%m%d', '{partition_id}01'), INTERVAL 1 MONTH)) || '01' AS STRING)
                    )
                """
            elif partition_type == 'YEAR':
                # For yearly partitions (YYYY)
                return f"""
                    {partition_column} >= PARSE_DATE('%Y%m%d', '{partition_id}0101')
                    AND {partition_column} < PARSE_DATE('%Y%m%d', '{str(int(partition_id) + 1)}0101')
                """
            elif partition_type == 'HOUR':
                # For hourly partitions (YYYYMMDDHH)
                year = partition_id[:4]
                month = partition_id[4:6]
                day = partition_id[6:8]
                hour = partition_id[8:]
                return f"{partition_column} = TIMESTAMP('{year}-{month}-{day} {hour}:00:00')"
            else:
                # For integer range partitioning
                return f"{partition_column} = {partition_id}"

        def export_to_gcs(project, dataset, table, gcp_secret_name, destination_bucket, destination_path, start_date=None, end_date=None):
            try:
                # Get GCP credentials
                credentials = get_gcp_credentials(gcp_secret_name)

                # Initialize BigQuery and Storage clients with credentials
                bq_client = bigquery.Client(credentials=credentials, project=project)
                storage_client = storage.Client(credentials=credentials, project=project)
                export_results = []

                partition_column, partition_type = get_partition_info(
                    bq_client,
                    project,
                    dataset,
                    table
                )

                if not partition_column or not partition_type:
                    raise ValueError(f"Table {project}.{dataset}.{table} is not partitioned")

                # Validate dates based on partition type
                validate_partition_dates(partition_type, start_date, end_date)

                partitions = get_partitions(
                    bq_client,
                    project,
                    dataset,
                    table,
                    storage_client,
                    destination_bucket,
                    destination_path,
                    start_date,
                    end_date
                )

                if not partitions:
                    return {
                        'status': 'skipped',
                        'message': f'No partitions need to be updated. All partitions are up to date in GCS path: gs://{destination_bucket}/{destination_path}/',
                        'partition_type': partition_type,
                        'start_date': start_date,
                        'end_date': end_date
                    }

                for partition in partitions:
                    partition_id = partition.partition_id
                    bq_last_modified = partition.last_modified_time
                    total_rows = partition.total_rows
                    total_bytes = partition.total_logical_bytes

                    # Update path to include partition_id=
                    partition_path = f"{destination_path}/partition_id={partition_id}"
                    clean_partition_path(storage_client, destination_bucket, partition_path)

                    # Get appropriate partition filter
                    partition_filter = get_partition_filter(partition_id, partition_type, partition_column)

                    export_sql = f"""
                    EXPORT DATA
                    OPTIONS(
                        uri = 'gs://{destination_bucket}/{destination_path}/partition_id={partition_id}/{bq_last_modified}_*.parquet',
                        format = 'PARQUET',
                        compression = 'GZIP',
                        overwrite = true
                    ) AS
                    SELECT *
                    FROM `{project}.{dataset}.{table}`
                    WHERE {partition_filter}
                    """

                    query_job = bq_client.query(export_sql)
                    result = query_job.result()

                    export_results.append({
                        'partition_id': partition_id,
                        'partition_type': partition_type,
                        'last_modified': bq_last_modified,
                        'total_rows': total_rows,
                        'total_bytes': total_bytes,
                        'bytes_processed': query_job.total_bytes_processed
                    })

                return {
                    'status': 'success',
                    'destination': f'gs://{destination_bucket}/{destination_path}/',
                    'partition_type': partition_type,
                    'start_date': start_date,
                    'end_date': end_date,
                    'partitions_exported': len(export_results),
                    'export_results': export_results
                }

            except Exception as e:
                return {
                    'status': 'error',
                    'error_message': str(e),
                    'start_date': start_date,
                    'end_date': end_date
                }
        $$
     {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
