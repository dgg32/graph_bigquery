import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery
import argparse
from apache_beam.runners.runner import PipelineState
from google.cloud import storage


parser = argparse.ArgumentParser()

args, beam_args = parser.parse_known_args()

beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    project="715484494373",
    job_name='import neo4j to bigquery',
    temp_location='gs://neo4j-bigquery-project',
    region='asia-northeast1')


p = beam.Pipeline(options=beam_options)

os.environ["GOOGLE_CLOUD_PROJECT"] = PARAM["gcp_project_id"]
client = bigquery.Client()

dataset_id = f"{PARAM['gcp_project_name']}.{PARAM['bigquery_dataset']}"

print (client.project)


try:
	client.get_dataset(dataset_id)
	
except:
	dataset = bigquery.Dataset(dataset_id)  #

	dataset.location = "asia-northeast1"
	dataset.description = "dataset for clinical trial data"

	dataset_ref = client.create_dataset(dataset, timeout=30)  # Make an API request.
	

bucket_client = storage.Client()
bucket = bucket_client.bucket(bucket_name)
for blob in bucket.list_blobs(prefix='tsv'):
  print (blob.name)



for f in bucket.list_blobs(prefix='tsv'):
    
    full_path = f"gs://{bucket_name}/{f.name}"

    filename = f.name.split("/")[1]
    print (full_path)
    import_data = (
        p
        | f'Read {full_path} files' >> beam.io.ReadFromText(full_path)
        | f'Write to {filename} output table' >> beam.io.WriteToBigQuery(
                ignore_unknown_columns=True,
                project=PARAM['gcp_project_name'],
                dataset=PARAM['bigquery_dataset'],
                table=filename.replace(".tsv", ""),
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

    ret = p.run()
    if ret.state == PipelineState.DONE:
        print('Success!!!')
    else:
        print('Error Running beam pipeline')