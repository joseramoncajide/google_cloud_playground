var projectId = "chrome-ranger-180013";
var datasetId = "google_cloud_playground";
var destination_table = "foo";

var tableId = "foo"

function importFileFromGCS (datasetId, tableId, bucketName, filename, projectId) {
  // [START bigquery_import_from_gcs]
  // Imports the Google Cloud client libraries
  const BigQuery = require('@google-cloud/bigquery');
  const Storage = require('@google-cloud/storage');

  // The project ID to use, e.g. "your-project-id"
  // const projectId = "your-project-id";

  // The ID of the dataset of the table into which data should be imported, e.g. "my_dataset"
  // const datasetId = "my_dataset";

  // The ID of the table into which data should be imported, e.g. "my_table"
  // const tableId = "my_table";

  // The name of the Google Cloud Storage bucket where the file is located, e.g. "my-bucket"
  // const bucketName = "my-bucket";

  // The name of the file from which data should be imported, e.g. "file.csv"
  // const filename = "file.csv";

  // Instantiates clients
  const bigquery = BigQuery({
    projectId: projectId
  });

  const storage = Storage({
    projectId: projectId
  });

  var bq_metadata = {
    encoding: 'ISO-8859-1',
    sourceFormat: 'CSV',
    autodetect: true,
    createDisposition: 'CREATE_IF_NEEDED',
    writeDisposition: 'WRITE_APPEND'
  };

  let job;

  // Imports data from a Google Cloud Storage file into the table
  bigquery
    .dataset(datasetId)
    .table(tableId)
    .import(storage.bucket(bucketName).file(filename), bq_metadata)
    .then((results) => {
      job = results[0];
      console.log(`Job ${job.id} started.`);

      // Wait for the job to finish
      return job.promise();
    })
    .then((results) => {
      // Get the job's status
      return job.getMetadata();
    }).then((metadata) => {
      // Check the job's status for errors
      const errors = metadata[0].status.errors;
      if (errors && errors.length > 0) {
        throw errors;
      }
    }).then(() => {
      console.log(`Job ${job.id} completed.`);
    })
    .catch((err) => {
      console.error('ERROR:', err);
    });
  // [END bigquery_import_from_gcs]
}

function importFile(datasetId, file_csv, destination_table){

  console.log(`Importing ${file_csv} to table ${destination_table} `);

  var bigquery = require('@google-cloud/bigquery')({
    projectId: projectId
  });

  const dataset = bigquery.dataset(datasetId);

  var bq_metadata = {
    encoding: 'ISO-8859-1',
    sourceFormat: 'CSV',
    autodetect: true,
    createDisposition: 'CREATE_IF_NEEDED',
    writeDisposition: 'WRITE_APPEND'
  };

  //var dataset = bigquery.dataset('viesgo_staging_itunes');
  var table = dataset.table(destination_table);

  var job;

    // Imports data from a local file into the table
    bigquery
      .dataset(datasetId)
      .table(destination_table)
      .import(file_csv, bq_metadata)
      .then((results) => {
        job = results[0];
        console.log(`Job ${job.id} started.`);
        return job.promise();
      })
      .then((results) => {
        console.log(`Job ${job.id} completed.`);
      })
      .catch((err) => {
        console.error('ERROR:', err);
      });
}


function printResult (rows) {
  console.log('Query Results:');
  rows.forEach(function (row) {
    let str = '';
    for (let key in row) {
      if (str) {
        str = `${str}\n`;
      }
      str = `${str}${key}: ${row[key]}`;
    }
    console.log(str);
  });
}

function queryShakespeare (projectId) {
  // Imports the Google Cloud client library
  const BigQuery = require('@google-cloud/bigquery');

  // The project ID to use, e.g. "your-project-id"
  // const projectId = "your-project-id";

  // The SQL query to run
  const sqlQuery = `SELECT
    corpus, COUNT(*) as unique_words
    FROM publicdata.samples.shakespeare
    GROUP BY
      corpus
    ORDER BY
    unique_words DESC LIMIT 10;`;

  // Instantiates a client
  const bigquery = BigQuery({
    projectId: projectId
  });

  // Query options list: https://cloud.google.com/bigquery/docs/reference/v2/jobs/query
  const options = {
    query: sqlQuery,
    useLegacySql: false // Use standard SQL syntax for queries.
  };

  // Runs the query
  bigquery
    .query(options)
    .then((results) => {
      const rows = results[0];
      printResult(rows);
    })
    .catch((err) => {
      console.error('ERROR:', err);
    });
}
/**
 * Background Cloud Function to be triggered by Cloud Storage.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */
exports.helloGCS = function (event, callback) {
  const file = event.data;

  if (file.resourceState === 'not_exists') {
    console.log(`File ${file.name} deleted.`);
  } else if (file.metageneration === '1') {
    // metageneration attribute is updated on metadata changes.
    // on create value is 1
    console.log(`File ${file.name} uploaded.`);

    console.log(`Bucket name: ${file.bucket}.`);

    var file_csv = "gs://" + file.bucket + "/" + file.name;

    var bucketName = file.bucket;
    var filename = file.name;

    // importFile(datasetId, file_csv, destination_table);

    //queryShakespeare(projectId);

    importFileFromGCS(datasetId, tableId, bucketName, filename, projectId);

  } else {
    console.log(`File ${file.name} metadata updated.`);
  }

  callback();
};
