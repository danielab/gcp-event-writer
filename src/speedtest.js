const BigQuery = require('@google-cloud/bigquery');

const projectId = 'cloud-workshop-18';
const datasetId = 'speedtest';
const tableId = 'results';

async function writeEventToBQ(receivedEvent) {
    const event = {...receivedEvent, timestamp: new Date(receivedEvent.timestamp)};

    const bigquery = new BigQuery({
        projectId: projectId,
    });

    console.log('About to insert: ', event);

    await bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert([event])
        .then(() => {
            console.log(`Inserted the event`);
        })
        .catch(err => {
            if (err && err.name === 'PartialFailureError') {
                if (err.errors && err.errors.length > 0) {
                    console.log('Insert errors:');
                    err.errors.forEach(err => console.error(err));
                }
            } else {
                console.error('ERROR:', err);
            }
        });
};

async function createTable(bigquery, tableId) {
    const schema = require('../resources/bigquery-schema.json');

    const options = {
        schema: schema,
    };

    await bigquery
        .dataset(datasetId)
        .createTable(tableId, options)
        .then(results => {
            const table = results[0];
            console.log(`Table "${table.id}" created.`);
        })
        .catch(err => {
            console.error('ERROR:', err);
        });
}

async function createTableIfNotExists() {
    const bigquery = new BigQuery({
        projectId: projectId,
    });

    await bigquery.dataset(datasetId)
        .table(tableId)
        .getRows({maxResults: 1})
        .catch(() => createTable(bigquery, tableId));
}

exports.handleSpeedtestEvent = async (speedtestEvent) => {
    await createTableIfNotExists();
    await writeEventToBQ(speedtestEvent);
};