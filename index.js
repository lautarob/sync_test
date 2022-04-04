/**
 * This exercise has you implement a synchronize() method that will
 * copy all records from the sourceDb into the targetDb() then start
 * polling for changes. Places where you need to add code have been
 * mostly marked and the goal is to get the runTest() to complete
 * successfully.
 *
 *
 * Requirements:
 *
 * Try to solve the following pieces for a production system. Note,
 * doing one well is better than doing all poorly. If you are unable to
 * complete certain requirements, please comment your approach to the
 * solution. Comments on the "why" you chose to implement the code the
 * way you did is highly preferred.
 *
 * 1. syncAllNoLimit(): Make sure what is in the source database is
 *    in our target database for all data in one sync. Think of a naive
 *    solution.
 * 2. syncAllSafely(): Make sure what is in the source database is in
 *    our target database for all data in batches during multiple
 *    syncs. Think of a pagination solution.
 * 3. syncNewChanges(): Make sure updates in the source database is in
 *    our target database without syncing all data for all time. Think
 *    of a delta changes solution.
 * 4. synchronize(): Create polling for the sync. A cadence where we
 *    check the source database for updates to sync.
 *
 * Feel free to use any libraries that you are comfortable with and
 * change the parameters and returns to solve for the requirements.
 *
 *
 * You will need to reference the following API documentation:
 *
 * Required: https://www.npmjs.com/package/nedb
 * Required: https://github.com/bajankristof/nedb-promises
 * Recommended: https://lodash.com/docs/4.17.15
 * Recommended: https://www.npmjs.com/package/await-the#api-reference
 */

const Datastore = require('nedb-promises');
const _ = require('lodash');
const the = require('await-the');
const faker = require('faker')


const RecordFactory = {
  create: function (data = {}) {
    return {
      name: data.name || faker.database.engine(),
      owner: data.owner || faker.name.firstName(),
      amount: data.amount || faker.finance.amount()
    }
  }
}

// The source database to sync updates from.
const sourceDb = new Datastore({
  inMemoryOnly: true,
  timestampData: true
});

// The target database that sendEvents() will write too.
const targetDb = new Datastore({
  inMemoryOnly: true,
  timestampData: true
});

let TOTAL_RECORDS;
let EVENTS_SENT = 0;
let LAST_SYNC_DATE = null;

const load = async () => {
  await sourceDb.insert(RecordFactory.create({ name: 'GE' }));
  await the.wait(300);
  await sourceDb.insert(RecordFactory.create());
  await the.wait(300);
  const lastRecord = await sourceDb.insert(RecordFactory.create());

  TOTAL_RECORDS = 3;
  LAST_SYNC_DATE = lastRecord.updatedAt
}


/**
 * API to send each document to in order to sync.
 */
const sendEvent = async data => {
  EVENTS_SENT += 1;
  console.log('event being sent: ');
  console.log(data);
  await targetDb.insert(data);
};


// Find and update an existing document.
const touch = async name => {
  const record = await sourceDb.update({ name }, { $set: { owner: 'test4' } }, { returnUpdatedDocs: true });
  if (record) {
    LAST_UPDATED_AT_DATE = record.updatedAt
  }
};


/**
 * Utility to log one record to the console for debugging.
 */
const read = async name => {
  const record = await sourceDb.findOne({ name });
  console.log(record);
};


/**
 * Utility to insert or update record depending if it exists in the targetdb.
 */
const upsert = async data => {
  EVENTS_SENT += 1;
  const exists = await targetDb.findOne({ _id: data._id })

  if (exists) {
    await targetDb.update({ _id: data._id }, data);
  } else {
    await targetDb.insert({ data });
  }
}


/**
 * Get all records out of the database and send them using
 * 'sendEvent()'.
 */
const syncAllNoLimit = async () => {
  const records = await sourceDb.find();
  for (const record of records) {
    await sendEvent(record);
  }
}

/**
 * Synchronize all records in batches.
 */
const syncAllSafely = async (batchSize) => {
  const recordsCount = await sourceDb.count();
  const limit = batchSize;
  let offset = 0;

  while (offset < recordsCount) {
    const records = await sourceDb.find().limit(limit).skip(offset)
    for (const record of records) {
      await sendEvent(record);
    }
    offset += batchSize
  }
}


/**
 * Sync changes since the last time the function was called with
 * with the passed in data.
 */
const syncNewChanges = async () => {
  const records = await sourceDb.find({ "updatedAt": { $gt: LAST_SYNC_DATE } })
  for (const record of records) {
    await upsert(record)
  }
  LAST_SYNC_DATE = new Date();
}


/**
 * Implement function to fully sync of the database and then
 * keep polling for changes.
 */
const synchronize = async () => {
  let syncCounter = 1
  await syncAllSafely(10)
  while (true) {
    console.log('Sync number: ', syncCounter)
    await syncNewChanges();
    syncCounter += 1;
  }
}


/**
 * Simple test construct to use while building up the functions
 * that will be needed for synchronize().
 */
// const runTest = async () => {
//   await load();

//   // Check what the saved data looks like.
//   await read('GE');

//   // EVENTS_SENT = 0;
//   // await syncAllNoLimit();

//   // // TODO: Maybe use something other than logs to validate use cases?
//   // // Something like `mocha` with `assert` or `chai` might be good libraries here.
//   // if (EVENTS_SENT === TOTAL_RECORDS) {
//   //   console.log('1. synchronized correct number of events')
//   // }

//   // EVENTS_SENT = 0;
//   // const data = await syncAllSafely(10);

//   // if (EVENTS_SENT === TOTAL_RECORDS) {
//   //   console.log('2. synchronized correct number of events')
//   // }

//   // Make some updates and then sync just the changed files.
//   EVENTS_SENT = 0;
//   await the.wait(300);
//   await touch('GE');
//   await syncNewChanges();

//   if (EVENTS_SENT === 1) {
//     console.log('3. synchronized correct number of events')
//   }
// }

// TODO:
// Call synchronize() instead of runTest() when you have synchronize working
// or add it to runTest().
runTest();
// synchronize();




// NOTES:

// 1- I implemented a factory and used faker to improve the way in which we are inserting data into the database.
// 2- The sync all limit is pretty straight forward, so we get all the rows in the source database and we inserted them in the target.
// 3- The sync all safely is an implementation of batching insertion, based on a batch size we implementad a kind of pagination to insert an amount of records in batches,
//    this implementation wanted to follow the sendEvent method that was required, but in real if we want to do a "batch insertion" we should use the massive insert.
// 4- The sync new changes is using a "flag variable" that's storing basically the date of the last sync, so basically when we do a new sync all the records that has update_at > than that date
//    are synced, I created an upsert method because we don't know if the records are new to insert or just updates.
// 5 - The synchronize method is just using the union of syncAllSafety and syncNewChanges to do the pooling.

