1- I implemented a factory and used faker to improve the way in which we are inserting data into the database.
2- The sync all limit is pretty straight forward, so we get all the rows in the source database and we inserted them in the target.
3- The sync all safely is an implementation of batching insertion, based on a batch size we implementad a kind of pagination to insert an amount of records in batches,
   this implementation wanted to follow the sendEvent method that was required, but in real if we want to do a "batch insertion" we should use the massive insert.
4- The sync new changes is using a "flag variable" that's storing basically the date of the last sync, so basically when we do a new sync all the records that has update_at > than that date
    are synced, I created an upsert method because we don't know if the records are new to insert or just updates.
5- The synchronize method is just using the union of syncAllSafety and syncNewChanges to do the pooling.
