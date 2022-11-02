# README

A thin library to help with bulk loading data into Google Datastore (Firestore in Datastore Mode). When you include this library in your `package.json`,
you also need to include `@google-cloud/datastore`. The library does not include it for you and treats it as a peer dependency.

## Usage

There are two classes provided by the library: `DatastoreWriter` and `BulkLoader`.

### `DatastoreWriter`

`DatastoreWriter` functions as a buffered writer. You `add` Datastore entities to it and it automatically batches and writes them to Datastore.
The constructor allows you to set various parameters such as:

- `buffer_size`: controls the buffer size. entities are stored in a buffer and flushed (written to Datastore) when the buffer gets full. Datastore does not allow writing more than 500 entities at a time. see [this](https://cloud.google.com/datastore/docs/concepts/limits). defaults to `500`.
- `max_concurrent_requests`: controls the maximum number of concurrent requests made to Google Datastore. It is recommended to keep this well below 100. defaults to `10`.
- `upsert`: controls whether to upsert or insert the entities. if set to true, entities will be upserted else they will be inserted. defaults to `false`.

`DatastoreWriter` provides following public methods, properties and events:

#### Methods

- `add`: add an entity to `DatastoreWriter`. Entities are batched and auto-flushed (written to Datastore) when the buffer gets full.
- `flush`: explicitly flush entities in the buffer.
- `ready`: returns a Promise that resolves when the number of concurrent requests in progress < `max_concurrent_requests`.

#### Properties

- `isBusy`: `true` if number of concurrent requests in progress = `max_concurrent_requests`.

#### Events

- `busy`: raised when number of concurrent requests in progress becomes equal to `max_concurrent_requests`.
- `idle`: raised when number of concurrent requests in progress drops below `max_concurrent_requests`.
- `stats`: raised after each request to Datastore. Contains following information:
  - `start`: when the request started (nanoseconds)
  - `end`: when the request ended (nanoseconds)
  - `elapsed`: `end - start` summed over all the requests ever made (nanoseconds)
  - `request_count`: number of requests that have been made until now
- `error`: exceptions and errors

### `BulkLoader`

A class that can be used to read data from a text file and load into Datastore. It is a good starting point to learn how to use `DatastoreWriter`.
The constructor expects a file to read, a Datastore instance and an object that can be used to convert a line from the text file to a
Datastore entity. Some optional parameters are supported:

- `num_lines`: number of lines to read from the given file. set to 0 or undefined to indicate no limit. defaults to `0`.
- `num_concurrent`: sets the maximum number of concurrent requests to Datastore. defaults to `10`.
- `batch_size`: sets the size of `DatastoreWriter`'s buffer. defaults to `500`.
- `upsert`: controls whether to upsert or insert the entities. if set to true, entities will be upserted else they will be inserted. defaults to `false`.

Following methods, properties and events are supported:

#### Methods

- `load`: starts loading the data from given file and returns a Promise that resolves when the load is completed.

#### Example

```
import { BulkLoader } from '@siddjain/datastore-writer';

let loader = new BulkLoader(csv_file, datastore, converter, options);
console.time('load');
loader.load().then(() => {
    console.log('Goodbye!');
    console.timeEnd('load');
}).catch(err => {
    console.error(err);
    console.timeEnd('load');
});
```

## License

[MIT](https://github.com/siddjain/datastore-writer/blob/master/LICENSE)
