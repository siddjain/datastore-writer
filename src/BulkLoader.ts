import { Datastore, Entity } from '@google-cloud/datastore';
import LineByLineReader from 'line-by-line'
import { strict as assert } from 'node:assert';
import { DatastoreWriter as Writer } from './DatastoreWriter'
import logger from './logger'
import _ from 'lodash'

const checkNull = (arg, name) => {
    if (!arg) {
        throw new Error(`${name} is null or undefined`);
    }
}

const checkTrue = (arg, msg) => {
    if (!arg) {
        throw new Error(msg);
    }
}

/**
 * Used to create a Datastore entity from a string representation
 */
export interface IEntityConverter {
    /**
     * Convert string to Datastore entity
     * @param line application dependent string representation of Datastore entity
     */
    convert(line: string): Entity
}

export interface IBulkLoaderOptions {
    /**
     * controls the maximum number of concurrent requests made to Google Datastore. It is recommended to keep this well below 100.
     */
    num_concurrent?: number;
    /**
     * number of lines to read from the text file (optional)
     */
    num_lines?: number;
    /**
     * controls the buffer size. entities are stored in a buffer and flushed (written to Datastore) when the buffer gets full.
     */
    batch_size?: number;
    /**
     * controls whether to upsert or insert the entities. if set to true, entities will be upserted else they will be inserted.
     */
    upsert?: boolean;
}

/**
 * Bulk load data from text file into Google Datastore (Firestore in Datastore Mode).
 */
export class BulkLoader {

    #writer: Writer;
    #path: string;
    #batch: number;
    #count: number;
    #numConcurrent: number;
    #nLines: number;
    #converter;
    #resolve;
    #reject;
    #lr: LineByLineReader;
    #isPaused: boolean;
    #requestCount: number;

    #default_options(): IBulkLoaderOptions {
        return {
            num_concurrent: 10,
            batch_size: 500,
            upsert: false
        }
    }

    constructor(filePath: string, datastore: Datastore, converter: IEntityConverter, options?: IBulkLoaderOptions) {
        if (!filePath) {
            throw new Error("filePath is null or empty");
        }
        if (!converter) {
            throw new Error("converter is null or undefined");
        }
        if (!datastore) {
            throw new Error("datastore is null or undefined");
        }
        if (_.isEmpty(options)) {
            options = this.#default_options();
        }
        if (!options.batch_size) { options.batch_size = 500; }
        if (!options.num_concurrent) { options.num_concurrent = 10; }
        if (options.upsert === null || options.upsert === undefined) { options.upsert = false; }
        checkTrue(options.batch_size > 0, "batch size must be greater than 0");
        checkTrue(options.num_concurrent > 0, "nConcurrent must be greater than 0");
        this.#path = filePath;
        this.#batch = options.batch_size;
        this.#numConcurrent = options.num_concurrent;
        this.#nLines = options.num_lines;
        this.#converter = converter;
        this.#requestCount = 0;
        this.#writer = new Writer(datastore, { buffer_size: options.batch_size, upsert: options.upsert, max_concurrent_requests: options.num_concurrent });
        this.#writer.on('error', err => this.#handleErrorEvent(err));
        this.#addStatsEvent();
    }

    #addStatsEvent() {
        this.#writer.on('stats', stats => {
            this.#requestCount++;
            let n = this.#requestCount * this.#batch;
            logger.info(`processed ${n} rows`);
        });
    }

    #waitForWriter(entity) {
        this.#writer.ready().then(() => {
            try {
                logger.debug('resuming...');
                assert.ok(this.#isPaused);
                this.#writer.add(entity);
                this.#lr.resume();
                this.#isPaused = false;
            } catch (err) {
                this.#handleErrorEvent(err);
            }
        }).catch(err => this.#handleErrorEvent(err));
    }

    #complete() {
        const sec = Number(this.#writer.elapsed) * 1e-9 / this.#writer.requestCount;
        logger.info(`avg. request time = ${sec} s`);
        if (this.#resolve) {
            this.#resolve();
        }
    }

    #handleLineEvent(line) {
        try {
            if (this.#nLines > 0 && this.#count === this.#nLines) {
                // Stops emitting 'line' events, closes the file and emits the 'end' event.
                this.#lr.close();
            } else {
                this.#count++;
                let entity = this.#converter.convert(line);
                if (this.#writer.isBusy) {
                    logger.debug('waiting for requests to complete...');
                    this.#lr.pause();
                    this.#isPaused = true;
                    this.#waitForWriter(entity);
                } else {
                    this.#writer.add(entity);
                }
            }
        } catch (err) {
            this.#handleErrorEvent(err);
        }
    }

    #handleEndEvent() {
        this.#writer.flush().then(() => this.#complete()).catch(err => this.#handleErrorEvent(err));
    }

    #handleErrorEvent(err) {
        if (err) {
            if (err.stack) {
                logger.error(err.stack);
            } else {
                logger.error(err);
            }
            if (this.#reject) {
                this.#reject(err);
            }
        }
    }

    load(): Promise<void> {
        // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/this
        //  arrow functions don't provide their own this binding (it retains the this value of the enclosing lexical context).
        return new Promise((resolve, reject) => {
            this.#resolve = resolve;
            this.#reject = reject;
            this.#lr = new LineByLineReader(this.#path);
            this.#count = 0;
            this.#lr.on('error', (err) => {
                this.#handleErrorEvent(err);
            });

            this.#lr.on('line', (line) => {
                this.#handleLineEvent(line)
            });

            this.#lr.on('end', () => {
                this.#handleEndEvent();
            });
        });
    }
}
