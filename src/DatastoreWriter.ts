/**
  MIT License
  
  Copyright (c) Siddharth Jain.
  
  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:
  
  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.
  
  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
 */

import { Datastore } from '@google-cloud/datastore'
import logger from './logger'
import { EventEmitter } from 'node:events';
import { strict as assert } from 'node:assert';
import { hrtime } from 'node:process';
import _ from 'lodash';

interface IDatastoreWriterOptions {
    /**
     * controls the buffer size. entities are stored in a buffer and flushed (written to Datastore) when the buffer gets full.
     */
    buffer_size: number;
    /**
     * controls the maximum number of concurrent requests made to Google Datastore. It is recommended to keep this well below 100.
     */
    max_concurrent_requests: number;
    /**
     * controls whether to upsert or insert the entities. if set to true, entities will be upserted else they will be inserted.
     */
    upsert: boolean;
}

/**
 * Class to make it easy to bulk load data into Google Datastore (Firestore in Datastore Mode).
 */
class DatastoreWriter extends EventEmitter {

    #entities = [];
    #datastore: Datastore;
    isBusy: boolean;
    #bufferSize: number;
    #bufferCount: number;
    #t0: bigint;
    #t1: bigint;
    #isTicking: boolean;
    #maxConcurrentRequests: number;
    #concurrentRequests: number;
    elapsed: bigint;
    requestCount: number;
    #upsert: boolean;

    /**
     * Create instance of @class DatastoreWriter
     * @param datastore an instance of Google Cloud Datastore
     * @param options options
     */
    constructor(datastore, options?: IDatastoreWriterOptions) {
        super();
        if (_.isEmpty(options)) {
            options = {
                buffer_size: 500,
                max_concurrent_requests: 10,
                upsert: false
            }
        }
        if (!options.buffer_size) { options.buffer_size = 500; }
        if (!options.max_concurrent_requests) { options.max_concurrent_requests = 10; }
        if (!datastore) {
            throw new Error("datastore is null or empty");
        }
        if (!options.buffer_size) {
            throw new Error("bufferSize is null or empty");
        }
        if (!options.max_concurrent_requests) {
            throw new Error("maxConcurrentRequests is null or empty")
        }
        this.#entities = [];
        this.#datastore = datastore;
        this.isBusy = false;
        this.#bufferCount = 0;
        this.#concurrentRequests = 0;
        this.elapsed = 0n;
        this.requestCount = 0;
        this.#bufferSize = options.buffer_size;
        this.#maxConcurrentRequests = options.max_concurrent_requests;
        this.#upsert = options.upsert;
        if (this.#bufferSize <= 0) {
            throw new Error(`bufferSize ${this.#bufferSize} cannot be less than or equal to 0`);
        }
        if (this.#maxConcurrentRequests <= 0) {
            throw new Error(`maxConcurrentRequests ${this.#maxConcurrentRequests} should be at least 1`);
        }
    }

    #changeStateToBusy() {
        if (!this.isBusy && this.#concurrentRequests === this.#maxConcurrentRequests) {
            this.isBusy = true;
            this.emit('busy');
        }
    }

    #changeStateToIdle() {
        if (this.isBusy && this.#concurrentRequests < this.#maxConcurrentRequests) {
            this.isBusy = false;
            this.emit('idle');
        }
    }

    /**
     * Always await this method before calling add or flush
     * @returns void
     */
    ready(): Promise<void> {
        return new Promise((resolve, reject) => {
            if (this.isBusy) {
                this.once('idle', () => resolve());
            } else {
                resolve();
            }
        })
    }

    /**
     * Add an entity to the buffer. The entity is not written to Datastore yet.
     * @param entity 
     */
    add(entity) {
        if (!entity) {
            throw new Error("entity is null or empty");
        }
        this.#entities.push(entity);
        this.#bufferCount++;
        if (this.#bufferCount % this.#bufferSize === 0) {
            this.flush().catch(err => {
                this.emit('error', err);
            })
        }
    }

    #startTimer() {
        assert.ok(!this.#isTicking);
        this.#isTicking = true;
        this.#t0 = hrtime.bigint();
    }

    #endTimer() {
        assert.ok(this.#isTicking);
        this.#isTicking = false;
        this.#t1 = hrtime.bigint();
        const nano = this.#t1 - this.#t0;
        this.elapsed += nano; // nanoseconds
        logger.debug(`Elapsed ${nano} ns`);
        this.emit('stats', {
            start: this.#t0,
            end: this.#t1,
            request_count: this.requestCount,
            elapsed: nano       // total time elapsed, not t1-t0
        });
    }

    /**
     * Writes all the entities in the buffer to the Datastore.
     */
    async flush() {
        let foo = this.#entities;
        this.#entities = [];
        if (foo.length > 0) {
            this.#concurrentRequests++;
            if (this.#concurrentRequests > this.#maxConcurrentRequests) {
                throw new Error(`exceeded maximum number of concurrent requests.`);
            }
            this.requestCount++;  // tracks the number of Datastore requests
            let requestNumber = this.requestCount;
            this.#changeStateToBusy();
            logger.info(`request ${requestNumber} (${this.#concurrentRequests} of ${this.#maxConcurrentRequests} concurrent): inserting ${foo.length} entities`);
            this.#startTimer();
            let result = null;
            if (this.#upsert) {
                result = await this.#datastore.upsert(foo); // insert or update if exists
            } else {
                result = await this.#datastore.insert(foo); // insert. throw error if exists
            }
            this.#concurrentRequests--;
            this.#endTimer();
            logger.debug(`Completed request ${requestNumber}: Mutation Results: ${result[0].mutationResults.length} Index Updates: ${result[0].indexUpdates}`);
            this.#changeStateToIdle();
        }
    }
}

export default DatastoreWriter;