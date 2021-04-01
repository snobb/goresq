// TS script enqueueing jobs for the example worker (see ./examples)
import { ConnectionOptions, Queue } from 'node-resque';

type Args = {
    class: string,
    [item: string]: unknown
}

async function enqueue_job (params: ConnectionOptions, payload: Args[]) {
    const queue = new Queue({ connection: params }, {});
    queue.on('error', (err: Error) => {
        // eslint-disable-next-line no-console
        console.error(err);
    });

    await queue.connect();
    await queue.enqueue('queue2.test', payload[0].class, payload);
    await queue.end();
}

(async () => {
    const params = {
        pkg: 'ioredis',
        host: '127.0.0.1',
        port: 6379,
        database: 4,
        namespace: 'resque'
    };

    const args = [{
        class: 'sum',
        task_data: [10, 30, 15]
    }];

    await enqueue_job(params, args);
})();
