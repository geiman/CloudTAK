import test from 'node:test';
import assert from 'node:assert';
import Worker from '../src/worker.js';
import fs from 'node:fs';
import Sinon from 'sinon';
import {
    S3Client,
    GetObjectCommand,
    PutObjectCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
} from '@aws-sdk/client-s3';
import { MockAgent, setGlobalDispatcher, getGlobalDispatcher } from 'undici';

// A KMZ with no Placemarks, only an embedded GroundOverlay image: the overlay's footprint is
// the one vector feature, and the image itself becomes a PMTiles child asset
test('Worker Import: GroundOverlay-only KMZ keeps its overlay', async (t) => {
    const mockAgent = new MockAgent();
    const originalDispatcher = getGlobalDispatcher();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);

    t.after(() => {
        Sinon.restore();
        setGlobalDispatcher(originalDispatcher);
        mockAgent.close();
    });

    const mockPool = mockAgent.get('http://localhost:5001');

    const created: Array<{ id: string; name: string; parent?: string }> = [];
    mockPool.intercept({
        path: /profile\/asset$/,
        method: 'POST',
    }).reply((req) => {
        const body = JSON.parse(String(req.body));
        created.push(body);
        return { statusCode: 200, data: JSON.stringify({ id: body.id, artifacts: body.artifacts ?? [] }) };
    }).persist();

    mockPool.intercept({
        path: /profile\/asset\//,
        method: 'PATCH',
    }).reply((req) => {
        const body = JSON.parse(String(req.body));
        return { statusCode: 200, data: JSON.stringify({ id: created[0].id, artifacts: body.artifacts ?? [] }) };
    }).persist();

    mockPool.intercept({
        path: /\/api\/import\/.*\/result/,
        method: 'POST',
    }).reply(200, JSON.stringify({})).persist();

    // the overlay image is also packaged as an icon
    mockPool.intercept({
        path: /\/api\/iconset/,
        method: 'POST',
    }).reply(200, JSON.stringify({})).persist();

    const uploaded: string[] = [];
    Sinon.stub(S3Client.prototype, 'send').callsFake(async (command) => {
        if (command instanceof GetObjectCommand) {
            return { Body: fs.createReadStream(new URL('./fixtures/kmz-overlay/mason_lake.kmz', import.meta.url)) };
        } else if (command instanceof CreateMultipartUploadCommand) {
            uploaded.push(String(command.input.Key));
            return { UploadId: '123' };
        } else if (command instanceof PutObjectCommand) {
            uploaded.push(String(command.input.Key));
            return { ETag: '"123"' };
        } else if (command instanceof UploadPartCommand) {
            return { ETag: '"123"' };
        } else if (command instanceof CompleteMultipartUploadCommand) {
            return { Location: '...' };
        }
        throw new Error(`Unexpected command: ${command.constructor.name}`);
    });

    const worker = new Worker({
        api: 'http://localhost:5001',
        secret: 'coe-wildland-fire',
        bucket: 'test-bucket',
        job: {
            id: 'c7d2f1c4-0d7e-4a51-9a49-1f1f0e5c2a11',
            created: '2025-08-25T18:08:21.563Z',
            updated: '2025-08-25T18:08:21.563Z',
            status: 'Running',
            error: null,
            result: {},
            name: 'mason_lake.kmz',
            username: 'admin@example.com',
            source: 'Upload',
            config: {},
            source_id: null,
        },
    });

    let failure: Error | undefined;
    worker.on('error', (err) => {
        failure = err;
    });

    await worker.process();

    assert.ifError(failure);

    const parent = created.find(a => !a.parent);
    assert.ok(parent, 'the uploaded KMZ should be created as an asset');

    const overlays = created.filter(a => a.parent === parent.id);
    assert.equal(overlays.length, 1, 'the GroundOverlay should become one child asset');
    assert.ok(overlays[0].name.endsWith('.pmtiles'), 'the child asset should be PMTiles');
    assert.ok(uploaded.some(key => key.endsWith(`${overlays[0].id}.pmtiles`)), 'the child PMTiles should be uploaded');
});
