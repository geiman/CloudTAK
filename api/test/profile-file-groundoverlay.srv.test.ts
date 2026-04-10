import test from 'node:test';
import assert from 'node:assert';
import { Readable } from 'node:stream';
import Sinon from 'sinon';
import Flight from './flight.js';
import S3 from '../lib/aws/s3.js';

const flight = new Flight();

flight.init({ takserver: true });
flight.takeoff();
flight.user({ username: 'admin' });

const assetId = '7f2e1a3b-9c2a-4a67-86a8-7de7337c3f11';
const manifest = {
    overlays: [{
        name: '26PGA_85',
        mime: 'image/png',
        ext: '.groundoverlay-0.png',
        opacity: 0.75,
        coordinates: [
            [-75.0, 40.0],
            [-74.0, 40.0],
            [-74.0, 39.0],
            [-75.0, 39.0]
        ]
    }]
};
const pngData = Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
    'base64'
);

test('PROFILE: seed GroundOverlay asset', async () => {
    try {
        assert.ok(flight.config, 'flight config available');

        const file = await flight.config.models.ProfileFile.generate({
            id: assetId,
            username: 'admin@example.com',
            name: '26PGA_85.kmz',
            path: '/',
            iconset: null,
            size: 12345,
            artifacts: [
                { ext: '.groundoverlays.json', size: 321 },
                { ext: '.groundoverlay-0.png', size: pngData.length }
            ]
        });

        assert.equal(file.id, assetId);
        assert.deepEqual(file.artifacts, [
            { ext: '.groundoverlays.json', size: 321 },
            { ext: '.groundoverlay-0.png', size: pngData.length }
        ]);
    } catch (err) {
        assert.ifError(err);
    }
});

test('GET: api/profile/asset/:asset/groundoverlays', async () => {
    let existsStub: Sinon.SinonStub | undefined;
    let getStub: Sinon.SinonStub | undefined;

    try {
        existsStub = Sinon.stub(S3, 'exists').callsFake(async (key: string) => {
            return key === `profile/admin@example.com/${assetId}.groundoverlays.json`;
        });

        getStub = Sinon.stub(S3, 'get').callsFake(async (key: string) => {
            assert.equal(key, `profile/admin@example.com/${assetId}.groundoverlays.json`);
            return Readable.from([Buffer.from(JSON.stringify(manifest))]);
        });

        const res = await flight.fetch(`/api/profile/asset/${assetId}/groundoverlays`, {
            method: 'GET',
            auth: {
                bearer: flight.token.admin
            }
        }, false);

        assert.equal(res.status, 200);
        assert.deepEqual(res.body, manifest);
        assert.equal(existsStub.callCount, 1);
        assert.equal(getStub.callCount, 1);
    } catch (err) {
        assert.ifError(err);
    } finally {
        existsStub?.restore();
        getStub?.restore();
        Sinon.restore();
    }
});

test('GET: api/profile/asset/:asset/groundoverlay/:index', async () => {
    let existsStub: Sinon.SinonStub | undefined;
    let getStub: Sinon.SinonStub | undefined;

    try {
        existsStub = Sinon.stub(S3, 'exists').callsFake(async (key: string) => {
            return [
                `profile/admin@example.com/${assetId}.groundoverlays.json`,
                `profile/admin@example.com/${assetId}.groundoverlay-0.png`
            ].includes(key);
        });

        getStub = Sinon.stub(S3, 'get').callsFake(async (key: string) => {
            if (key === `profile/admin@example.com/${assetId}.groundoverlays.json`) {
                return Readable.from([Buffer.from(JSON.stringify(manifest))]);
            } else if (key === `profile/admin@example.com/${assetId}.groundoverlay-0.png`) {
                return Readable.from([pngData]);
            }

            throw new Error(`Unexpected key: ${key}`);
        });

        const res = await flight.fetch(`/api/profile/asset/${assetId}/groundoverlay/0`, {
            method: 'GET',
            auth: {
                bearer: flight.token.admin
            }
        }, { verify: false, json: false, binary: true });

        assert.equal(res.status, 200);
        assert.equal(res.headers.get('content-type'), 'image/png');
        assert.deepEqual(Buffer.from(res.body), pngData);
        assert.equal(existsStub.callCount, 2);
        assert.equal(getStub.callCount, 2);
    } catch (err) {
        assert.ifError(err);
    } finally {
        existsStub?.restore();
        getStub?.restore();
        Sinon.restore();
    }
});

test('GET: api/profile/asset/:asset/groundoverlay/:index missing index', async () => {
    let existsStub: Sinon.SinonStub | undefined;
    let getStub: Sinon.SinonStub | undefined;

    try {
        existsStub = Sinon.stub(S3, 'exists').callsFake(async (key: string) => {
            return key === `profile/admin@example.com/${assetId}.groundoverlays.json`;
        });

        getStub = Sinon.stub(S3, 'get').callsFake(async (key: string) => {
            assert.equal(key, `profile/admin@example.com/${assetId}.groundoverlays.json`);
            return Readable.from([Buffer.from(JSON.stringify(manifest))]);
        });

        const res = await flight.fetch(`/api/profile/asset/${assetId}/groundoverlay/1`, {
            method: 'GET',
            auth: {
                bearer: flight.token.admin
            }
        }, false);

        assert.equal(res.status, 404);
        assert.deepEqual(res.body, {
            status: 404,
            message: 'Ground overlay image does not exist',
            messages: []
        });
        assert.equal(existsStub.callCount, 1);
        assert.equal(getStub.callCount, 1);
    } catch (err) {
        assert.ifError(err);
    } finally {
        existsStub?.restore();
        getStub?.restore();
        Sinon.restore();
    }
});

flight.landing();
