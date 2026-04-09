import test from 'node:test';
import assert from 'node:assert';
import Flight from './flight.js';
import { MockAgent, setGlobalDispatcher, getGlobalDispatcher } from 'undici';
import stream2buffer from '../lib/stream.js';

const flight = new Flight();

flight.init({ takserver: true });
flight.takeoff();
flight.user();

test('GET: api/video/lease - MediaServer Query', async () => {
    try {
        const res = await flight.fetch('/api/video/lease?impersonate=true&ephemeral=all', {
            method: 'GET',
            auth: {
                bearer: flight.token.admin
            }
        }, true);

        assert.deepEqual(res.body, {
            total: 0,
            items: []
        });
    } catch (err) {
        assert.ifError(err);
    }
});

let agent: MockAgent;
let originalDispatcher: any;
let leaseId: number;
let leasePath: string;
let publishedLeaseId: number;
let publishedLeasePath: string;

test('Mock Media Server Start', async () => {
    originalDispatcher = getGlobalDispatcher();
    agent = new MockAgent();
    agent.disableNetConnect();
    agent.enableNetConnect('localhost:5001');
    setGlobalDispatcher(agent);

    const mediaClient = agent.get('http://media-server:9997');
    mediaClient.intercept({
        path: '/path',
        method: 'POST'
    }).reply(200, {});

    mediaClient.intercept({
        path: '/v3/config/global/get',
        method: 'GET'
    }).reply(200, {
        api: true,
        apiAddress: ':9997',
        metrics: true,
        metricsAddress: ':9998',
        pprof: false,
        pprofAddress: '',
        playback: false,
        playbackAddress: '',
        rtsp: true,
        rtspAddress: ':8554',
        rtspsAddress: '',
        rtspAuthMethods: [],
        rtmp: true,
        rtmpAddress: ':1935',
        rtmpsAddress: '',
        hls: true,
        hlsAddress: ':8888',
        webrtc: false,
        webrtcAddress: '',
        srt: false,
        srtAddress: ''
    }).persist();

    mediaClient.intercept({
        path: '/path',
        method: 'GET'
    }).reply(200, {
        pageCount: 0,
        itemCount: 0,
        items: []
    }).persist();


    try {
        await flight.config!.models.Setting.generate({
            key: 'media::internal_url',
            value: 'http://media-server:9997'
        });

        await flight.config!.models.Setting.generate({
            key: 'media::public_url',
            value: 'https://video.example.com'
        });
    } catch (err) {
        assert.ifError(err);
    }
});

test('GET: api/video/service - Separate Internal and Public URLs', async () => {
    try {
        const res = await flight.fetch('/api/video/service', {
            method: 'GET',
            auth: {
                bearer: flight.token.admin
            }
        }, true);

        assert.equal(res.status, 200, 'Status 200');
        assert.equal(res.body.url, 'http://media-server:9997', 'Internal URL matches');
        assert.equal(res.body.internal, 'http://media-server:9997', 'Internal alias matches');
        assert.equal(res.body.external, 'https://video.example.com', 'Public URL matches');
        assert.equal(res.body.public, 'https://video.example.com', 'Public alias matches');
    } catch (err) {
        assert.ifError(err);
    }
});

test('POST: api/video/lease - Create Lease', async () => {
    try {
        const res = await flight.fetch('/api/video/lease', {
            method: 'POST',
            auth: {
                bearer: flight.token.admin
            },
            body: {
                name: 'Test Lease',
                duration: 3600
            }
        }, true);

        assert.equal(res.status, 200, 'Status 200');
        assert.ok(res.body.id, 'Lease ID returned');
        assert.equal(res.body.name, 'Test Lease', 'Name matches');
        leaseId = res.body.id;
        leasePath = res.body.path;
    } catch (err) {
        assert.ifError(err);
    }
});

test('GET: api/video/lease/:lease - Get Lease', async () => {
    try {
        const res = await flight.fetch(`/api/video/lease/${leaseId}`, {
            method: 'GET',
            auth: {
                bearer: flight.token.admin
            }
        }, true);

        assert.equal(res.status, 200, 'Status 200');
        assert.equal(res.body.id, leaseId, 'Lease ID matches');
        assert.equal(res.body.name, 'Test Lease', 'Name matches');
    } catch (err) {
        assert.ifError(err);
    }
});

test('PATCH: api/video/lease/:lease - Update Lease', async () => {
    const mediaClient = agent.get('http://media-server:9997');

    mediaClient.intercept({
        path: `/path/${leasePath}`,
        method: 'GET'
    }).reply(200, {
        name: leasePath,
        confName: leasePath,
        source: null,
        ready: true,
        readyTime: null,
        tracks: [],
        bytesReceived: 0,
        bytesSent: 0,
        readers: []
    });

    mediaClient.intercept({
        path: `/path/${leasePath}`,
        method: 'PATCH'
    }).reply(200, {});

    try {
        const res = await flight.fetch(`/api/video/lease/${leaseId}`, {
            method: 'PATCH',
            auth: {
                bearer: flight.token.admin
            },
            body: {
                name: 'Updated Lease Name',
                recording: false,
                publish: false
            }
        }, true);

        assert.equal(res.status, 200, 'Status 200');
        assert.equal(res.body.id, leaseId, 'Lease ID matches');
        assert.equal(res.body.name, 'Updated Lease Name', 'Name updated');
    } catch (err) {
        assert.ifError(err);
    }
});

test('POST: api/video/lease - Publish Lease via TAK v2 video API', async () => {
    let body = '';
    let requestUrl = '';

    flight.tak.mockMarti.unshift(async (request, response) => {
        if (request.method === 'POST' && request.url?.startsWith('/Marti/api/video')) {
            requestUrl = request.url;
            body = String(await stream2buffer(request));
            response.statusCode = 200;
            response.end();
            return true;
        }

        return false;
    });

    try {
        const res = await flight.fetch('/api/video/lease', {
            method: 'POST',
            auth: {
                bearer: flight.token.admin
            },
            body: {
                name: 'Published Lease',
                duration: 3600,
                publish: true,
                channel: 'Blue'
            }
        }, true);

        assert.equal(res.status, 200, 'Status 200');
        assert.ok(res.body.id, 'Lease ID returned');
        publishedLeaseId = res.body.id;
        publishedLeasePath = res.body.path;

        const payload = JSON.parse(body);
        assert.equal(requestUrl, '/Marti/api/video?group=Blue', 'V2 publish scopes video to selected group');
        assert.deepEqual(payload, {
            videoConnections: [{
                uuid: publishedLeasePath,
                active: true,
                alias: 'Published Lease',
                thumbnail: '',
                classification: '',
                feeds: [{
                    uuid: publishedLeasePath,
                    active: true,
                    alias: 'Published Lease',
                    url: `https://video.example.com/stream/${publishedLeasePath}/index.m3u8`,
                    macAddress: '',
                    roverPort: '-1',
                    ignoreEmbeddedKLV: '',
                    source: '',
                    networkTimeout: '5000',
                    bufferTime: '',
                    rtspReliable: '0',
                    thumbnail: '',
                    classification: '',
                    latitude: '',
                    longitude: '',
                    fov: '',
                    heading: '',
                    range: ''
                }]
            }]
        }, 'V2 payload includes richer feed metadata');
    } catch (err) {
        assert.ifError(err);
    }
});

test('DELETE: api/video/lease/:lease - Delete published TAK v2 feed', async () => {
    flight.tak.mockMarti.unshift(async (request, response) => {
        if (request.method === 'DELETE' && request.url === `/Marti/api/video/${publishedLeasePath}`) {
            response.statusCode = 200;
            response.end();
            return true;
        }

        return false;
    });

    const mediaClient = agent.get('http://media-server:9997');
    mediaClient.intercept({
        path: `/path/${publishedLeasePath}`,
        method: 'DELETE'
    }).reply(200, {});

    try {
        const res = await flight.fetch(`/api/video/lease/${publishedLeaseId}`, {
            method: 'DELETE',
            auth: {
                bearer: flight.token.admin
            }
        }, true);

        assert.equal(res.status, 200, 'Status 200');
        assert.ok(flight.tak.martiRequests.includes(`DELETE /Marti/api/video/${publishedLeasePath}`), 'V2 feed deleted');
        assert.ok(!flight.tak.martiRequests.includes('GET /Marti/vcm'), 'Legacy feed list not requested');
    } catch (err) {
        assert.ifError(err);
    }
});

test('Mock Media Server Stop', async () => {
    setGlobalDispatcher(originalDispatcher);
    await agent.close();
});

flight.landing();
