import test from 'node:test';
import assert from 'node:assert';
import Flight from './flight.js';
import { MockAgent, setGlobalDispatcher, getGlobalDispatcher } from 'undici';
import stream2buffer from '../lib/stream.js';

const flight = new Flight();

flight.init({ takserver: true });
flight.takeoff();
flight.user();
flight.user({ username: 'video-uploader', admin: false });

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
let legacyLeaseId: number;
let legacyLeasePath: string;

let activeGroups = [
    {
        name: 'Blue',
        direction: 'IN',
        created: '2026-01-01T00:00:00Z',
        type: 'SYSTEM',
        bitpos: 1,
        active: false,
        description: 'Blue'
    },
    {
        name: 'ESS',
        direction: 'IN',
        created: '2026-01-01T00:00:00Z',
        type: 'SYSTEM',
        bitpos: 2,
        active: false,
        description: 'ESS'
    }
];

let legacyFeeds: Array<{
    id: number;
    uuid: string;
}> = [];

function legacyVideoFeedXML(feeds: Array<{ id: number; uuid: string }>): string {
    return `<?xml version="1.0" encoding="UTF-8"?><videoConnections>${feeds.map((feed) => `<feed><id>${feed.id}</id><uid>${feed.uuid}</uid><alias>Legacy Feed</alias></feed>`).join('')}</videoConnections>`;
}

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

        await flight.config!.models.Setting.generate({
            key: 'video::legacy_uploader_username',
            value: 'video-uploader@example.com'
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
        assert.equal(res.body.publish_protocol, 'hls', 'Default publish protocol is HLS');
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
        assert.equal(res.body.publish_protocol, 'hls', 'Default publish protocol remains HLS');
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

test('POST: api/video/lease - Publish RTSP lease via legacy TAK uploader flow', async () => {
    let legacyBody = '';
    activeGroups = activeGroups.map((group) => ({ ...group, active: false }));
    legacyFeeds = [];

    flight.tak.mockMarti.unshift(async (request, response) => {
        if (request.method === 'GET' && request.url === '/Marti/api/groups/all?useCache=true') {
            response.setHeader('Content-Type', 'application/json');
            response.write(JSON.stringify({ version: '3', type: 'com.bbn.marti.remote.groups.Group', data: activeGroups }));
            response.end();
            return true;
        } else if (request.method === 'PUT' && request.url === '/Marti/api/groups/active') {
            activeGroups = JSON.parse(String(await stream2buffer(request)));
            response.statusCode = 200;
            response.end();
            return true;
        } else if (request.method === 'GET' && request.url === '/Marti/vcm') {
            response.setHeader('Content-Type', 'application/xml');
            response.write(legacyVideoFeedXML(legacyFeeds));
            response.end();
            return true;
        } else if (request.method === 'POST' && request.url?.startsWith('/Marti/vcu')) {
            legacyBody = String(await stream2buffer(request));
            const params = new URLSearchParams(legacyBody);
            legacyFeeds = [{ id: 101, uuid: String(params.get('uuid')) }];
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
                name: 'Published RTSP Lease',
                duration: 3600,
                publish: true,
                publish_protocol: 'rtsp',
                channel: 'Blue'
            }
        }, true);

        assert.equal(res.status, 200, 'Status 200');
        assert.equal(res.body.publish_protocol, 'rtsp', 'Selected publish protocol is persisted');
        legacyLeaseId = res.body.id;
        legacyLeasePath = res.body.path;

        const params = new URLSearchParams(legacyBody);
        assert.equal(params.get('uuid'), legacyLeasePath, 'Legacy payload uses lease path as UUID');
        assert.equal(params.get('alias'), 'Published RTSP Lease', 'Legacy payload uses lease name as alias');
        assert.equal(params.get('protocol'), 'rtsp', 'Legacy payload splits protocol');
        assert.equal(params.get('address'), 'video.example.com', 'Legacy payload splits host');
        assert.equal(params.get('port'), '8554', 'Legacy payload splits port');
        assert.equal(params.get('path'), `/${legacyLeasePath}`, 'Legacy payload splits path');
        assert.equal(params.get('timeout'), '5000', 'Legacy payload uses default timeout');
        assert.equal(params.get('roverPort'), '-1', 'Legacy payload uses default rover port');
        assert.deepEqual(activeGroups.filter((group) => group.active).map((group) => group.name), [], 'Legacy uploader groups are restored after publish');
        assert.ok(flight.tak.martiRequests.includes('PUT /Marti/api/groups/active'), 'Legacy publish updates active groups');
        assert.ok(flight.tak.martiRequests.includes('POST /Marti/vcu'), 'Legacy publish uses v1 upload endpoint');
        assert.ok(!flight.tak.martiRequests.includes('POST /Marti/api/video?group=Blue'), 'Legacy RTSP publish does not use v2 endpoint');
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

test('DELETE: api/video/lease/:lease - Delete published TAK legacy feed', async () => {
    activeGroups = activeGroups.map((group) => ({ ...group, active: false }));
    legacyFeeds = [{ id: 101, uuid: legacyLeasePath }];

    flight.tak.mockMarti.unshift(async (request, response) => {
        if (request.method === 'GET' && request.url === '/Marti/api/groups/all?useCache=true') {
            response.setHeader('Content-Type', 'application/json');
            response.write(JSON.stringify({ version: '3', type: 'com.bbn.marti.remote.groups.Group', data: activeGroups }));
            response.end();
            return true;
        } else if (request.method === 'PUT' && request.url === '/Marti/api/groups/active') {
            activeGroups = JSON.parse(String(await stream2buffer(request)));
            response.statusCode = 200;
            response.end();
            return true;
        } else if (request.method === 'GET' && request.url === '/Marti/vcm') {
            response.setHeader('Content-Type', 'application/xml');
            response.write(legacyVideoFeedXML(legacyFeeds));
            response.end();
            return true;
        } else if (request.method === 'DELETE' && request.url === '/Marti/vcm?id=101') {
            legacyFeeds = [];
            response.statusCode = 200;
            response.end();
            return true;
        }

        return false;
    });

    const mediaClient = agent.get('http://media-server:9997');
    mediaClient.intercept({
        path: `/path/${legacyLeasePath}`,
        method: 'DELETE'
    }).reply(200, {});

    try {
        const res = await flight.fetch(`/api/video/lease/${legacyLeaseId}`, {
            method: 'DELETE',
            auth: {
                bearer: flight.token.admin
            }
        }, true);

        assert.equal(res.status, 200, 'Status 200');
        assert.ok(flight.tak.martiRequests.includes('GET /Marti/vcm'), 'Legacy delete lists legacy feeds');
        assert.ok(flight.tak.martiRequests.includes('DELETE /Marti/vcm?id=101'), 'Legacy feed deleted');
        assert.deepEqual(activeGroups.filter((group) => group.active).map((group) => group.name), [], 'Legacy uploader groups are restored after delete');
        assert.ok(!flight.tak.martiRequests.includes(`DELETE /Marti/api/video/${legacyLeasePath}`), 'Legacy delete does not use v2 endpoint');
    } catch (err) {
        assert.ifError(err);
    }
});

test('Mock Media Server Stop', async () => {
    setGlobalDispatcher(originalDispatcher);
    await agent.close();
});

flight.landing();
