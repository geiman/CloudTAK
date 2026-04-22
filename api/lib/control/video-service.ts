import Err from '@openaddresses/batch-error';
import jwt from 'jsonwebtoken';
import Config from '../config.js';
import { eq } from 'drizzle-orm'
import { AuthResourceAccess } from '../auth.js';
import { Type, Static } from '@sinclair/typebox';
import { VideoLease } from '../schema.js';
import { VideoLeaseResponse } from '../types.js';
import { VideoLease_SourceType } from '../enums.js';
import fetch from '../fetch.js';
import { Agent } from 'undici';
import { TAKAPI, APIAuthCertificate } from '@tak-ps/node-tak';
import xmljs from 'xml-js';

export enum ProtocolPopulation {
    TEMPLATE,
    WRITE,
    READ
}

export enum Protocol {
    RTSP = "rtsp",
    RTML = "rtmp",
    HLS = "hls",
    WEBRTC = "webrtc",
    SRT = "srt",
}

export enum Action {
    PUBLISH = "publish",
    READ = "read",
    PLAYBACK = "playback",
    API = "api",
    METRICS = "metrics",
    PPROF = "pprof",
}

export const TakPublishProtocolSchema = Type.Union([
    Type.Literal(Protocol.HLS),
    Type.Literal(Protocol.RTSP),
    Type.Literal(Protocol.RTML),
    Type.Literal(Protocol.SRT),
]);

export type TakPublishProtocol = 'hls' | 'rtsp' | 'rtmp' | 'srt';

export const Protocols = Type.Object({
    rtmp: Type.Optional(Type.Object({
        name: Type.String(),
        url: Type.String()
    })),
    rtsp: Type.Optional(Type.Object({
        name: Type.String(),
        url: Type.String()
    })),
    webrtc: Type.Optional(Type.Object({
        name: Type.String(),
        url: Type.String()
    })),
    hls: Type.Optional(Type.Object({
        name: Type.String(),
        url: Type.String()
    })),
    srt: Type.Optional(Type.Object({
        name: Type.String(),
        url: Type.String()
    }))
})

export const VideoConfig = Type.Object({
    api: Type.Boolean(),
    apiAddress: Type.String(),

    metrics: Type.Boolean(),
    metricsAddress: Type.String(),

    pprof: Type.Boolean(),
    pprofAddress: Type.String(),

    playback: Type.Boolean(),
    playbackAddress: Type.String(),

    rtsp: Type.Boolean(),
    rtspAddress: Type.String(),
    rtspsAddress: Type.String(),
    rtspAuthMethods: Type.Array(Type.String()),

    rtmp: Type.Boolean(),
    rtmpAddress: Type.String(),
    rtmpsAddress: Type.String(),

    hls: Type.Boolean(),
    hlsAddress: Type.String(),

    webrtc: Type.Boolean(),
    webrtcAddress: Type.String(),

    srt: Type.Boolean(),
    srtAddress: Type.String(),
})

export const PathConfig = Type.Object({
    name: Type.String(),
    source: Type.String(),
    record: Type.Boolean(),
});

export const PathListItem = Type.Object({
    name: Type.String(),
    confName: Type.String(),

    source: Type.Union([
        Type.Object({
            id: Type.String(),
            type: Type.String(),
        }),
        Type.Null()
    ]),

    ready: Type.Boolean(),
    readyTime: Type.Union([Type.Null(), Type.String()]),
    tracks: Type.Array(Type.String()),
    bytesReceived: Type.Integer(),
    bytesSent: Type.Integer(),
    readers: Type.Array(Type.Object({
        type: Type.String(),
        id: Type.String()
    }))
});

export const Recording = Type.Object({
    name: Type.String(),
    segmenets: Type.Array(Type.Object({
        start: Type.String()
    }))
});

export const PathsList = Type.Object({
    pageCount: Type.Integer(),
    itemCount: Type.Integer(),
    items: Type.Array(PathListItem)
})

export const Configuration = Type.Object({
    configured: Type.Boolean(),
    url: Type.Optional(Type.String()),
    external: Type.Optional(Type.String()),
    internal: Type.Optional(Type.String()),
    public: Type.Optional(Type.String()),
    config: Type.Optional(VideoConfig),
    paths: Type.Optional(Type.Array(PathListItem))
});

export default class VideoServiceControl {
    config: Config;
    static legacyUploaderLocks = new Map<string, Promise<void>>();

    constructor(config: Config) {
        this.config = config;
    }

    async settingUrl(key: 'media::url' | 'media::internal_url' | 'media::public_url'): Promise<string | null> {
        try {
            const url = await this.config.models.Setting.from(key);
            if (!url.value || typeof url.value !== 'string') return null;

            new URL(url.value);
            return url.value;
        } catch (err) {
            if (err instanceof Error && err.message.includes('Not Found')) {
                return null;
            } else if (err instanceof TypeError && err.message.includes('Invalid URL')) {
                throw new Err(400, null, `Invalid ${key} setting`);
            } else {
                throw new Err(500, err instanceof Error ? err : new Error(String(err)), 'Media Service Configuration Error');
            }
        }
    }

    async mediaSettings(): Promise<{
        configured: boolean;
        url?: string;
        internal_url?: string;
        public_url?: string;
        token?: string;
    }> {
        const legacy = await this.settingUrl('media::url');
        const internal = await this.settingUrl('media::internal_url');
        const publicUrl = await this.settingUrl('media::public_url');

        const resolvedInternal = internal || legacy || publicUrl;
        const resolvedPublic = publicUrl || legacy || internal;

        if (!resolvedInternal) {
            return {
                configured: false
            };
        }

        return {
            configured: true,
            url: resolvedInternal,
            internal_url: resolvedInternal,
            public_url: resolvedPublic || resolvedInternal,
            token: jwt.sign({
                internal: true,
                access: AuthResourceAccess.MEDIA
            }, this.config.SigningSecret)
        }
    }

    async url(): Promise<URL | null> {
        const settings = await this.mediaSettings();
        if (!settings.configured || !settings.public_url) return null;

        return new URL(settings.public_url);
    }

    async settings(): Promise<{
        configured: boolean;
        url?: string;
        internal_url?: string;
        public_url?: string;
        token?: string;
    }> {
        return await this.mediaSettings();
    }

    headers(token?: string): Headers {
        const headers = new Headers();
        if (token) {
            headers.append('Authorization', `Bearer ${token}`);
        }

        return headers;
    }

    defaultPort(protocol: string): string {
        switch (protocol) {
        case 'http:':
            return '80';
        case 'https:':
            return '443';
        case 'rtsp:':
            return '554';
        case 'rtmp:':
            return '1935';
        case 'rtmps:':
            return '443';
        case 'srt:':
            return '9000';
        default:
            return '';
        }
    }

    async takAuthForLease(lease: Static<typeof VideoLeaseResponse>): Promise<{
        cert: string;
        key: string;
    }> {
        if (lease.username) {
            return (await this.config.models.Profile.from(lease.username)).auth;
        } else if (lease.connection) {
            return (await this.config.models.Connection.from(lease.connection)).auth;
        } else {
            return this.config.serverCert();
        }
    }

    takVideoDispatcher(auth: {
        cert: string;
        key: string;
    }): Agent {
        return new Agent({
            connect: {
                cert: auth.cert,
                key: auth.key,
                rejectUnauthorized: false,
            }
        });
    }

    takVideoUrl(pathname: string): URL {
        return new URL(pathname, String(this.config.server.api));
    }

    takePublishProtocol(lease: Static<typeof VideoLeaseResponse>): TakPublishProtocol {
        switch (lease.publish_protocol) {
        case Protocol.RTSP:
        case Protocol.RTML:
        case Protocol.SRT:
            return lease.publish_protocol;
        case Protocol.HLS:
        default:
            return Protocol.HLS;
        }
    }

    async takLegacyUploaderProfile(): Promise<Awaited<ReturnType<Config['models']['Profile']['from']>>> {
        const { value } = await this.config.models.Setting.typed('video::legacy_uploader_username', '');
        const username = String(value || '').trim();

        if (!username) {
            throw new Err(400, null, 'Legacy TAK video uploader username is not configured');
        }

        const profile = await this.config.models.Profile.from(username);

        if (profile.system_admin) {
            throw new Err(400, null, 'Legacy TAK video uploader must not be a system administrator');
        }

        return profile;
    }

    async takLegacyUploaderApi(): Promise<{
        profile: Awaited<ReturnType<Config['models']['Profile']['from']>>;
        api: TAKAPI;
    }> {
        const profile = await this.takLegacyUploaderProfile();

        return {
            profile,
            api: await TAKAPI.init(
                new URL(String(this.config.server.api)),
                new APIAuthCertificate(profile.auth.cert, profile.auth.key)
            )
        };
    }

    async withLegacyUploaderLock<T>(key: string, task: () => Promise<T>): Promise<T> {
        const previous = VideoServiceControl.legacyUploaderLocks.get(key);
        let release: () => void = () => {};
        const gate = new Promise<void>((resolve) => {
            release = resolve;
        });

        VideoServiceControl.legacyUploaderLocks.set(key, gate);

        if (previous) {
            await previous.catch(() => undefined);
        }

        try {
            return await task();
        } finally {
            release();

            if (VideoServiceControl.legacyUploaderLocks.get(key) === gate) {
                VideoServiceControl.legacyUploaderLocks.delete(key);
            }
        }
    }

    async wait(ms: number): Promise<void> {
        await new Promise((resolve) => setTimeout(resolve, ms));
    }

    activeGroupNames(groups: Array<{
        name: string;
        active: boolean;
    }>): string[] {
        return [...new Set(
            groups
                .filter((group) => group.active)
                .map((group) => group.name)
        )].sort();
    }

    async verifyLegacyUploaderGroups(api: TAKAPI, expectedNames: string[]): Promise<Array<{
        name: string;
        direction: string;
        created: string;
        type: string;
        bitpos: number;
        active: boolean;
        description?: string;
    }>> {
        const expected = [...expectedNames].sort();
        const deadline = Date.now() + 2000;
        let last: Array<{
            name: string;
            direction: string;
            created: string;
            type: string;
            bitpos: number;
            active: boolean;
            description?: string;
        }> | undefined;

        while (Date.now() <= deadline) {
            last = (await api.Group.list({ useCache: true })).data;

            if (JSON.stringify(this.activeGroupNames(last)) === JSON.stringify(expected)) {
                return last;
            }

            await this.wait(100);
        }

        throw new Err(500, null, `Legacy video uploader active groups did not converge to: ${expected.join(', ')}. Last active groups: ${(last ? this.activeGroupNames(last) : []).join(', ')}`);
    }

    async setLegacyUploaderGroups(api: TAKAPI, groups: Array<{
        name: string;
        direction: string;
        created: string;
        type: string;
        bitpos: number;
        active: boolean;
        description?: string;
    }>, targetNames: string[]): Promise<void> {
        const missing = targetNames.filter((target) => !groups.some((group) => group.name === target));
        if (missing.length) {
            throw new Err(400, null, `Legacy uploader is not a member of TAK group(s): ${missing.join(', ')}`);
        }

        await api.Group.update(groups.map((group) => ({
            ...group,
            active: targetNames.includes(group.name)
        })));

        await this.verifyLegacyUploaderGroups(api, targetNames);
    }

    async withLegacyUploaderGroups<T>(lease: Static<typeof VideoLeaseResponse>, task: (api: TAKAPI) => Promise<T>): Promise<T> {
        if (!lease.channel) throw new Err(400, null, 'Channel must be set when publish is true');
        const targetChannel = lease.channel;

        const { profile, api } = await this.takLegacyUploaderApi();
        const lockKey = `${String(this.config.server.api)}|${profile.username}`;

        return await this.withLegacyUploaderLock(lockKey, async () => {
            const originalGroups = (await api.Group.list({ useCache: true })).data;
            let taskErr: unknown;
            let restoreErr: unknown;
            let result: T | undefined;

            try {
                await this.setLegacyUploaderGroups(api, originalGroups, [targetChannel]);
                result = await task(api);
            } catch (err) {
                taskErr = err;
            } finally {
                try {
                    await this.setLegacyUploaderGroups(api, originalGroups, this.activeGroupNames(originalGroups));
                } catch (err) {
                    restoreErr = err;
                    console.error('Failed to restore legacy uploader active groups', err);
                }
            }

            if (taskErr) throw taskErr;
            if (restoreErr) throw restoreErr;
            if (result === undefined) throw new Err(500, null, 'Legacy uploader task returned no result');

            return result;
        });
    }

    parseLegacyVideoFeedList(xml: string): Array<{
        id: number;
        uuid: string;
    }> {
        const parsed = xmljs.xml2js(xml, { compact: true }) as {
            videoConnections?: {
                feed?: Array<Record<string, { _text?: string }>> | Record<string, { _text?: string }>;
            };
        };

        const rawFeeds = parsed.videoConnections?.feed;
        const feeds = Array.isArray(rawFeeds)
            ? rawFeeds
            : rawFeeds
                ? [rawFeeds]
                : [];

        return feeds
            .map((feed) => ({
                id: Number(feed.id?._text),
                uuid: String(feed.uid?._text || ''),
            }))
            .filter((feed) => Number.isFinite(feed.id) && feed.uuid.length > 0);
    }

    async legacyTakVideoFeedByUUID(api: TAKAPI, uuid: string): Promise<{
        id: number;
        uuid: string;
    } | undefined> {
        const url = this.takVideoUrl('/Marti/vcm');
        const xml = await api.fetch(url, { method: 'GET' }) as string;
        return this.parseLegacyVideoFeedList(xml).find((feed) => feed.uuid === uuid);
    }

    async legacyTakVideoConnectionPayload(lease: Static<typeof VideoLeaseResponse>): Promise<URLSearchParams> {
        const protocols = await this.protocols(lease, ProtocolPopulation.READ);
        const publishProtocol = this.takePublishProtocol(lease);
        const feedProtocol = protocols[publishProtocol];

        if (!feedProtocol) {
            throw new Err(400, null, `Configured TAK publish protocol is unavailable: ${publishProtocol.toUpperCase()}`);
        }

        const feedUrl = new URL(feedProtocol.url);
        const payload = new URLSearchParams();

        payload.set('uuid', lease.path);
        payload.set('active', 'on');
        payload.set('alias', lease.name);
        payload.set('protocol', feedUrl.protocol.replace(/:$/, ''));
        payload.set('address', feedUrl.hostname);
        payload.set('port', feedUrl.port || this.defaultPort(feedUrl.protocol));
        payload.set('path', `${feedUrl.pathname}${feedUrl.search}`);
        payload.set('preferredMacAddress', '');
        payload.set('roverPort', '-1');
        payload.set('timeout', '5000');
        payload.set('buffer', '');
        payload.set('latitude', '');
        payload.set('longitude', '');
        payload.set('fov', '');
        payload.set('heading', '');
        payload.set('range', '');
        payload.set('thumbnail', '');
        payload.set('classification', '');

        return payload;
    }

    async takVideoConnectionPayload(lease: Static<typeof VideoLeaseResponse>): Promise<{
        uuid: string;
        active: boolean;
        alias: string;
        thumbnail: string;
        classification: string;
        feeds: Array<Record<string, string | boolean>>;
    }> {
        const protocols = await this.protocols(lease, ProtocolPopulation.READ);
        const feedProtocol = protocols.hls;

        if (!feedProtocol) throw new Err(400, null, 'Configured TAK publish protocol is unavailable: HLS');

        return {
            uuid: lease.path,
            active: true,
            alias: lease.name,
            thumbnail: '',
            classification: '',
            feeds: [{
                uuid: lease.path,
                active: true,
                alias: lease.name,
                url: feedProtocol.url,
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
                range: '',
            }]
        };
    }

    async publishTakVideoFeed(lease: Static<typeof VideoLeaseResponse>): Promise<void> {
        if (!lease.channel) throw new Err(400, null, 'Channel must be set when publish is true');

        const publishProtocol = this.takePublishProtocol(lease);

        if (publishProtocol !== Protocol.HLS) {
            await this.withLegacyUploaderGroups(lease, async (api) => {
                const existing = await this.legacyTakVideoFeedByUUID(api, lease.path);
                const url = this.takVideoUrl('/Marti/vcu');
                if (existing) url.searchParams.set('feedId', String(existing.id));

                await api.fetch(url, {
                    method: 'POST',
                    body: await this.legacyTakVideoConnectionPayload(lease),
                });
            });

            return;
        }

        const auth = await this.takAuthForLease(lease);
        const dispatcher = this.takVideoDispatcher(auth);
        const url = this.takVideoUrl('/Marti/api/video');
        url.searchParams.append('group', lease.channel);

        try {
            const res = await fetch(url, {
                method: 'POST',
                dispatcher,
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    videoConnections: [await this.takVideoConnectionPayload(lease)]
                }),
            });

            if (!res.ok) {
                throw new Err(res.status, new Error(await res.text()), 'Failed to publish TAK video feed');
            }
        } finally {
            await dispatcher.close();
        }
    }

    async deleteMediaPath(pathid: string): Promise<void> {
        const video = await this.settings();
        if (!video.configured) throw new Err(400, null, 'Media Integration is not configured');

        const headers = this.headers(video.token);
        const url = new URL(`/path/${pathid}`, video.internal_url);
        if (!url.port) url.port = '9997';

        const res = await fetch(url, {
            method: 'DELETE',
            headers: Object.fromEntries(headers.entries()),
        });

        if (!res.ok && res.status !== 404) {
            throw new Err(res.status, null, await res.text());
        }
    }

    async upsertMediaPath(pathid: string, opts: {
        source?: string | null;
        record: boolean;
    }): Promise<void> {
        const video = await this.settings();
        if (!video.configured) throw new Err(400, null, 'Media Integration is not configured');

        const headers = this.headers(video.token);
        headers.append('Content-Type', 'application/json');

        const payload = {
            name: pathid,
            source: opts.source,
            record: opts.record,
        };

        try {
            await this.path(pathid);

            const url = new URL(`/path/${pathid}`, video.internal_url);
            if (!url.port) url.port = '9997';

            const res = await fetch(url, {
                method: 'PATCH',
                headers: Object.fromEntries(headers.entries()),
                body: JSON.stringify(payload),
            });

            if (!res.ok) throw new Err(500, null, await res.text());
        } catch (err) {
            if (!(err instanceof Err) || err.status !== 404) {
                throw err;
            }

            const url = new URL('/path', video.internal_url);
            if (!url.port) url.port = '9997';

            const res = await fetch(url, {
                method: 'POST',
                headers: Object.fromEntries(headers.entries()),
                body: JSON.stringify(payload),
            });

            if (!res.ok) {
                const text = await res.text();

                if (text.includes('path already exists')) {
                    const patchUrl = new URL(`/path/${pathid}`, video.internal_url);
                    if (!patchUrl.port) patchUrl.port = '9997';

                    const patchRes = await fetch(patchUrl, {
                        method: 'PATCH',
                        headers: Object.fromEntries(headers.entries()),
                        body: JSON.stringify(payload),
                    });

                    if (!patchRes.ok) throw new Err(500, null, await patchRes.text());
                } else {
                    throw new Err(500, null, text);
                }
            }
        }
    }

    async rollbackGeneratedLease(lease: Static<typeof VideoLeaseResponse>, opts: {
        deleteTakFeed: boolean;
        deleteMediaPath: boolean;
    }): Promise<void> {
        if (opts.deleteTakFeed && lease.publish) {
            try {
                await this.deleteTakVideoFeed(lease);
            } catch (err) {
                console.error('Failed to roll back TAK video feed after lease create error', err);
            }
        }

        if (opts.deleteMediaPath) {
            try {
                await this.deleteMediaPath(lease.path);
            } catch (err) {
                console.error('Failed to roll back media path after lease create error', err);
            }
        }

        try {
            await this.config.models.VideoLease.delete(lease.id);
        } catch (err) {
            console.error('Failed to roll back lease record after lease create error', err);
        }
    }

    async deleteTakVideoFeed(lease: Static<typeof VideoLeaseResponse>): Promise<void> {
        const publishProtocol = this.takePublishProtocol(lease);

        if (publishProtocol !== Protocol.HLS) {
            await this.withLegacyUploaderGroups(lease, async (api) => {
                const existing = await this.legacyTakVideoFeedByUUID(api, lease.path);
                if (!existing) return;

                const url = this.takVideoUrl('/Marti/vcm');
                url.searchParams.set('id', String(existing.id));

                const res = await api.fetch(url, { method: 'DELETE' }, true);
                if (!res.ok && res.status !== 404) {
                    throw new Err(res.status, null, 'Failed to delete TAK legacy video feed');
                }
            });

            return;
        }

        const auth = await this.takAuthForLease(lease);
        const dispatcher = this.takVideoDispatcher(auth);

        try {
            const url = this.takVideoUrl(`/Marti/api/video/${encodeURIComponent(lease.path)}`);
            const res = await fetch(url, {
                method: 'DELETE',
                dispatcher,
            });

            if (!res.ok && res.status !== 404) {
                throw new Err(res.status, new Error(await res.text()), 'Failed to delete TAK video feed');
            }
        } finally {
            await dispatcher.close();
        }
    }

    async configuration(): Promise<Static<typeof Configuration>> {
        const video = await this.settings();

        if (!video.configured) return video;

        const headers = this.headers(video.token);

        const url = new URL('/v3/config/global/get', video.internal_url);
        if (!url.port) url.port = '9997';

        const res = await fetch(url, { headers: Object.fromEntries(headers.entries()) })
        if (!res.ok) throw new Err(500, null, await res.text())
        const body = await res.typed(VideoConfig);

        // TODO support paging
        const urlPaths = new URL('/path', video.internal_url);
        if (!urlPaths.port) urlPaths.port = '9997';

        const resPaths = await fetch(urlPaths, { headers: Object.fromEntries(headers.entries()) })
        if (!resPaths.ok) throw new Err(500, null, await resPaths.text())

        const paths = await resPaths.typed(PathsList);

        return {
            configured: video.configured,
            url: video.internal_url,
            external: video.public_url,
            internal: video.internal_url,
            public: video.public_url,
            config: body,
            paths: paths.items,
        };
    }

    async protocols(
        lease: Static<typeof VideoLeaseResponse>,
        populated = ProtocolPopulation.TEMPLATE
    ): Promise<Static<typeof Protocols>> {
        const protocols: Static<typeof Protocols> = {};
        const c = await this.configuration();

        if (!c.configured || !c.external) return protocols;

        if (c.config && c.config.rtsp) {
            // Format: rtsp://localhost:8554/mystream
            const url = new URL(`/${lease.path}`, c.external.replace(/^http(s)?:/, 'rtsp:'))
            url.port = c.config.rtspAddress.replace(':', '');

            if (lease.read_user && lease.stream_user) {
                if (populated === ProtocolPopulation.READ && lease.read_user && lease.read_pass) {
                    url.username = lease.read_user;
                    url.password = lease.read_pass;

                    protocols.rtsp = {
                        name: 'Real-Time Streaming Protocol (RTSP)',
                        url: String(url)
                    }
                } else if (populated === ProtocolPopulation.WRITE && lease.stream_user && lease.stream_pass) {
                    url.username = lease.stream_user;
                    url.password = lease.stream_pass;

                    protocols.rtsp = {
                        name: 'Real-Time Streaming Protocol (RTSP)',
                        url: String(url)
                    }
                } else {
                    const rtspurl = new URL(String(url))
                    rtspurl.username = 'username';
                    rtspurl.password = 'password';

                    protocols.rtsp = {
                        name: 'Real-Time Streaming Protocol (RTSP)',
                        url: String(rtspurl).replace(/username:password/, '{{username}}:{{password}}')
                    }
                }
            } else {
                protocols.rtsp = {
                    name: 'Real-Time Streaming Protocol (RTSP)',
                    url: String(url)
                }
            }
        }

        if (c.config && c.config.rtmp) {
            // Format: rtmp://localhost/mystream
            const url = new URL(`/${lease.path}`, c.external.replace(/^http(s)?:/, 'rtmp:'))
            url.port = c.config.rtmpAddress.replace(':', '');

            protocols.rtmp = {
                name: 'Real-Time Messaging Protocol (RTMP)',
                url: String(url)
            }

            if (lease.stream_user && lease.read_user) {
                if (populated === ProtocolPopulation.TEMPLATE) {
                    protocols.rtmp.url = `${protocols.rtmp.url}?user={{username}}&pass={{password}}`;
                } else if (populated === ProtocolPopulation.READ) {
                    protocols.rtmp.url = `${protocols.rtmp.url}?user=${lease.read_user}&pass=${lease.read_pass}`;
                } else if (populated === ProtocolPopulation.WRITE) {
                    protocols.rtmp.url = `${protocols.rtmp.url}?user=${lease.stream_user}&pass=${lease.stream_pass}`;
                }
            }

        }

        if (c.config && c.config.srt) {
            // Format: srt://localhost:8890?streamid=publish:mystream
            const url = new URL(c.external.replace(/^http(s)?:/, 'srt:'))
            url.port = c.config.srtAddress.replace(':', '');

            if (lease.stream_user && lease.read_user) {
                if (populated === ProtocolPopulation.READ) {
                    protocols.srt = {
                        name: 'Secure Reliable Transport (SRT)',
                        url: String(url) + `?streamid={{mode}}:${lease.path}:${lease.read_user}}:${lease.read_pass}`
                    }
                } else if (populated === ProtocolPopulation.WRITE) {
                    protocols.srt = {
                        name: 'Secure Reliable Transport (SRT)',
                        url: String(url) + `?streamid={{mode}}:${lease.path}:${lease.stream_user}}:${lease.stream_pass}`
                    }
                } else {
                    protocols.srt = {
                        name: 'Secure Reliable Transport (SRT)',
                        url: String(url) + `?streamid={{mode}}:${lease.path}:{{username}}:{{password}}`
                    }
                }
            } else {
                protocols.srt = {
                    name: 'Secure Reliable Transport (SRT)',
                    url: String(url) + `?streamid={{mode}}:${lease.path}`
                }
            }
        }

        if (c.config && c.config.hls) {
            // Format: http://localhost:9997/mystream/index.m3u8 - Proxied
            const url = new URL(`/stream/${lease.path}/index.m3u8`, c.external);

            if (lease.stream_user && lease.read_user) {
                if (populated === ProtocolPopulation.READ && lease.read_user && lease.read_pass) {
                    const hlsurl = new URL(String(url))
                    hlsurl.username = lease.read_user;
                    hlsurl.password = lease.read_pass;

                    protocols.hls = {
                        name: 'HTTP Live Streaming (HLS)',
                        url: String(hlsurl)
                    }
                } else if (populated === ProtocolPopulation.WRITE && lease.stream_user && lease.stream_pass) {
                    const hlsurl = new URL(String(url))
                    hlsurl.username = lease.stream_user;
                    hlsurl.password = lease.stream_pass;

                    protocols.hls = {
                        name: 'HTTP Live Streaming (HLS)',
                        url: String(hlsurl)
                    }
                } else {
                    const hlsurl = new URL(String(url))
                    hlsurl.username = 'username';
                    hlsurl.password = 'password';

                    protocols.hls = {
                        name: 'HTTP Live Streaming (HLS)',
                        url: String(hlsurl).replace(/username:password/, '{{username}}:{{password}}')
                    }
                }
            } else {
                protocols.hls = {
                    name: 'HTTP Live Streaming (HLS)',
                    url: String(url)
                }
            }
        }

        if (c.config && c.config.webrtc) {
            // Format: http://localhost:8889/mystream
            const url = new URL(`/${lease.path}`, c.external);
            url.port = c.config.webrtcAddress.replace(':', '');

            protocols.webrtc = {
                name: 'Web Real-Time Communication (WebRTC)',
                url: String(url)
            }
        }

        return protocols;
    }

    async updateSecure(
        lease: Static<typeof VideoLeaseResponse>,
        secure: boolean,
        rotate?: boolean
    ): Promise<void> {
        const video = await this.settings();

        if (!video.configured) return;

        if (secure && (!lease.stream_user || !lease.stream_pass || !lease.read_user || !lease.read_pass)) {
            await this.config.models.VideoLease.commit(lease.id, {
                stream_user: `write${lease.id}`,
                stream_pass: Math.random().toString(20).substr(2, 6),
                read_user: `read${lease.id}`,
                read_pass: Math.random().toString(20).substr(2, 6)
            });
        } else if (secure && rotate) {
            await this.config.models.VideoLease.commit(lease.id, {
                read_user: `read${lease.id}`,
                read_pass: Math.random().toString(20).substr(2, 6)
            });
        } else if (!secure && (lease.stream_user || lease.stream_pass || lease.read_user || lease.read_pass)) {
            await this.config.models.VideoLease.commit(lease.id, {
                stream_user: null,
                stream_pass: null,
                read_user: null,
                read_pass: null
            });
        }
    }

    async generate(opts: {
        name: string;
        ephemeral: boolean;
        expiration: string | null;
        source_id: string | null | undefined;
        source_type?: VideoLease_SourceType;
        source_model?: string;
        path: string;
        username?: string;
        connection?: number;
        layer?: number;
        recording: boolean;
        publish: boolean;
        publish_protocol: TakPublishProtocol;
        secure: boolean;
        share: boolean;
        channel?: string | null;
        proxy?: string | null;
    }): Promise<Static<typeof VideoLeaseResponse>> {
        const video = await this.settings();
        if (!video.configured) throw new Err(400, null, 'Media Integration is not configured');

        if (opts.username && opts.connection) {
            throw new Err(400, null, 'Either username or connection must be set but not both');
        } else if (opts.share && !opts.channel) {
            throw new Err(400, null, 'Channel must be set when share is true');
        } else if (opts.publish && !opts.channel) {
            throw new Err(400, null, 'Channel must be set when publish is true');
        }

        const lease = await this.config.models.VideoLease.generate({
            name: opts.name,
            expiration: opts.expiration,
            ephemeral: opts.ephemeral,
            path: opts.path,
            recording: opts.recording,
            publish: opts.publish,
            publish_protocol: opts.publish_protocol,
            source_id: opts.source_id,
            source_type: opts.source_type,
            source_model: opts.source_model,
            username: opts.username,
            connection: opts.connection,
            layer: opts.layer,
            share: opts.share,
            channel: opts.channel,
            proxy: opts.proxy
        });

        await this.updateSecure(lease, opts.secure);
        let mediaPathCreated = false;
        let takFeedPublished = false;

        try {
            if (lease.proxy) {
                try {
                    const proxy = new URL(lease.proxy);

                    // Check for HLS Errors
                    if (['http:', 'https:'].includes(proxy.protocol)) {
                        const res = await fetch(proxy);

                        if (res.status === 404) {
                            throw new Err(400, null, 'External Video Server reports Video Stream not found');
                        } else if (!res.ok) {
                            throw new Err(res.status, null, `External Video Server failed stream video - HTTP Error ${res.status}, ${await res.text()}`);
                        }
                    } else {
                        await this.upsertMediaPath(lease.path, {
                            source: lease.proxy,
                            record: lease.recording,
                        });
                        mediaPathCreated = true;
                    }
                } catch (err) {
                    if (err instanceof Err) {
                        throw err;
                    // @ts-expect-error code is not defined in type
                    } else if (err instanceof TypeError && err.code === 'ERR_INVALID_URL') {
                        throw new Err(400, null, 'Invalid Video Stream URL');
                    } else {
                        throw new Err(500, err instanceof Error ? err : new Error(String(err)), 'Failed to generate proxy stream');
                    }
                }
            } else {
                await this.upsertMediaPath(lease.path, {
                    record: lease.recording,
                });
                mediaPathCreated = true;
            }

            if (lease.publish) {
                await this.publishTakVideoFeed(lease);
                takFeedPublished = true;
            }

            return lease;
        } catch (err) {
            await this.rollbackGeneratedLease(lease, {
                deleteTakFeed: takFeedPublished,
                deleteMediaPath: mediaPathCreated,
            });
            throw err;
        }
    }

    /**
     * Fetches a lease and performs permission checks based on the provided options
     *
     * @param leaseid Integer Lease ID or String Lease Path
     *
     * @param opts Options containing connection, username,
     * @param opts.connection Connection ID if accessing via Connection
     * @param opts.username Username if accessing via CloudTAK Map
     * @param opts.admin Boolean indicating if the user is an admin
     */
    async from(
        id: number | string,
        opts: {
            connection?: number
            username?: string
            admin: boolean
        }
    ): Promise<Static<typeof VideoLeaseResponse>> {
        let lease;

        if (typeof id === 'string') {
            lease = await this.config.models.VideoLease.from(eq(VideoLease.path, id));
        } else {
            lease = await this.config.models.VideoLease.from(id);
        }

        if (opts.admin) return lease;

        if (opts.connection) {
            if (lease.connection !== opts.connection) {
                throw new Err(400, null, 'Connections can only access leases created in the context of the connection');
            } else {
                return lease;
            }
        } else if (opts.username) {
            if (opts.username === lease.username) {
                return lease;
            } else {
                const profile = await this.config.models.Profile.from(opts.username);
                const api = await TAKAPI.init(new URL(String(this.config.server.api)), new APIAuthCertificate(profile.auth.cert, profile.auth.key));
                const groups = (await api.Group.list({ useCache: true }))
                    .data.map((group) => group.name);

                if (lease.username !== opts.username && (!lease.share || !lease.channel || !groups.includes(lease.channel))) {
                    throw new Err(400, null, 'You can only access a lease you created or that is assigned to a channel you are in');
                }

                return lease;
            }
        } else {
            return lease;
        }
    }

    async commit(
        leaseid: number,
        body: {
            name?: string,
            channel?: string | null,
            share?: boolean,
            secure?: boolean,
            secure_rotate?: boolean
            expiration?: string | null,
            recording?: boolean,
            publish?: boolean,
            publish_protocol?: TakPublishProtocol,
            source_id: string | null | undefined;
            source_type?: VideoLease_SourceType,
            source_model?: string,
            proxy?: string | null,
        },
        opts: {
            connection?: number;
            username?: string;
            admin: boolean;
        }
    ): Promise<Static<typeof VideoLeaseResponse>> {
        const video = await this.settings();
        if (!video.configured) throw new Err(400, null, 'Media Integration is not configured');

        let lease = await this.from(leaseid, opts);

        if (lease.connection && !opts.connection) {
            throw new Err(400, null, 'Lease must be edited in the context of a Connection');
        } else if (lease.username && !opts.username) {
            throw new Err(400, null, 'Lease must be edited in the context of the CloudTAK Map');
        } else if (
            (body.share && body.channel === undefined && !lease.channel)
            || (body.share && body.channel === null)
            || (lease.share && body.channel === null)
        ) {
            throw new Err(400, null, 'Channel must be set when share is true');
        } else if (
            (body.publish && body.channel === undefined && !lease.channel)
            || (body.publish && body.channel === null)
            || (lease.publish && body.channel === null)
        ) {
            throw new Err(400, null, 'Channel must be set when publish is true');
        }

        if (body.secure !== undefined) {
            // Performs Permission Check
            await this.updateSecure(lease, body.secure, body.secure_rotate);
        }

        const wasPublished = lease.publish;

        lease = await this.config.models.VideoLease.commit(leaseid, body);

        try {
            if (wasPublished) {
                try {
                    await this.deleteTakVideoFeed(lease);
                } catch (err) {
                    console.error(err);
                }
            }

            if (lease.publish) {
                try {
                    await this.publishTakVideoFeed(lease);
                } catch (err) {
                    console.error(err);
                }
            }
        } catch (err) {
            console.error(err);
        }

        await this.upsertMediaPath(lease.path, {
            source: lease.proxy,
            record: lease.recording,
        });

        return lease;
    }

    /**
     * Fetch Path Information from Media Server
     */
    async path(pathid: string): Promise<Static<typeof PathListItem>> {
        const video = await this.settings();
        if (!video.configured) throw new Err(400, null, 'Media Integration is not configured');

        const headers = this.headers(video.token);

        const url = new URL(`/path/${pathid}`, video.internal_url);
        if (!url.port) url.port = '9997';

        const res = await fetch(url, {
            method: 'GET',
            headers: Object.fromEntries(headers.entries()),
        });

        if (res.ok) {
            return await res.typed(PathListItem);
        } else {
            throw new Err(res.status, new Error(await res.text()), 'Media Server Error');
        }
    }

    async recordings(path: string): Promise<Static<typeof Recording>> {
        const video = await this.settings();
        if (!video.configured) throw new Err(400, null, 'Media Integration is not configured');

        const headers = this.headers(video.token);

        const url = new URL(`/v3/recordings/get/${path}`, video.internal_url);
        if (!url.port) url.port = '9997';

        const res = await fetch(url, {
            method: 'GET',
            headers: Object.fromEntries(headers.entries()),
        });

        if (res.ok) {
            return await res.typed(Recording);
        } else {
            throw new Err(res.status, new Error(await res.text()), 'Media Server Error');
        }
    }

    async delete(
        leaseid: number,
        opts: {
            username?: string;
            connection?: number;
            admin: boolean;
        }
    ): Promise<void> {
        const video = await this.settings();

        if (!opts.username && !opts.connection) {
            throw new Err(400, null, 'Either connection or username config must be provided');
        } else if (opts.username && opts.connection)  {
            throw new Err(400, null, 'connection and username cannot both be provided');
        }

        if (!video.configured) throw new Err(400, null, 'Media Integration is not configured');

        const lease = await this.from(leaseid, opts);

        if (opts.connection && lease.connection !== opts.connection) {
            throw new Err(400, null, `Lease does not belong to connection ${opts.connection}`);
        } else if (opts.username && lease.username !== opts.username) {
            throw new Err(400, null, `Lease does not belong to user ${opts.username}`);
        }

        await this.config.models.VideoLease.delete(leaseid);

        await this.deleteMediaPath(lease.path);

        if (lease.publish) {
            try {
                await this.deleteTakVideoFeed(lease);
            } catch (err) {
                console.error(err);
            }
        }

        return;
    }
}
