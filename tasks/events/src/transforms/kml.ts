import fs from 'node:fs/promises';
import type { Message, LocalMessage, Transform, ConvertResponse } from '../types.ts';
import path from 'node:path';
import Sharp from 'sharp';
import { glob } from 'glob';
import StreamZip from 'node-stream-zip';
import { kml } from '@tmcw/togeojson';
import { DOMParser, type Document, type Element } from '@xmldom/xmldom';
import { isSafeUrl } from '../safeurl.ts';
import { fetch } from 'undici';

const MAX_NETWORK_LINK_DEPTH = 3;
const NETWORK_LINK_FETCH_TIMEOUT_MS = 10_000;

type GeoJSONFeatures = ReturnType<typeof kml>['features'];

type GroundOverlayArtifact = {
    name: string;
    path: string;
    ext: string;
    mime?: string;
    coordinates: [[number, number], [number, number], [number, number], [number, number]];
    opacity?: number;
};

type KMLDocumentContents = {
    features: GeoJSONFeatures;
    groundOverlays: GroundOverlayArtifact[];
};

export default class KML implements Transform {
    static register() {
        return {
            inputs: ['.kml', '.kmz']
        };
    }

    msg: Message;
    local: LocalMessage;

    constructor(
        msg: Message,
        local: LocalMessage
    ) {
        this.msg = msg;
        this.local = local;
    }

    nodeText(parent: Element, tagName: string): string | undefined {
        const value = parent.getElementsByTagName(tagName)[0]?.textContent;
        return value === undefined || value === null ? undefined : value.trim();
    }

    parseOverlayOpacity(color?: string): number | undefined {
        if (!color) return undefined;

        const normalized = color.trim();
        if (!/^[0-9a-fA-F]{8}$/.test(normalized)) return undefined;

        return parseInt(normalized.slice(0, 2), 16) / 255;
    }

    rotateCoordinate(
        lon: number,
        lat: number,
        centerLon: number,
        centerLat: number,
        angleDegrees: number
    ): [number, number] {
        const angle = angleDegrees * (Math.PI / 180);
        const cos = Math.cos(angle);
        const sin = Math.sin(angle);
        const dx = lon - centerLon;
        const dy = lat - centerLat;

        return [
            centerLon + (dx * cos) - (dy * sin),
            centerLat + (dx * sin) + (dy * cos)
        ];
    }

    latLonBoxToCoordinates(overlay: Element): [[number, number], [number, number], [number, number], [number, number]] | undefined {
        const box = overlay.getElementsByTagName('LatLonBox')[0];
        if (!box) return undefined;

        const north = Number(this.nodeText(box, 'north'));
        const south = Number(this.nodeText(box, 'south'));
        const east = Number(this.nodeText(box, 'east'));
        const west = Number(this.nodeText(box, 'west'));

        if ([north, south, east, west].some((value) => Number.isNaN(value))) return undefined;

        const rotation = Number(this.nodeText(box, 'rotation') || '0');

        const topLeft: [number, number] = [west, north];
        const topRight: [number, number] = [east, north];
        const bottomRight: [number, number] = [east, south];
        const bottomLeft: [number, number] = [west, south];

        if (!rotation) {
            return [topLeft, topRight, bottomRight, bottomLeft];
        }

        const centerLon = (west + east) / 2;
        const centerLat = (north + south) / 2;

        return [
            this.rotateCoordinate(...topLeft, centerLon, centerLat, -rotation),
            this.rotateCoordinate(...topRight, centerLon, centerLat, -rotation),
            this.rotateCoordinate(...bottomRight, centerLon, centerLat, -rotation),
            this.rotateCoordinate(...bottomLeft, centerLon, centerLat, -rotation)
        ];
    }

    latLonQuadToCoordinates(overlay: Element): [[number, number], [number, number], [number, number], [number, number]] | undefined {
        const quads = overlay.getElementsByTagName('gx:LatLonQuad');
        const quad = quads[0];
        if (!quad) return undefined;
        const coordinates = this.nodeText(quad, 'coordinates');

        if (!coordinates) return undefined;

        const parsed = coordinates
            .trim()
            .split(/\s+/)
            .map((coord) => coord.split(',').map(Number))
            .filter((coord) => coord.length >= 2 && !coord.slice(0, 2).some((value) => Number.isNaN(value)))
            .map((coord) => [coord[0], coord[1]] as [number, number]);

        if (parsed.length !== 4) return undefined;

        const byLat = [...parsed].sort((a, b) => b[1] - a[1]);
        const top = byLat.slice(0, 2).sort((a, b) => a[0] - b[0]);
        const bottom = byLat.slice(2).sort((a, b) => b[0] - a[0]);

        return [top[0], top[1], bottom[0], bottom[1]];
    }

    async materializeHref(
        href: string,
        localDir: string | null,
        baseUrl: string | null,
        prefix: string
    ): Promise<{ path: string; ext: string; mime?: string } | undefined> {
        if (href.startsWith('data:')) {
            const match = /^data:([^;,]+)?(?:;base64)?,(.*)$/i.exec(href);
            if (!match) return undefined;

            const mime = (match[1] || '').toLowerCase();
            const ext = mime.includes('jpeg') ? '.jpg'
                : mime.includes('webp') ? '.webp'
                : mime.includes('gif') ? '.gif'
                : '.png';
            const filepath = path.join(this.local.tmpdir, `${prefix}${ext}`);
            const body = match[2] || '';
            const buffer = href.includes(';base64,')
                ? Buffer.from(body, 'base64')
                : Buffer.from(decodeURIComponent(body), 'utf8');

            await fs.writeFile(filepath, buffer);
            return { path: filepath, ext, mime: mime || undefined };
        }

        if (!href.startsWith('http://') && !href.startsWith('https://')) {
            if (!localDir) return undefined;

            const resolved = path.resolve(localDir, href);
            const tmpdirSafe = path.resolve(this.local.tmpdir);

            if (resolved !== tmpdirSafe && !resolved.startsWith(tmpdirSafe + path.sep)) {
                console.warn(`GroundOverlay ${href} would escape data directory, skipping`);
                return undefined;
            }

            const ext = path.extname(resolved).toLowerCase() || '.png';
            return { path: resolved, ext };
        }

        let resolvedHref = href;
        if (baseUrl && !href.startsWith('http://') && !href.startsWith('https://')) {
            resolvedHref = new URL(href, baseUrl).toString();
        }

        const { safe, url, reason } = await isSafeUrl(resolvedHref);
        if (!safe || !url) {
            console.warn(`GroundOverlay ${href} skipped — ${reason}`);
            return undefined;
        }

        const res = await fetch(url, {
            signal: AbortSignal.timeout(NETWORK_LINK_FETCH_TIMEOUT_MS)
        });

        if (!res.ok) {
            throw new Error(`HTTP ${res.status} ${await res.text()}`);
        }

        const pathnameExt = path.extname(url.pathname).toLowerCase();
        const contentType = res.headers.get('content-type')?.toLowerCase() || '';
        const ext = pathnameExt
            || (contentType.includes('jpeg') ? '.jpg'
                : contentType.includes('webp') ? '.webp'
                : contentType.includes('gif') ? '.gif'
                : '.png');

        const filepath = path.join(this.local.tmpdir, `${prefix}${ext}`);
        await fs.writeFile(filepath, Buffer.from(await res.arrayBuffer()));

        return { path: filepath, ext, mime: contentType || undefined };
    }

    async extractGroundOverlays(
        dom: Document,
        baseUrl: string | null = null,
        localDir: string | null = null
    ): Promise<GroundOverlayArtifact[]> {
        const overlays = Array.from(dom.getElementsByTagName('GroundOverlay')) as Element[];
        const results: GroundOverlayArtifact[] = [];

        for (const [index, overlay] of overlays.entries()) {
            const href = this.nodeText(overlay, 'href');
            if (!href) continue;

            const coordinates = this.latLonQuadToCoordinates(overlay) || this.latLonBoxToCoordinates(overlay);
            if (!coordinates) {
                console.warn(`GroundOverlay ${href} is missing valid bounds, skipping`);
                continue;
            }

            const materialized = await this.materializeHref(
                href,
                localDir,
                baseUrl,
                `groundoverlay-${Date.now()}-${index}`
            );

            if (!materialized) {
                console.warn(`GroundOverlay ${href} could not be materialized, skipping`);
                continue;
            }

            results.push({
                name: this.nodeText(overlay, 'name') || path.parse(href).name || `Ground Overlay ${index + 1}`,
                path: materialized.path,
                ext: materialized.ext,
                mime: materialized.mime,
                coordinates,
                opacity: this.parseOverlayOpacity(this.nodeText(overlay, 'color'))
            });
        }

        return results;
    }

    async fetchDocument(
        kmlContent: string,
        icons: Map<string, Buffer>,
        depth: number,
        baseUrl: string | null = null,
        localDir: string | null = null,
        visited: Set<string> = new Set()
    ): Promise<KMLDocumentContents> {
        const dom = new DOMParser().parseFromString(kmlContent, 'text/xml');
        const allFeatures = kml(dom).features;
        const groundOverlays = await this.extractGroundOverlays(dom, baseUrl, localDir);

        const features: GeoJSONFeatures = [];

        for (const feat of allFeatures) {
            if (!feat.properties) feat.properties = {};

            if (feat.properties['@geometry-type'] === 'networklink') {
                if (depth >= MAX_NETWORK_LINK_DEPTH) {
                    console.warn(`NetworkLink skipped — max depth of ${MAX_NETWORK_LINK_DEPTH} reached`);
                    continue;
                }

                let href = feat.properties.href as string | undefined;

                if (!href) {
                    console.warn('NetworkLink has no href, skipping');
                    continue;
                }

                // Reject any explicit URI scheme other than http / https before local-path handling
                if (href.includes('://') && !href.startsWith('http://') && !href.startsWith('https://')) {
                    console.warn(`NetworkLink ${href} skipped — unsupported protocol`);
                    continue;
                }

                if (!href.startsWith('http://') && !href.startsWith('https://')) {
                    if (localDir) {
                        // Local relative resolution — path must stay within tmpdir
                        const resolved = path.resolve(localDir, href);
                        const tmpdirSafe = path.resolve(this.local.tmpdir);

                        if (resolved !== tmpdirSafe && !resolved.startsWith(tmpdirSafe + path.sep)) {
                            console.warn(`NetworkLink ${href} would escape data directory, skipping`);
                            continue;
                        }

                        if (visited.has(resolved)) {
                            console.warn(`NetworkLink ${resolved} already visited, skipping`);
                            continue;
                        }
                        visited.add(resolved);

                        try {
                            const localContent = await fs.readFile(resolved, 'utf8');
                            const linked = await this.fetchDocument(
                                localContent, icons, depth + 1, null, path.dirname(resolved), visited
                            );
                            features.push(...linked.features);
                            groundOverlays.push(...linked.groundOverlays);
                        } catch (err) {
                            console.warn(`NetworkLink local file ${href} not readable (${err})`);
                        }

                        continue;
                    } else if (baseUrl) {
                        // HTTP relative resolution — resolved URL must stay on the same origin
                        let resolved: URL;
                        try {
                            resolved = new URL(href, baseUrl);
                        } catch {
                            console.warn(`NetworkLink href ${href} could not be resolved relative to ${baseUrl}, skipping`);
                            continue;
                        }

                        const base = new URL(baseUrl);
                        if (resolved.origin !== base.origin) {
                            console.warn(`NetworkLink ${href} resolved to a different origin (${resolved.origin}), skipping`);
                            continue;
                        }

                        href = resolved.toString();
                        // fall through to SSRF check and fetch below
                    } else {
                        console.warn(`NetworkLink ${href} is relative but no base URL or local directory is available, skipping`);
                        continue;
                    }
                }

                const { safe, url, reason } = await isSafeUrl(href);
                if (!safe || !url) {
                    console.warn(`NetworkLink ${href} skipped — ${reason}`);
                    continue;
                }

                // Normalise the URL for deduplication (strip trailing slash, lowercase host)
                url.hostname = url.hostname.toLowerCase();
                if (url.pathname.length > 1 && url.pathname.endsWith('/')) {
                    url.pathname = url.pathname.replace(/\/+$/, '');
                }
                const normalized = url.toString();
                if (visited.has(normalized)) {
                    console.warn(`NetworkLink ${normalized} already visited, skipping`);
                    continue;
                }
                visited.add(normalized);

                try {
                    const res = await fetch(normalized, {
                        signal: AbortSignal.timeout(NETWORK_LINK_FETCH_TIMEOUT_MS)
                    });

                    if (!res.ok) throw new Error(`HTTP ${res.status} ${await res.text()}`);

                    const buf = Buffer.from(await res.arrayBuffer());
                    // Detect KMZ by ZIP magic bytes (PK\x03\x04) since content-type is unreliable
                    const isKmz = buf.length >= 4
                        && buf[0] === 0x50 && buf[1] === 0x4B
                        && buf[2] === 0x03 && buf[3] === 0x04;

                    let linked: KMLDocumentContents;

                    if (isKmz) {
                        const tmpKmzPath = path.join(this.local.tmpdir, `nl-${Date.now()}.kmz`);
                        const extractDir = tmpKmzPath.replace(/\.kmz$/, '');
                        await fs.writeFile(tmpKmzPath, buf);
                        await fs.mkdir(extractDir, { recursive: true });
                        const zip = new StreamZip.async({ file: tmpKmzPath });
                        try {
                            const entries = await zip.entries();
                            let kmlFileName = 'doc.kml';
                            
                            if (!entries['doc.kml']) {
                                // Look for alternative KML files if doc.kml doesn't exist
                                const kmlFiles = Object.keys(entries).filter(name => name.toLowerCase().endsWith('.kml'));
                                
                                if (kmlFiles.length === 0) {
                                    console.warn(`NetworkLink ${normalized} KMZ has no KML files, skipping`);
                                    continue;
                                } else if (kmlFiles.length > 1) {
                                    console.warn(`NetworkLink ${normalized} KMZ has multiple KML files but no doc.kml, skipping`);
                                    continue;
                                } else {
                                    kmlFileName = kmlFiles[0];
                                    console.log(`NetworkLink ${normalized} KMZ using ${kmlFileName} instead of doc.kml`);
                                }
                            }
                            
                            const kmlFileResolved = path.resolve(extractDir, kmlFileName);
                            const extractDirResolved = path.resolve(extractDir);
                            if (kmlFileResolved !== extractDirResolved && !kmlFileResolved.startsWith(extractDirResolved + path.sep)) {
                                console.warn(`NetworkLink ${normalized} KMZ path traversal attempt detected (${kmlFileName}), skipping`);
                                continue;
                            }
                            
                            // Extract everything so icon assets bundled in the linked KMZ are
                            // available on disk for the glob-based icon resolver.
                            await zip.extract(null, extractDir);
                            const kmlContent = await fs.readFile(kmlFileResolved, 'utf8');
                            // Use the directory containing the extracted KML as localDir so
                            // relative paths (icon refs, nested NetworkLinks) resolve correctly.
                            const kmlDir = path.dirname(kmlFileResolved);
                            linked = await this.fetchDocument(kmlContent, icons, depth + 1, normalized, kmlDir, visited);
                        } finally {
                            await zip.close();
                            await fs.unlink(tmpKmzPath).catch(() => { /* ignore */ });
                        }
                    } else {
                        linked = await this.fetchDocument(buf.toString('utf8'), icons, depth + 1, normalized, null, visited);
                    }

                    features.push(...linked.features);
                    groundOverlays.push(...linked.groundOverlays);
                } catch (err) {
                    console.warn(`NetworkLink ${normalized} not retrievable (${err})`);
                }

                continue;
            }

            if (feat.properties['@geometry-type'] === 'groundoverlay') {
                continue;
            }

            if (feat.properties.icon && !icons.has(feat.properties.icon as string)) {
                if ((feat.properties.icon as string).startsWith('http')) {
                    try {
                        const res = await fetch(feat.properties.icon as string);

                        if (!res.ok) {
                            throw new Error(`HTTP ${res.status} ${await res.text()}`);
                        }

                        const iconbuffer = Buffer.from(await res.arrayBuffer());

                        icons.set(feat.properties.icon as string, iconbuffer);
                    } catch (err) {
                        console.warn(`icon ${feat.properties.icon} not retrievable (${err})`);
                    }
                } else {
                    const search = await glob(path.resolve(this.local.tmpdir, '**/' + feat.properties.icon));
                    if (!search.length) {
                        console.warn(`icon ${feat.properties.icon} not found`);
                        continue;
                    }

                    icons.set(feat.properties.icon as string, await fs.readFile(search[0]));
                }
            }

            features.push(feat);
        }

        return {
            features,
            groundOverlays
        };
    }

    async convert(): Promise<ConvertResponse> {
        const icons = new Map<string, Buffer>();

        let asset;

        if (this.local.ext === '.kmz') {
            const zip = new StreamZip.async({
                file: this.local.raw,
                skipEntryNameValidation: true
            });

            try {
                const preentries = await zip.entries();
                let kmlFileName = 'doc.kml';

                if (!preentries['doc.kml']) {
                    // Look for alternative KML files if doc.kml doesn't exist
                    const kmlFiles = Object.keys(preentries).filter(name => name.toLowerCase().endsWith('.kml'));
                    
                    if (kmlFiles.length === 0) {
                        throw new Error('No KML files found in KMZ');
                    } else if (kmlFiles.length > 1) {
                        throw new Error('Multiple KML files found in KMZ but no doc.kml - ambiguous which file to use');
                    } else {
                        kmlFileName = kmlFiles[0];
                        console.log(`Using ${kmlFileName} instead of doc.kml in KMZ`);
                    }
                }

                const kmlFileResolved = path.resolve(this.local.tmpdir, kmlFileName);
                const tmpdirResolved = path.resolve(this.local.tmpdir);
                if (kmlFileResolved !== tmpdirResolved && !kmlFileResolved.startsWith(tmpdirResolved + path.sep)) {
                    throw new Error(`Path traversal attempt detected in KMZ: ${kmlFileName}`);
                }

                await zip.extract(null, this.local.tmpdir);

                asset = kmlFileResolved;
            } finally {
                await zip.close();
            }
        } else {
            asset = path.resolve(this.local.raw);
        }

        const document = await this.fetchDocument(String(await fs.readFile(asset)), icons, 0, null, path.dirname(asset));
        let output: string | undefined;

        if (document.features.length) {
            console.error('ok - converted to GeoJSON');

            output = path.resolve(this.local.tmpdir, this.local.id + '.geojsonld');

            await fs.writeFile(output, document.features.map((feat: GeoJSONFeatures[number]) => {
                return JSON.stringify(feat);
            }).join('\n'));
        }

        const iconMap = new Set<{
            name: string;
            data: string;
        }>();

        for (const [name, icon] of icons.entries()) {
            try {
                const contents = await (Sharp(icon)
                    .png()
                    .toBuffer());

                iconMap.add({
                    name: name.replace(/.[a-z]+$/, '.png'),
                    data: `data:image/png;base64,${contents.toString('base64')}`
                });
            } catch (err) {
                console.error(`failing to process ${name}`, err);
            }
        }

        return {
            asset: output,
            icons: iconMap.size ? iconMap : undefined,
            groundOverlays: document.groundOverlays.length ? document.groundOverlays : undefined
        };
    }
}
