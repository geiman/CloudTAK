import type { paths } from '@cloudtak/api-types';

export type ConvertResponse = {
    asset?: string;
    icons?: Set<{
        name: string;
        data: string;
    }>;
    groundOverlays?: Array<{
        name: string;
        path: string;
        ext: string;
        mime?: string;
        coordinates: [[number, number], [number, number], [number, number], [number, number]];
        opacity?: number;
    }>;
}

export interface Transform {
    convert(): Promise<ConvertResponse>;
}

export type Message = {
    api: string
    bucket: string
    secret: string
    job: Import
};

export type LocalMessage = {
    id: string;
    tmpdir: string;
    ext: string;
    name: string;
    raw: string;
}

export type ImportList = paths["/api/import"]["get"]["responses"]["200"]["content"]["application/json"]
export type Import = paths["/api/import/{:import}"]["get"]["responses"]["200"]["content"]["application/json"]
export type Asset = paths["/api/profile/asset"]["post"]["responses"]["200"]["content"]["application/json"]
export type Basemap = paths["/api/basemap"]["post"]["responses"]["200"]["content"]["application/json"]
export type ProfileFeature = paths["/api/profile/feature"]["put"]["responses"]["200"]["content"]["application/json"]
