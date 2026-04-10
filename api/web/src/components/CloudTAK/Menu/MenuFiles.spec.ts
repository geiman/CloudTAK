import { mount, flushPromises } from '@vue/test-utils';
import { describe, expect, it, vi, beforeEach } from 'vitest';

const {
    pushMock,
    addOverlayMock,
    getOverlayBeforeIdMock,
    overlayCreateMock,
    stdMock,
    stdurlMock,
    serverGetMock
} = vi.hoisted(() => ({
    pushMock: vi.fn(),
    addOverlayMock: vi.fn(),
    getOverlayBeforeIdMock: vi.fn(() => 'before-overlay'),
    overlayCreateMock: vi.fn(async (overlay: Record<string, unknown>) => overlay),
    stdMock: vi.fn(),
    stdurlMock: vi.fn((url: string | URL) => new URL(String(url), 'http://localhost:8080')),
    serverGetMock: vi.fn()
}));

vi.mock('vue-router', () => ({
    useRouter: () => ({
        push: pushMock
    })
}));

vi.mock('../../../stores/map.ts', () => ({
    useMapStore: () => ({
        overlays: [],
        addOverlay: addOverlayMock,
        getOverlayBeforeId: getOverlayBeforeIdMock
    })
}));

vi.mock('../../../base/overlay.ts', () => ({
    default: {
        create: overlayCreateMock
    }
}));

vi.mock('../../../std.ts', () => ({
    std: stdMock,
    stdurl: stdurlMock,
    server: {
        GET: serverGetMock,
        PATCH: vi.fn(),
        DELETE: vi.fn()
    }
}));

import MenuFiles from './MenuFiles.vue';

describe('MenuFiles', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        serverGetMock.mockResolvedValue({
            data: {
                total: 1,
                tiles: { url: 'http://localhost:5001/tiles/profile/admin@example.com/' },
                items: [{
                    id: 'groundoverlay-asset',
                    name: '26PGA_85.kmz',
                    path: '/',
                    size: 12345,
                    updated: '2026-04-10T15:00:00.000Z',
                    iconset: null,
                    artifacts: [
                        { ext: '.groundoverlays.json', size: 321 }
                    ]
                }]
            }
        });

        stdMock.mockResolvedValue({
            overlays: [{
                name: '26PGA_85',
                ext: '.groundoverlay-0.png',
                opacity: 0.75,
                coordinates: [
                    [-75, 40],
                    [-74, 40],
                    [-74, 39],
                    [-75, 39]
                ]
            }]
        });
    });

    it('creates image overlays from the GroundOverlay manifest endpoint', async () => {
        const wrapper = mount(MenuFiles, {
            global: {
                directives: {
                    tooltip: () => {}
                },
                stubs: {
                    MenuTemplate: {
                        template: '<div><slot name="buttons" /><slot /></div>'
                    },
                    TablerSlidedown: {
                        template: '<div><slot /><slot name="expanded" /></div>'
                    },
                    TablerIconButton: true,
                    TablerRefreshButton: true,
                    TablerInput: true,
                    TablerPager: true,
                    TablerAlert: true,
                    TablerNone: true,
                    TablerLoading: true,
                    TablerBytes: true,
                    TablerEpoch: true,
                    TablerDelete: {
                        template: '<div />'
                    },
                    ShareToPackage: true,
                    ShareToMission: true,
                    Upload: true,
                    IconAmbulance: true,
                    IconPackage: true,
                    IconUpload: true,
                    IconMapOff: true,
                    IconMapPlus: true,
                    IconDownload: true,
                    IconCursorText: true
                }
            }
        });

        await flushPromises();

        const addOverlayButton = wrapper.findAll('[role="menuitem"]').find((node) => {
            return node.text().includes('Add to Map as Overlay');
        });

        expect(addOverlayButton).toBeTruthy();

        await addOverlayButton!.trigger('click');
        await flushPromises();

        expect(stdMock).toHaveBeenCalledWith('/api/profile/asset/groundoverlay-asset/groundoverlays');
        expect(stdurlMock).toHaveBeenCalledWith('/api/profile/asset/groundoverlay-asset/groundoverlay/0');
        expect(overlayCreateMock).toHaveBeenCalledWith(expect.objectContaining({
            url: 'http://localhost:8080/api/profile/asset/groundoverlay-asset/groundoverlay/0',
            mode: 'profile',
            mode_id: 'groundoverlay-asset',
            type: 'image',
            opacity: 0.75,
            coordinates: [
                [-75, 40],
                [-74, 40],
                [-74, 39],
                [-75, 39]
            ]
        }), {
            before: 'before-overlay'
        });
        expect(addOverlayMock).toHaveBeenCalledTimes(1);
        expect(pushMock).toHaveBeenCalledWith('/menu/overlays');
        expect(stdurlMock).not.toHaveBeenCalledWith(expect.stringContaining('.groundoverlay-0.png'));
    });
});
