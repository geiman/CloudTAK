<template>
    <SlideDownHeader
        v-model='isOpen'
        label='Map Settings'
    >
        <template #right>
            <TablerIconButton
                v-if='!edit && isOpen'
                title='Edit'
                @click.stop='edit = true'
            >
                <IconPencil stroke='1' />
            </TablerIconButton>
            <div
                v-else-if='edit && isOpen'
                class='d-flex gap-1'
            >
                <TablerIconButton
                    color='rgba(var(--tblr-primary-rgb), 0.14)'
                    title='Save'
                    @click.stop='save'
                >
                    <IconDeviceFloppy
                        color='rgb(var(--tblr-primary-rgb))'
                        stroke='1'
                    />
                </TablerIconButton>
                <TablerIconButton
                    title='Cancel'
                    @click.stop='edit = false; fetch()'
                >
                    <IconX stroke='1' />
                </TablerIconButton>
            </div>
        </template>
        <div class='col-lg-12 py-2 px-2 border rounded'>
            <TablerLoading v-if='loading' />
            <template v-else>
                <TablerAlert
                    v-if='err'
                    :err='err'
                />
                <div class='row'>
                    <div class='col-lg-12'>
                        <TablerInput
                            v-model='config[`map::center`]'
                            label='Initial Map Center (<lat>,<lng>)'
                            placeholder='Latitude, Longitude'
                            :error='validateLatLng(config[`map::center`])'
                            :disabled='!edit'
                        />
                    </div>
                    <div class='col-lg-12'>
                        <TablerInput
                            v-model='config[`map::zoom`]'
                            label='Initial Map Zoom'
                            :disabled='!edit'
                        />
                    </div>
                    <div class='col-lg-12'>
                        <TablerInput
                            v-model='config[`map::pitch`]'
                            label='Initial Map Pitch'
                            :disabled='!edit'
                        />
                    </div>
                    <div class='col-lg-12'>
                        <TablerInput
                            v-model='config[`map::bearing`]'
                            label='Initial Map Bearing'
                            :disabled='!edit'
                        />
                    </div>
                    <div class='col-lg-12 mt-3'>
                        <label class='form-label'>Default Basemap</label>
                        <BasemapSelect
                            v-model='config[`map::basemap`]'
                            :disabled='!edit'
                        />
                    </div>
                    <div class='col-lg-12 mt-3'>
                        <TablerInput
                            v-model='config[`map::groundoverlay::max_size_mb`]'
                            label='GroundOverlay Max Size (MiB)'
                            description='Maximum size allowed for a single imported GroundOverlay image.'
                            :error='validatePositiveInteger(config[`map::groundoverlay::max_size_mb`])'
                            :disabled='!edit'
                        />
                    </div>
                    <div class='col-lg-12'>
                        <TablerInput
                            v-model='config[`map::groundoverlay::max_total_size_mb`]'
                            label='GroundOverlay Total Budget (MiB)'
                            description='Maximum combined GroundOverlay download budget for one imported asset.'
                            :error='validatePositiveInteger(config[`map::groundoverlay::max_total_size_mb`])'
                            :disabled='!edit'
                        />
                    </div>
                    <div class='col-lg-12'>
                        <TablerInput
                            v-model='config[`map::groundoverlay::max_count`]'
                            label='GroundOverlay Max Count'
                            description='Maximum number of GroundOverlay images allowed per imported asset.'
                            :error='validatePositiveInteger(config[`map::groundoverlay::max_count`])'
                            :disabled='!edit'
                        />
                    </div>
                </div>
            </template>
        </div>
    </SlideDownHeader>
</template>

<script setup lang="ts">
import SlideDownHeader from '../../CloudTAK/util/SlideDownHeader.vue';
import { ref, watch, onMounted } from 'vue';
import { server } from '../../../std.ts';
import { validateLatLng } from '../../../base/validators.ts';
import BasemapSelect from '../../util/BasemapSelect.vue';
import {
    TablerLoading,
    TablerInput,
    TablerIconButton,
    TablerAlert
} from '@tak-ps/vue-tabler';
import {
    IconPencil,
    IconDeviceFloppy,
    IconX
} from '@tabler/icons-vue';

const isOpen = ref<boolean>(false);
const loading = ref<boolean>(false);
const edit = ref<boolean>(false);
const err = ref<Error | null>(null);

const config = ref<{
    'map::center': string;
    'map::zoom': number;
    'map::bearing': number;
    'map::pitch': number;
    'map::basemap': number | null;
    'map::groundoverlay::max_size_mb': number;
    'map::groundoverlay::max_total_size_mb': number;
    'map::groundoverlay::max_count': number;
}>({
    'map::center': '40,-100', // Default Lat,Lng
    'map::zoom': 4,
    'map::bearing': 0,
    'map::pitch': 0,
    'map::basemap': null,
    'map::groundoverlay::max_size_mb': 500,
    'map::groundoverlay::max_total_size_mb': 1024,
    'map::groundoverlay::max_count': 10
});

onMounted(() => {
     if (isOpen.value) fetch();
});

watch(isOpen, (newState) => {
    if (newState && !edit.value) fetch();
});

function validatePositiveInteger(value: number): string {
    if (!Number.isInteger(Number(value)) || Number(value) < 1) {
        return 'Value must be a positive integer';
    }

    return '';
}

async function fetch() {
    loading.value = true;
    err.value = null;
    try {
        const res = await server.GET('/api/config', {
            params: {
                query: {
                    keys: Object.keys(config.value).join(',')
                }
            }
        });
        if (res.error) throw new Error(res.error.message);
        const data = res.data as Record<string, number | string | null | undefined>;

        config.value = {
            // DB is Lng,Lat. UI is Lat,Lng
            'map::center': typeof data['map::center'] === 'string'
                ? String(data['map::center']).split(',').reverse().join(',')
                : config.value['map::center'],
            'map::zoom': Number(data['map::zoom'] ?? config.value['map::zoom']),
            'map::bearing': Number(data['map::bearing'] ?? config.value['map::bearing']),
            'map::pitch': Number(data['map::pitch'] ?? config.value['map::pitch']),
            'map::basemap': data['map::basemap'] === null || data['map::basemap'] === undefined ? config.value['map::basemap'] : Number(data['map::basemap']),
            'map::groundoverlay::max_size_mb': Number(data['map::groundoverlay::max_size_mb'] ?? config.value['map::groundoverlay::max_size_mb']),
            'map::groundoverlay::max_total_size_mb': Number(data['map::groundoverlay::max_total_size_mb'] ?? config.value['map::groundoverlay::max_total_size_mb']),
            'map::groundoverlay::max_count': Number(data['map::groundoverlay::max_count'] ?? config.value['map::groundoverlay::max_count']),
        };
    } catch (error) {
        err.value = error instanceof Error ? error : new Error(String(error));
    }
    loading.value = false;
}

async function save() {
    loading.value = true;
    err.value = null;
    try {
        const payload = { ...config.value };
        // Save as Lng,Lat
        payload['map::center'] = payload['map::center'].split(',').reverse().join(',');
        payload['map::groundoverlay::max_size_mb'] = Number(payload['map::groundoverlay::max_size_mb']);
        payload['map::groundoverlay::max_total_size_mb'] = Number(payload['map::groundoverlay::max_total_size_mb']);
        payload['map::groundoverlay::max_count'] = Number(payload['map::groundoverlay::max_count']);

        const res = await server.PUT('/api/config', {
            body: payload as any
        });
        if (res.error) throw new Error(res.error.message);
        edit.value = false;
    } catch (error) {
        err.value = error instanceof Error ? error : new Error(String(error));
        console.error('Failed to save Map config:', error);
    }
    loading.value = false;
}
</script>
