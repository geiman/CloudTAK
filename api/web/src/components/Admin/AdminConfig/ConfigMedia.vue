<template>
    <SlideDownHeader
        v-model='isOpen'
        label='Media Server'
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
                            v-model='config["media::internal_url"]'
                            :disabled='!edit'
                            :error='validateOptionalURL(config["media::internal_url"])'
                            label='Internal Media URL'
                            description='Used by CloudTAK for service-to-service media API calls.'
                            placeholder='http://media:9997'
                        />
                    </div>
                    <div class='col-lg-12 mt-3'>
                        <TablerInput
                            v-model='config["media::public_url"]'
                            :disabled='!edit'
                            :error='validateOptionalURL(config["media::public_url"])'
                            label='Public Media URL'
                            description='Used for browser-facing playback URLs and lease metadata.'
                            placeholder='https://video.example.com'
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
import { std, stdurl } from '../../../std.ts';
import { validateURL } from '../../../base/validators.ts';
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

interface MediaConfig {
    'media::url': string;
    'media::internal_url': string;
    'media::public_url': string;
}

const isOpen = ref<boolean>(false);
const loading = ref<boolean>(false);
const edit = ref<boolean>(false);
const err = ref<Error | null>(null);

const config = ref<MediaConfig>({
    'media::url': '',
    'media::internal_url': '',
    'media::public_url': '',
});

onMounted(() => {
     if (isOpen.value) void fetch();
});

watch(isOpen, (newState) => {
    if (newState && !edit.value) void fetch();
});

function validateOptionalURL(value: string): string {
    if (!value.trim()) return '';
    return validateURL(value);
}

async function fetch(): Promise<void> {
    loading.value = true;
    err.value = null;
    try {
        const url = stdurl('/api/config');
        url.searchParams.set('keys', Object.keys(config.value).join(','));
        const res = await std(url) as Partial<MediaConfig>;
        const legacy = res['media::url'] || '';
        const internal = res['media::internal_url'] || legacy;
        const publicUrl = res['media::public_url'] || legacy || internal;
        config.value = {
            'media::url': legacy,
            'media::internal_url': internal,
            'media::public_url': publicUrl,
        };
    } catch (error) {
        err.value = error instanceof Error ? error : new Error(String(error));
    }
    loading.value = false;
}

async function save(): Promise<void> {
    loading.value = true;
    err.value = null;
    try {
        const body = {
            'media::internal_url': config.value['media::internal_url'].trim(),
            'media::public_url': config.value['media::public_url'].trim(),
        };

        await std(`/api/config`, {
            method: 'PUT',
            body
        });
        edit.value = false;
        await fetch();
    } catch (error) {
        err.value = error instanceof Error ? error : new Error(String(error));
        console.error('Failed to save Media config:', error);
    }
    loading.value = false;
}
</script>
