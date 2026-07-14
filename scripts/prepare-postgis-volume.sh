#!/usr/bin/env bash

set -Eeuo pipefail

POSTGRES_DATA_DIR=/var/lib/postgresql/data
POSTGRES_USER=${POSTGRES_USER:-docker}
POSTGRES_DB=${POSTGRES_DB:-gis}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-docker}
POSTGIS_IMAGE=${POSTGIS_IMAGE:-postgis/postgis:17-3.4-alpine}
BACKUP_DIR=${CLOUDTAK_BACKUP_DIR:-$HOME/cloudtak-backups}
MIGRATION_LABEL=io.cloudtak.postgis.prepared
SOURCE_LABEL=io.cloudtak.postgis.source

log() {
    printf '[postgis-volume] %s\n' "$*"
}

fail() {
    printf '[postgis-volume] ERROR: %s\n' "$*" >&2
    exit 1
}

env_value() {
    local key=$1

    if [[ -f .env ]]; then
        sed -n "s/^${key}=//p" .env | tail -n 1
    fi
}

VOLUME_NAME=${POSTGIS_VOLUME_NAME:-$(env_value POSTGIS_VOLUME_NAME)}
VOLUME_NAME=${VOLUME_NAME:-cloudtak-postgis-data}

if [[ ! "$VOLUME_NAME" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]+$ ]]; then
    fail "Invalid POSTGIS_VOLUME_NAME: $VOLUME_NAME"
fi

command -v docker >/dev/null 2>&1 || fail 'Docker is required'
docker info >/dev/null 2>&1 || fail 'Docker is not available to the current user'

source_container=${POSTGIS_SOURCE_CONTAINER:-}
if [[ -z "$source_container" ]]; then
    source_container=$(docker compose ps --all --quiet postgis 2>/dev/null | head -n 1)
fi

temporary_container=
created_volume=false
source_initially_running=false
migration_complete=false
stopped_app_containers=()

cleanup() {
    local status=$?
    trap - EXIT INT TERM

    if [[ -n "$temporary_container" ]]; then
        docker rm --force "$temporary_container" >/dev/null 2>&1 || true
    fi

    if [[ $status -ne 0 ]]; then
        if [[ -n "$source_container" ]]; then
            if [[ "$source_initially_running" == true ]]; then
                docker start "$source_container" >/dev/null 2>&1 || true
            else
                docker stop "$source_container" >/dev/null 2>&1 || true
            fi
        fi

        for container in "${stopped_app_containers[@]}"; do
            docker start "$container" >/dev/null 2>&1 || true
        done

        if [[ "$created_volume" == true && "$migration_complete" != true ]]; then
            docker volume rm "$VOLUME_NAME" >/dev/null 2>&1 || true
        fi
    fi

    exit "$status"
}
trap cleanup EXIT INT TERM

volume_exists=false
if docker volume inspect "$VOLUME_NAME" >/dev/null 2>&1; then
    volume_exists=true
fi

if [[ -n "$source_container" ]]; then
    docker inspect "$source_container" >/dev/null 2>&1 || fail "PostGIS source container does not exist: $source_container"
    source_initially_running=$(docker inspect --format '{{.State.Running}}' "$source_container")

    current_mount=$(docker inspect --format "{{range .Mounts}}{{if eq .Destination \"$POSTGRES_DATA_DIR\"}}{{if eq .Type \"volume\"}}{{.Name}}{{else}}{{.Source}}{{end}}{{end}}{{end}}" "$source_container")
    if [[ "$current_mount" == "$VOLUME_NAME" ]]; then
        log "PostgreSQL already uses persistent volume $VOLUME_NAME"
        migration_complete=true
        exit 0
    fi
fi

if [[ "$volume_exists" == true ]]; then
    prepared=$(docker volume inspect --format "{{index .Labels \"$MIGRATION_LABEL\"}}" "$VOLUME_NAME")
    prepared_source=$(docker volume inspect --format "{{index .Labels \"$SOURCE_LABEL\"}}" "$VOLUME_NAME")

    if [[ "$prepared" == true && ( -z "$source_container" || "$prepared_source" == "$source_container" ) ]]; then
        log "Persistent volume $VOLUME_NAME is prepared and ready"
        migration_complete=true
        exit 0
    fi

    fail "Volume $VOLUME_NAME already exists but was not prepared by this migration. Refusing to overwrite it."
fi

if [[ -z "$source_container" ]]; then
    docker volume create \
        --label "$MIGRATION_LABEL=true" \
        --label "$SOURCE_LABEL=new-installation" \
        "$VOLUME_NAME" >/dev/null
    created_volume=true
    migration_complete=true
    log "Created persistent PostgreSQL volume $VOLUME_NAME for a new installation"
    exit 0
fi

for service in api events retention tiles; do
    container=$(docker compose ps --quiet "$service" 2>/dev/null | head -n 1)
    if [[ -n "$container" ]]; then
        stopped_app_containers+=("$container")
        log "Stopping $service to prevent database writes during migration"
        docker stop "$container" >/dev/null
    fi
done

if [[ $(docker inspect --format '{{.State.Running}}' "$source_container") != true ]]; then
    log 'Starting the existing PostgreSQL container for backup'
    docker start "$source_container" >/dev/null
fi

mkdir -p "$BACKUP_DIR"
backup_file="$BACKUP_DIR/cloudtak-volume-migration-$(date -u +%Y%m%dT%H%M%SZ).dump"

log "Backing up the existing database to $backup_file"
docker exec "$source_container" pg_dump \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --format custom > "$backup_file"

if [[ ! -s "$backup_file" ]]; then
    fail "PostgreSQL backup is empty: $backup_file"
fi

source_table_count=$(docker exec "$source_container" psql \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --tuples-only \
    --no-align \
    --command "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public';")

docker volume create \
    --label "$MIGRATION_LABEL=true" \
    --label "$SOURCE_LABEL=$source_container" \
    "$VOLUME_NAME" >/dev/null
created_volume=true

temporary_container="cloudtak-postgis-migration-$$"
log "Restoring the backup into persistent volume $VOLUME_NAME"
docker run --detach \
    --name "$temporary_container" \
    --env "POSTGRES_DB=$POSTGRES_DB" \
    --env "POSTGRES_USER=$POSTGRES_USER" \
    --env "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" \
    --volume "$VOLUME_NAME:$POSTGRES_DATA_DIR" \
    "$POSTGIS_IMAGE" >/dev/null

ready=false
for _ in $(seq 1 60); do
    if docker exec "$temporary_container" pg_isready --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 1
done

if [[ "$ready" != true ]]; then
    fail 'Timed out waiting for the migration PostgreSQL container'
fi

docker exec --interactive "$temporary_container" pg_restore \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --clean \
    --if-exists \
    --exit-on-error \
    --no-owner \
    --no-privileges < "$backup_file"

target_table_count=$(docker exec "$temporary_container" psql \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --tuples-only \
    --no-align \
    --command "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public';")

if [[ "$source_table_count" != "$target_table_count" ]]; then
    fail "Verification failed: source has $source_table_count public tables, restored volume has $target_table_count"
fi

docker stop "$source_container" >/dev/null
migration_complete=true
log "Verified $target_table_count public tables in the restored database"
log "Migration complete. The original container is stopped and the backup remains available."
log "Run 'docker compose up -d postgis' (or './cloudtak.sh start') to attach the persistent volume."
