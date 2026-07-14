<p align=center><img src='./api/web/public/CloudTAKLogo.svg' alt='CloudTAK Logo' width='128'/></p>

<h1 align=center>CloudTAK</h1>

<p align=center>Full Featured in-browser TAK Client</p>
<p align=center>&</p>
<p align=center>Facilitate ETL operations to bring non-TAK data sources into a TAK Server</p>

<p align='center'>
    <a href="https://codecov.io/gh/dfpc-coe/CloudTAK" >
        <img src="https://codecov.io/github/dfpc-coe/CloudTAK/graph/badge.svg?token=O9PK0XT9Z2"/>
    </a>
</p>

<img src='./docs/Screenshot.png' alt='Screenshot of CloudTAK'/>

## Documentation

Deployment, local development, and administration guidance now live in the CloudTAK documentation site so the repo root is not a second source of truth.

- Deployment: https://docs.cloudtak.io/deploy/
- Local development: https://docs.cloudtak.io/develop/
- Administration: https://docs.cloudtak.io/admin/

> [!NOTE]
> Local development and Docker Compose expose the core map experience, but a full AWS deployment is still required for the complete optional ETL infrastructure.

> [!IMPORTANT]
> Before starting Docker Compose for the first time after this persistence change,
> run `./cloudtak.sh database-volume`. On an existing installation, this command
> stops database writers, backs up the current PostGIS container, restores and
> verifies the database in the persistent volume, and retains the backup. On a
> new installation, it creates the empty persistent volume. Compose intentionally
> refuses to create an unprepared database volume.

