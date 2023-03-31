# target-mssql

`target-mssql` is a Singer target for Microsoft SQL Server databases.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

## Known limitations

- Objects and arrays are converted to strings, as writing json/arrays isn't supported in the underlying library that is used.
- Does not handle encoded strings

## Installation

Install from Meltano:
```bash
meltano add loader target-mssql
```

Install from PyPi:

```bash
pipx install target-mssql
```

Install from GitHub:

```bash
pipx install git+https://github.com/storebrand/target-mssql.git@main
```

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install target-mssql
```

Install from GitHub:

```bash
pipx install git+https://github.com/storebrand/target-mssql.git@main
```

-->

## Configuration

## Accepted Config Options
Regarding connection info, either the `sqlalchemy_url` or `username`, `password`, `host`, and `database` needs to be specified. If the `sqlalchemy_url` is set, the other connection parameters are ignored.

## Capabilities

* `about`
* `stream-maps`
* `schema-flattening`

## Settings

| Setting              | Required | Default | Description |
|:---------------------|:--------:|:-------:|:------------|
| sqlalchemy_url       | False    | None    | SQLAlchemy connection string |
| username             | False    | None    | SQL Server username |
| password             | False    | None    | SQL Server password |
| host                 | False    | None    | SQL Server host |
| port                 | False    | 1433    | SQL Server port |
| database             | False    | None    | SQL Server database |
| default_target_schema| False    | None    | Default target schema to write to |
| stream_maps          | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config    | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled   | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `target-mssql --about`

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-mssql --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `target-mssql` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-mssql --version
target-mssql --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-mssql --config /path/to/target-mssql-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_mssql/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-mssql` CLI interface directly using `poetry run`:

```bash
poetry run target-mssql --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-mssql
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-mssql --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano elt tap-carbon-intensity target-mssql
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano SDK to
develop your own Singer taps and targets.
