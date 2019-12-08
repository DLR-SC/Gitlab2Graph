# Gitlab2Graph

A Pipeline processor to extract data from Gitlab and transform it to a graph representation.

## Setup

### Install

Gitlab2Graph requires Python >= 3.6.5. We recommend a virtual environment to work with it. 

```bash
# Create virtual environment
python3.6 -m venv env

# Activate environment
source env/bin/activate

# Install dependencies
pip install -U pip
pip install -r requirements.txt
```

### Obtain private access token for Gitlab

Go to https://YOUR-GITLAB/profile/personal_access_tokens and claim a personal access token.
The necessary scopes are `api` and `read_user`.

### Configure project

Put the obtained token into a new configuration file e.g. named `project1.ini` placed under the `configurations` folder.
An example file on how to configure a project can be found [here](configurations/project_name.ini.example).

Add the connection parameters for the Gitlab and the Neo4j database, you like to use.
The project_id defines the Gitlab project the pipelines shall be executed for.   


```ini
[GITLAB]
token = deadc0ffee

[NEO4J]
hostname = localhost
;bolt or http
protocol = bolt
port = 7687
db = db/data/
user = neo4j
password = neo4jneo4j

[GENERAL]
project_id = 23

[ProjectPipeline]
my_extra_parameter = 42
```

### Usage

```
gitlab2graph.py [-h] configuration [configuration ...]

    Executes a pipelines for one or more Gitlab projects defined by
    one or more configuration files.

    positional arguments:
        configuration  Configuration file(s) in INI format
```

**Note:** Configuration files are expected to be in the configurations directory

#### Examples

```bash
# With activated environment
python gitlab2graph.py project1.ini
```

or

```bash
# With activated environment
python gitlab2graph.py project1.ini project2.ini
```

## How to cite?

If you use this work in a scientific publication, please cite the specific version that you have 
used as follows:

> Stoffers, Martin. (2019, October 13). Gitlab2Graph (Version 1.0.0). Zenodo. http://doi.org/10.5281/zenodo.3469386

You can find information about the release number and the publication date in the
[CHANGELOG](CHANGELOG.md).

## License

The script code and the accompanying material is licensed under the terms of the [MIT License](LICENSE).
