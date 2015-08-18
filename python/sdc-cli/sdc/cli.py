import api

import click
import ConfigParser
import json
import os
import requests

APP_NAME = 'sdc-cli'
DEFAULT_CFG = os.path.join(click.get_app_dir(APP_NAME), 'config.ini')
SDC_CFG_SECTION = 'sdc'

pass_api = click.make_pass_decorator(api.DataCollector)


@click.group()
@click.option(
    '--sdc-url',
    default=None,
    envvar='SDC_URL',
    help='Base URL of SDC REST endpoint.',
)
@click.option(
    '--sdc-user',
    default=None,
    envvar='SDC_USER',
    help='Username for logging into SDC',
)
@click.option(
    '--sdc-password',
    default=None,
    envvar='SDC_PASSWORD',
    help='Password for logging into SDC',
)
@click.option(
    '--config-file',
    default=None,
    envvar='SDC_CLI_CFG',
    help='Default location: %s' % os.path.join(click.get_app_dir(APP_NAME), 'config.ini'),
)
@click.option(
    '--auth-type',
    type=click.Choice(['none', 'basic', 'digest', 'form']),
    default=None,
    envvar='SDC_AUTH_TYPE',
    help='Type of authentication configured in the target SDC.',
)
@click.pass_context
def cli(ctx, sdc_url, sdc_user, sdc_password, config_file, auth_type):
    """sdc-cli is the command line interface for controlling StreamSets Data Collector via its REST API."""
    _create_default_config(config_file)

    cfg_values = {
        'url': sdc_url,
        'user': sdc_user,
        'password': sdc_password,
        'auth_type': auth_type,
    }
    cfg_values = _read_config(cfg_values, config_file)
    ctx.obj = api.DataCollector(cfg_values['url'], cfg_values['user'], cfg_values['password'], cfg_values['auth_type'])


@cli.command('ping')
@pass_api
def ping(sdc_api):
    """Issues a ping to the server and returns whether it is alive or not."""
    try:
        click.echo(sdc_api.ping().text)
    except requests.ConnectionError:
        click.echo('SDC was unreachable.')


@cli.group('library')
def library():
    """Commands for pipeline library resource."""


@library.command('list')
@click.option(
    '--full',
    is_flag=True,
    help='Output the full pipeline description, not just the name.'
)
@click.option('--pretty', is_flag=True, help='Pretty prints JSON when specified.')
@pass_api
def list_pipelines(sdc_api, full, pretty):
    """Lists the pipelines stored in the pipeline library."""
    if full:
        _print_json(sdc_api.list_pipelines(), pretty)
    else:
        _print_json(sdc_api.list_pipeline_names(), pretty)


@library.command('import')
@click.argument('json_file', type=click.File('rb'))
@click.option('--pretty', is_flag=True, help='Pretty prints JSON when specified.')
@pass_api
def import_pipeline(sdc_api, json_file, pretty):
    """Imports a new pipeline or updates an existing pipeline specified from a JSON file."""
    p = json.loads(json_file.read())
    imported = sdc_api.import_pipeline(p)
    _print_json(imported, pretty)


@library.command('delete')
@click.argument('pipeline_name')
@pass_api
def delete_pipeline(sdc_api, pipeline_name):
    """Deletes the specified pipeline by name."""
    r = sdc_api.delete_pipeline(pipeline_name)
    if r.status_code == requests.codes.ok:
        click.echo('Deleted [%s] successfully.' % pipeline_name)
    else:
        click.echo('Failed to delete [%s].' % pipeline_name)


@cli.group('pipeline')
def pipeline():
    """Commands for pipeline resource."""


@pipeline.command('show')
@click.argument('pipeline_name')
@click.option('--pretty', is_flag=True, help='Pretty prints JSON when specified.')
@pass_api
def show_pipeline(sdc_api, pipeline_name, pretty):
    """Displays the pipeline config for the pipeline name provided."""
    _print_json(sdc_api.get_pipeline(pipeline_name), pretty)


@pipeline.command('start')
@click.argument('pipeline_name')
@click.option('--rev', default=None)
@pass_api
def start_pipeline(sdc_api, pipeline_name, rev):
    """Start the specified pipeline."""
    sdc_api.start_pipeline(pipeline_name, rev)
    click.echo('Starting [%s]...' % pipeline_name)


@pipeline.command('stop')
@pass_api
def stop_pipeline(sdc_api):
    """Stop the currently running pipeline."""
    sdc_api.stop_pipeline()
    click.echo('Stopping...')


@pipeline.command('reset-origin')
@click.argument('pipeline_name')
@pass_api
def reset_origin(sdc_api, pipeline_name):
    """Reset the origin of the specified pipeline."""
    click.echo(sdc_api.reset_offset(pipeline_name))


@pipeline.command('status')
@click.option('--pretty', is_flag=True, help='Pretty prints JSON when specified.')
@pass_api
def pipeline_status(sdc_api, pretty):
    """Displays the status of the currently active pipeline."""
    _print_json(sdc_api.pipeline_status(), pretty)


@pipeline.command('metrics')
@click.option('--pretty', is_flag=True, help='Pretty prints JSON when specified.')
@pass_api
def pipeline_metrics(sdc_api, pretty):
    _print_json(sdc_api.pipeline_metrics(), pretty)


@pipeline.group('rules')
def rules():
    """View and Modify pipeline rules."""


@rules.command('show')
@click.argument('pipeline_name')
@click.option('--pretty', is_flag=True, help='Pretty prints JSON when specified.')
@pass_api
def show_rules(sdc_api, pipeline_name, pretty):
    _print_json(sdc_api.get_rules(pipeline_name), pretty)


def _read_config(cfg_values=None, cfg=DEFAULT_CFG):
    parser = ConfigParser.SafeConfigParser()
    parser.read([cfg])
    if cfg_values is None:
        cfg_values = {}
    for key, value in parser.items(SDC_CFG_SECTION):
        if cfg_values[key] is None:
            cfg_values[key] = value
    return cfg_values


def _write_default_config(cfg=DEFAULT_CFG):
    with open(cfg, 'w') as cfg_file:
        parser = ConfigParser.SafeConfigParser()
        parser.add_section(SDC_CFG_SECTION)
        parser.set(SDC_CFG_SECTION, '# url', 'http://localhost:18630')
        parser.set(SDC_CFG_SECTION, '# user', 'admin')
        parser.set(SDC_CFG_SECTION, '# password', 'admin')
        parser.set(SDC_CFG_SECTION, '# auth_type', 'form')
        parser.write(cfg_file)


def _create_default_config(config_path=None):
    if config_path is None:
        cfg_dir = os.path.dirname(DEFAULT_CFG)
        config_path = DEFAULT_CFG
    else:
        cfg_dir = os.path.dirname(config_path)
        
    if not os.path.exists(cfg_dir):
        os.makedirs(cfg_dir)
    if not os.path.exists(config_path):
        _write_default_config(config_path)

def _print_json(d, pretty=False):
    """Print dict as JSON, optionally in pretty print mode."""
    if pretty:
        click.echo(
            json.dumps(
                d,
                sort_keys=True,
                indent=2,
                separators=(',', ': ')
            )
        )
    else:
        click.echo(json.dumps(d))
