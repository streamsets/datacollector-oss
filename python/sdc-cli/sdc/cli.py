import api

import click
import json
import requests

pass_api = click.make_pass_decorator(api.DataCollector)


@click.group()
@click.option(
    '--sdc-url',
    default='http://localhost:18630/rest/v1',
    envvar='SDC_URL',
    help='Base URL of SDC REST endpoint.',
)
@click.pass_context
def cli(ctx, sdc_url):
    """sdc-cli is the command line interface for controlling StreamSets Data Collector via its REST API."""
    ctx.obj = api.DataCollector(sdc_url)


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
@pass_api
def list_pipelines(sdc_api, full):
    """Lists the pipelines stored in the pipeline library."""
    if full:
        click.echo(json.dumps(sdc_api.list_pipelines()))
    else:
        click.echo(json.dumps(sdc_api.list_pipeline_names()))


@library.command('import')
@click.argument('json_file', type=click.File('rb'))
@click.option('--pretty', is_flag=True, help='Pretty prints JSON when specified.')
@pass_api
def import_pipeline(sdc_api, json_file, pretty):
    """Imports a pipeline specified from a JSON file."""
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
