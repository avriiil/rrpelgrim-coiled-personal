import datetime
import coiled
from distributed import Client
import dask.bag as db
import ujson

from prefect import task, Flow, case
from prefect.tasks.control_flow import merge

# SET-UP
# spin up cluster function
@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def start_cluster(name="prefect", software="coiled-examples/prefect", n_workers=10):
    '''
    This task spins up a Coiled cluster and connects it to the Dask client.

    name: name of the cluster, defaults to "prefect"
    software: Coiled software environment to install on all workers, defaults to "coiled-examples/prefect"
    n_workers: number of Dask workers in the cluster, defaults to 10
    '''
    cluster = coiled.Cluster(
        name=name,
        software=software,
        n_workers=n_workers,
        )
    client = Client(cluster)
    return client

# create list of filenames to fetch
@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def create_list(start_date, end_date, format="%d-%m-%Y"):
    '''
    This task generates a list of filenames to fetch from the Github Archive API.

    start_date: a string containing the start date
    end_date: a string containing the end date
    format: datetime format, defaults to "%d-%m-%Y"
    '''
    start = datetime.datetime.strptime(start_date, format)
    end = datetime.datetime.strptime(end_date, format)
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
    prefix = "https://data.gharchive.org/"
    filenames = []
    for date in date_generated:
        for hour in range(1,24):
            filenames.append(prefix + date.strftime("%Y-%m-%d") + '-' + str(hour) + '.json.gz')
    return filenames

# EXTRACT
# get data from Github api
@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def get_github_data(filenames):
    '''
    Task to fetch JSON data from Github Archive project and filter out PushEvents.

    filenames: list of filenames created with create_list() task
    '''
    records = db.read_text(filenames).map(ujson.loads)
    push_events = records.filter(lambda record: record["type"] == "PushEvent")
    return push_events



# TRANSFORM 
# transform json into dataframe
@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def to_dataframe(push_events):
    '''
    This task processes the nested json data into a flat, tabular format. 
    Each row represents a single commit.

    push_events: PushEvent data fetched with get_github_data()
    '''
    def process(record):
            try:
                for commit in record["payload"]["commits"]:
                    yield {
                        "user": record["actor"]["login"],
                        "repo": record["repo"]["name"],
                        "created_at": record["created_at"],
                        "message": commit["message"],
                        "author": commit["author"]["name"],
                    }
            except KeyError:
                pass
        
    processed = push_events.map(process)
    df = processed.flatten().to_dataframe()
    return df


# LOAD
# write to parquet
@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def to_parquet(df, path):
    '''
    This task writes the flattened dataframe of PushEvents to the specified path as a parquet file.

    path: directory to write parquet file to, can be local or cloud store.
    '''
    df.to_parquet(
        path,
        engine='pyarrow',
        compression='snappy'
    )

@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def check_size(filenames):
    # there are probably many (better) ways to do this, but let's use a trivial method for now
    return len(filenames) < 50

# Build Prefect Flow
with Flow(name="Github ETL Test") as flow:
    filenames = create_list(start_date="01-01-2015", end_date="31-12-2015")
    check_size = check_size(filenames)
    
    # check if len(filenames) < 50
    #if True, start localcluster as client
    with case(check_size, True):
        client1 = Client()
    #if false, start Coiled cluster as client
    with case(check_size, False):
        client2 = start_cluster()

    client = merge(client1, client2)
    push_events = get_github_data(filenames)
    df = to_dataframe(push_events)
    
    # can also include local vs cloud write here depending on size
    # but let's just try this for now
    to_parquet(df, path="s3://coiled-datasets/prefect/prefect-test.parq")

flow.run()