import networkx as nx
from meteostat import Daily
import matplotlib.pyplot as plt
import re
from GCPFuncs import *


def get_network_data(buck, file):
    """ Downloads adjacency list from GCP bucket, reads/analyzes and returns graph object and various dataframes """
    # Download temp file and store as NX Graph object in a variable
    f = dl_from_gcp_bucket(buck, file)
    G = nx.read_adjlist(f)  # Return with index [0]
    # Delete the temp file once its been read
    os.remove(f)

    # Create nodes [1], edges [2], and matrix [3] dataframes
    df_nodes, df_edges, df_mat = \
        pd.DataFrame(G.nodes, columns=['root_node']), \
        pd.DataFrame(G.edges, columns=['root_node', 'edge']), \
        nx.to_pandas_adjacency(G)
    df_mat = df_mat.reset_index(level=0)
    df_mat = df_mat.rename(columns={'index': 'root_node'})

    # Create number of hops dataframe [4]
    # # Create dict of shortest paths between every node
    paths = dict(nx.all_pairs_shortest_path(G))
    # # Create empty lists to store the number of hops data
    root_node, dt_node, num_hops = [], [], []
    # # Loop through the paths dict and populate lists
    for node in paths:
        for key, value in paths[node].items():
            root_node.append(node)
            dt_node.append(key)
            # Compute the length of each shortest path list
            num_hops.append(len([item for item in value if item]) - 1)
    # # Create dataframe from lists
    df_hops = pd.DataFrame()
    df_hops['root_node'], df_hops['dt_node'], df_hops['num_hops'] = root_node, dt_node, num_hops

    # Create dataframe from cluster values [5]
    df_cl = pd.Series(nx.clustering(G)).to_frame('cluster_value')
    df_cl = df_cl.reset_index(level=0)
    df_cl = df_cl.rename(columns={'index': 'root_node'})

    # Replace all string ID values with integers for all dataframes
    df_nodes['root_node'] = df_nodes['root_node'].map(lambda x: x.lstrip('NODE')).astype(int)
    df_edges['root_node'] = df_edges['root_node'].map(lambda x: x.lstrip('NODE')).astype(int)
    df_edges['edge'] = df_edges['edge'].map(lambda x: x.lstrip('NODE')).astype(int)
    df_mat['root_node'] = df_mat['root_node'].map(lambda x: x.lstrip('NODE')).astype(int)
    df_hops['root_node'] = df_hops['root_node'].map(lambda x: x.lstrip('NODE')).astype(int)
    df_hops['dt_node'] = df_hops['dt_node'].map(lambda x: x.lstrip('NODE')).astype(int)
    df_cl['root_node'] = df_cl['root_node'].map(lambda x: x.lstrip('NODE')).astype(int)

    return G, df_nodes, df_edges, df_mat, df_hops, df_cl


def get_visits_data(buck, file):
    """ Reads and transforms line separated JSON objects from a GCP bucket .txt file into a dataframe """
    df = pd.read_json(f'gcs://{buck}/{file}', lines=True)
    # Remove special characters from column names
    df = df.rename(columns=lambda x: re.sub(':', '', x))
    # Replace all string ID values with integers
    df['task_id'] = df['task_id'].map(lambda x: x.lstrip('TASK')).astype(int)
    df['node_id'] = df['node_id'].map(lambda x: x.lstrip('NODE')).astype(int)
    df['node_type'] = df['node_type'].map(lambda x: x.lstrip('TYPE')).astype(int)
    df['task_type'] = df['task_type'].map(lambda x: x.lstrip('TASK')).astype(int)
    df['engineer_skill_level'] = df['engineer_skill_level'].map(lambda x: x.lstrip('LEVEL')).astype(int)
    # Convert all string dates to timestamps
    df['visit_date'], df['original_reported_date'] = \
        pd.to_datetime(df['visit_date']), pd.to_datetime(df['original_reported_date'])

    return df


def get_weather_data(start, end, city):
    """ Retrieves time series regional weather data from an API as a dataframe """
    df = Daily(city, start, end).fetch()
    df = df.reset_index(level=0)

    return df


def get_data_info(buck, vis_file, net_file):
    """ Prints data info in console and visualises network structure """
    print("\n\t ------------------ NETWORK[0] GRAPH OBJECT ------------------")
    print(type(get_network_data(buck, net_file)[0]))
    print(get_network_data(buck, net_file)[0])
    print("\n\t ------------------ NETWORK[1] NODES DATAFRAME ------------------")
    print(get_network_data(buck, net_file)[1].info())
    print("\n\t ------------------ NETWORK[2] EDGES DATAFRAME ------------------")
    print(get_network_data(buck, net_file)[2].info())
    print("\n\t ------------------ NETWORK[3] MATRIX DATAFRAME ------------------")
    print(get_network_data(buck, net_file)[3].info())
    print("\n\t ------------------ NETWORK[4] HOPS DATAFRAME ------------------")
    print(get_network_data(buck, net_file)[4].info())
    print("\n\t ------------------ NETWORK[5] CLUSTERS DATAFRAME ------------------")
    print(get_network_data(buck, net_file)[5].info())
    print("\n\t ------------------ VISITS DATAFRAME ------------------")
    df_visits = get_visits_data(buck.name, vis_file)
    print(df_visits.info())
    print("\n\t ------------------ WEATHER DATAFRAME ------------------")
    min_date, max_date = df_visits['visit_date'].min(), df_visits['visit_date'].max()
    print(get_weather_data(min_date, max_date, london).info())
    # Draw a graph visualisation
    nx.draw(get_network_data(bucket, net_file)[0])
    plt.show()


def upload_dataframes_to_bq(buck, vis_file, net_file):
    """ Builds dataframes and uploads initial datasets to Big Query tables """
    # Upload the visits dataframe, also store to get date variables for weather
    df_visits = get_visits_data(buck.name, vis_file)
    df_visits.to_gbq(BQ_VISITS, BQ_PROJECT, if_exists='append')
    # Store the file name in the processed visits log
    log = pd.DataFrame([vis_file])
    log['timestamp'] = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    write_df_to_g_sheet(visits_log, 0, log)
    # Get the min and max visit dates for weather data range
    min_date, max_date = df_visits['visit_date'].min(), df_visits['visit_date'].max()
    # Upload the weather data
    get_weather_data(min_date, max_date, london).to_gbq(BQ_WEATHER, BQ_PROJECT, if_exists='append')
    # Upload the various network dataframes
    get_network_data(buck, net_file)[1].to_gbq(BQ_NETWORK_NODES, BQ_PROJECT, if_exists='append')
    get_network_data(buck, net_file)[2].to_gbq(BQ_NETWORK_EDGES, BQ_PROJECT, if_exists='append')
    get_network_data(buck, net_file)[3].to_gbq(BQ_NETWORK_MATRIX, BQ_PROJECT, if_exists='append')
    get_network_data(buck, net_file)[4].to_gbq(BQ_NETWORK_HOPS, BQ_PROJECT, if_exists='append')
    get_network_data(buck, net_file)[5].to_gbq(BQ_NETWORK_CL, BQ_PROJECT, if_exists='append')


def dynamic_visits_upload():
    """ Loops through the files in the visits bucket. Checks if already processed and processes if not found """
    # Search through the visits bucket and read the file names
    for file in list_bucket_contents(bucket):
        # Check if the file name exists in the processed visits log, and skip over it if it does
        if file in get_df_from_g_sheet(visits_log, 'Sheet1')['processed_file'].tolist():
            print(f"File: {file} already exists, skipping...")
        else:
            # Upload the visits file if it's not found in the visits log
            print(f"File: {file} not found, processing...")
            df_visits = get_visits_data(bucket.name, file)
            print(df_visits.info())
            # df_visits.to_gbq(BQ_VISITS, BQ_PROJECT, if_exists='append')

            # Retrieve min and max visit date for weather API and upload weather data to BQ
            min_date, max_date = df_visits['visit_date'].min(), df_visits['visit_date'].max()
            print(get_weather_data(min_date, max_date, london).info())
            # get_weather_data(min_date, max_date, london).to_gbq(BQ_WEATHER, BQ_PROJECT, if_exists='append')

            # Store the file name in the visits log
            log = pd.DataFrame([file])
            log['timestamp'] = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
            write_df_to_g_sheet(visits_log, 0, log)


if __name__ == '__main__':
    get_data_info(bucket, visits_file, network_file)
    # upload_dataframes_to_bq(bucket, visits_file, network_file)
    # dynamic_visits_upload()
