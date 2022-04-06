import requests
import ast
from GCPFuncs import BQ_PROJECT, BQ_VISITS

url, header = "http://127.0.0.1:5000/", "/visits_api/"


def api_call(sql):
    """ Calls the API and returns query results as a dictionary """
    response = requests.get(url + header + sql)
    # print(response)
    return ast.literal_eval(response.text[1:-2].replace("\\", ""))


def incomplete_tasks():
    """ Analyses the task statuses and returns number of incomplete tasks """
    # Get the total number of unique tasks
    sql_total = \
        f"SELECT COUNT(DISTINCT task_id) total_tasks " \
        f"FROM {BQ_PROJECT}.{BQ_VISITS} "
    total_tasks = api_call(sql_total)
    print(f"Total tasks: {total_tasks['total_tasks']['0']}")
    # Get the number of tasks marked as failed
    sql_failed = \
        f"SELECT COUNT(DISTINCT task_id) failed_tasks " \
        f"FROM {BQ_PROJECT}.{BQ_VISITS} " \
        f"WHERE outcome != 'SUCCESS'"
    failed_tasks = api_call(sql_failed)
    print(f"Failed tasks: {failed_tasks['failed_tasks']['0']}")
    # Get the number of tasks marked as successful
    sql_successful = \
        f"SELECT COUNT(DISTINCT task_id) successful_tasks " \
        f"FROM {BQ_PROJECT}.{BQ_VISITS} " \
        f"WHERE outcome = 'SUCCESS'"
    successful_tasks = api_call(sql_successful)
    print(f"Successful tasks: {successful_tasks['successful_tasks']['0']}")
    # Return total tasks minus successful tasks
    return total_tasks['total_tasks']['0'] - successful_tasks['successful_tasks']['0']


def engineer_skill_levels(task_id):
    """ Returns a list of unique engineer skill levels that have visited for a given task"""
    sql = \
        f"SELECT DISTINCT engineer_skill_level " \
        f"FROM {BQ_PROJECT}.{BQ_VISITS} " \
        f"WHERE task_id = {task_id}"
    data = api_call(sql)['engineer_skill_level']
    levels_list = []
    for key, value in data.items():
        levels_list.append(value)
    return levels_list


def run():
    print(f"\nThere are currently {incomplete_tasks()} incomplete tasks.")
    while True:
        task = input("\nSearch for a task ID, or type 'q' to exit: ")
        try:
            if task == 'q':
                print("Goodbye.")
                break
            int(task)
            if engineer_skill_levels(task):
                print(f"\nFor task_id {task}, "
                      f"visits were conducted by engineers with skill levels: \n{engineer_skill_levels(task)}.")
            else:
                print(f"No visits found for task {task}.")
        except ValueError:
            print("Error - please input an integer value.")


if __name__ == '__main__':
    run()
