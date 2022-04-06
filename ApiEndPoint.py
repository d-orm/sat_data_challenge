from flask import Flask
from flask_restful import Api, Resource
import pandas_gbq
from GCPFuncs import BQ_PROJECT

# Initialize Flask REST API
app = Flask(__name__)
api = Api(app)


def get_bq_data(sql):
    """ Retrieves data from a Big Query table using SQL """
    df = pandas_gbq.read_gbq(sql, project_id=BQ_PROJECT)
    print(df)
    return df.to_json()


class GetRequest(Resource):
    """ Instantiates a resource for making API calls through the BQ function """
    def get(self, sql):
        df = get_bq_data(sql)
        return df


# Specify the API url and input parameters
api.add_resource(GetRequest, "/visits_api/<string:sql>")

# Run the API server
if __name__ == "__main__":
    app.run(debug=True)
