from flask import Flask, jsonify, json
from flask_restful import fields, marshal_with, reqparse, abort, Api, Resource
from cassandra.cluster import Cluster
import requests
import requests_cache

requests_cache.install_cache('external_api_cache', backend='sqlite', expire_after=36000)

cluster = Cluster(contact_points=['172.17.0.2'], port=9042)
session = cluster.connect()
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
api = Api(app)
# CREATE CUSTOM INDEX employee_firstname_idx ON bth.employee (firstname) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS', 'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', 'case_sensitive': 'false'};
name_parser = reqparse.RequestParser()
name_parser.add_argument('name', type=str)

parser = reqparse.RequestParser(bundle_errors=True)
parser.add_argument('show_id', type=int, required=True)
parser.add_argument('cast')
parser.add_argument('country')
parser.add_argument('date_added')
parser.add_argument('description')
parser.add_argument('director')
parser.add_argument('duration')
parser.add_argument('listed_in')
parser.add_argument('rating')
parser.add_argument('release_year', type=int, help='Integer required: {error_msg}')
parser.add_argument('title')
parser.add_argument('type')

# fields = {
#     'show_id': fields.Integer,
#     'cast': fields.String,
#     'country': fields.String,
#     'date_added': fields.String,
#     'description': fields.String,
#     'director': fields.String,
#     'duration': fields.String,
#     'listed_in': fields.String,
#     'rating': fields.String,
#     'release_year': fields.Integer,
#     'title': fields.String,
#     'type': fields.String,
# }


class HelloWorld(Resource):
    def get(self):
        args = name_parser.parse_args()
        return {'hello': args['name']} if args['name'] != None else {'hello': 'world'}

class TVList(Resource):
    def get(self):
        rows = session.execute("""select json * from netflix.titles where type = 'TV Show' limit 10 ALLOW FILTERING;""")
        response = [json.loads(result[0]) for result in rows]
        # test = jsonify(response)
        # test2 = test.get_json()
        return jsonify(response)
        # return test2[0]['cast']

    # @marshal_with(fields)
    def post(self):
        args = parser.parse_args()
        rows = session.execute("""select json * from netflix.titles where show_id = {};""".format(args["show_id"]))
        response = [json.loads(result[0]) for result in rows]
        if response == []:
            columnList = ""
            valueList = ""
            for key, arg in args.items():
                if arg is not None:
                    if not str(arg).isnumeric():
                        arg = "'"+arg+"'"
                    columnList += ", "+ key
                    valueList +=  ", " + str(arg)
                #     args[key] = 'null'
                # elif not str(arg).isnumeric():
                #     arg = "'"+arg+"'"
            query = "insert into netflix.titles ("+columnList.strip(", ")+") values ("+valueList.strip(", ")+");"
            # print(columnList.strip(", "), valueList.strip(", "))
            print(query)
            # response = session.execute("""insert into netflix.titles (show_id, cast, country, date_added, description, director, duration, listed_in, rating, release_year, title, type) values ({},{},{},{},{},{},{},{},{},{},{},{});""".format(args['show_id'],args['cast'],args['country'],args['date_added'],args['description'],args['director'],args['duration'],args['listed_in'],args['rating'],args['release_year'],args['title'],args['type']))
            session.execute(query)
            return {'success' : True}, 201
        else:
            abort(404, message="That ID already exists!")

class MovieList(Resource):
    def get(self):
        rows = session.execute("""select json * from netflix.titles where type = 'Movie' limit 10 ALLOW FILTERING;""")
        response = [json.loads(result[0]) for result in rows]
        return jsonify(response)

    # @marshal_with(fields)
    def post(self):
        args = parser.parse_args()
        rows = session.execute("""select json * from netflix.titles where show_id = {};""".format(args["show_id"]))
        response = [json.loads(result[0]) for result in rows]
        if response == []:
            columnList = ""
            valueList = ""
            for key, arg in args.items():
                if arg is not None:
                    if not str(arg).isnumeric():
                        arg = "'"+arg+"'"
                    columnList += ", "+ key
                    valueList +=  ", " + str(arg)
                #     args[key] = 'null'
                # elif not str(arg).isnumeric():
                #     arg = "'"+arg+"'"
            query = "insert into netflix.titles ("+columnList.strip(", ")+") values ("+valueList.strip(", ")+");"
            # print(columnList.strip(", "), valueList.strip(", "))
            print(query)
            # response = session.execute("""insert into netflix.titles (show_id, cast, country, date_added, description, director, duration, listed_in, rating, release_year, title, type) values ({},{},{},{},{},{},{},{},{},{},{},{});""".format(args['show_id'],args['cast'],args['country'],args['date_added'],args['description'],args['director'],args['duration'],args['listed_in'],args['rating'],args['release_year'],args['title'],args['type']))
            session.execute(query)
            return {'success' : True}, 201
        else:
            abort(404, message="That ID already exists!")

# class Search_by_id(Resource):
#     def get(self, netflix_id):
#         if netflix_id.isnumeric():
#             rows = session.execute("""select json * from netflix.titles where show_id = {};""".format(netflix_id))
#             response = [json.loads(result[0]) for result in rows]
#             return jsonify(response) if response != [] else {'error' : 'That netflix ID does not exist!'}, 404
#         else:
#             abort(404, message="ID can only contain integer!")

# class Search_by_title(Resource):
#     def get(self, title):
#         rows = session.execute("""select json * from netflix.titles where title like '%{}' limit 10 ALLOW FILTERING;""".format(title))
#         response = [json.loads(result[0]) for result in rows]
#         if response != []:
#             return jsonify(response)
#         else:
#             abort(404, message="Title containing {} doesn't exist".format(title))
#             # return {'error':'No title contain that word! P.S. Query is case sensitive'}, 404
#         # return jsonify(response) if response != [] else {'No title contain that word! P.S. Query is case sensitive'}, 404

class Show(Resource):
    def get(self, show_id):
        if show_id.isnumeric():
            rows = session.execute("""select json * from netflix.titles where show_id = {};""".format(show_id))
            response = [json.loads(result[0]) for result in rows]
            if response != []:
                return jsonify(response)
            else:
                abort(404, message="That ID doesn't exist!")
            # return jsonify(response) if response != [] else {'error' : 'That netflix ID does not exist!'}, 404
        else:
            rows = session.execute("""select json * from netflix.titles where title like '%{}' limit 10 ALLOW FILTERING;""".format(show_id))
            response = [json.loads(result[0]) for result in rows]
            if response != []:
                return jsonify(response)
            else:
                abort(404, message="Title containing {} doesn't exist!".format(show_id))

    def delete(self, show_id):
        rows = session.execute("""select json * from netflix.titles where show_id = {};""".format(show_id))
        response = [json.loads(result[0]) for result in rows]
        if response != []:
            session.execute("DELETE FROM netflix.titles WHERE show_id = {} IF EXISTS;".format(show_id))
            return {'success' : True}, 204
        else:
            abort(404, message="That show ID doesn't exist!")

    def put(self, show_id):
        rows = session.execute("""select json * from netflix.titles where show_id = {};""".format(show_id))
        response = [json.loads(result[0]) for result in rows]
        if response != []:
            parser.remove_argument('show_id')
            args = parser.parse_args()
            parser.add_argument('show_id', type=int, required=True)
            values = ""
            for key, arg in args.items():
                if arg is not None:
                    # if key == "show_id":
                    #     args[key]=show_id
                    if not str(arg).isnumeric():
                        arg = "'"+arg+"'"
                    values += ", " + key + " = " + str(arg)
            query = "update netflix.titles set "+values.strip(", ")+" where show_id = "+show_id+";"
            # print(columnList.strip(", "), valueList.strip(", "))
            # print(query)
            session.execute(query)
            return {'success' : True}, 201
        else:
            abort(404, message="That show ID doesn't exist!")

class External(Resource):
    def get(self, keyword):
        resp = requests.get("http://api.tvmaze.com/search/shows?q={}".format(keyword))
        if resp.ok:
            return resp.json()
        else:
            print(resp.reason)

api.add_resource(HelloWorld, '/')
api.add_resource(TVList, '/tvshows')
api.add_resource(MovieList, '/movies')
# api.add_resource(Search_by_id, '/search/id/<netflix_id>')
# api.add_resource(Search_by_title, '/search/title/<title>')
api.add_resource(Show, '/show/<show_id>')
api.add_resource(External, '/external/<keyword>')

if __name__ == '__main__':
    # app.run(debug=True)
    app.run(host='0.0.0.0', port=80)