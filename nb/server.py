import grpc
import station_pb2
from concurrent import futures
import pandas as pd
import station_pb2_grpc

from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement
try:
    cluster = Cluster(['project-5-anushkap5-db-1', 'project-5-anushkap5-db-2', 'project-5-anushkap5-db-3'])
    session = cluster.connect()
except Exception as e:
    print(e)

class Station(station_pb2_grpc.StationServicer):
    def RecordTemps(self, request, context):
        insert_statement = session.prepare("insert into weather.stations (id, date, record) values (?, ?, {tmin: ?, tmax: ?})""")
        insert_statement.consistency_level = ConsistencyLevel.ONE
        try:
            session.execute(insert_statement, (request.station, request.date, request.tmin, request.tmax))
            return station_pb2.RecordTempsReply()
        except:
            return station_pb2.RecordTempsReply(error="Insert failed.")

    def StationMax(self, request, context):
        max_statement = session.prepare("select max(record.tmax) as tmax_val from weather.stations where id=?")
        max_statement.consistency_level = ConsistencyLevel.ALL
        try:
            val = session.execute(max_statement, (request.station,)).all()[0].tmax_val
            return station_pb2.StationMaxReply(tmax = val)
        except:
            return station_pb2.StationMaxReply(error="Unable to return maximum tmax for given station.")

server = grpc.server(futures.ThreadPoolExecutor(max_workers=1), options=[("grpc.so_reuseport", 0)])
station_pb2_grpc.add_StationServicer_to_server(Station(), server)

server.add_insecure_port('[::]:5444')
server.start()
server.wait_for_termination()

