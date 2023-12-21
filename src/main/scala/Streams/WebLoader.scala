package Streams

import DataStructures.StreamStruct
import WebApi.{Request, Respone, api_data, webapi}
import org.apache.spark.sql.DataFrame

trait WebLoader {

def get_req_data (p_req:Request): StreamStruct
}
