namespace java cn.edu.thssdb.rpc.thrift

// do not change this definition
struct Status {
  1: required i32 code;
  2: optional string msg;
}

// do not change this definition
struct GetTimeReq {
}

// do not change this definition
struct ConnectReq{
  1: required string username
  2: required string password
}

// do not change this definition
struct ConnectResp{
  1: required Status status
  2: required i64 sessionId
}

// do not change this definition
struct DisconnectReq{
  1: required i64 sessionId
}

// do not change this definition
struct DisconnectResp{
  1: required Status status
}

// do not change this definition
struct GetTimeResp {
  1: required string time
  2: required Status status
}

// do not change this definition
struct ExecuteStatementReq {
  1: required i64 sessionId
  2: required string statement
}

// do not change this definition
struct ExecuteStatementResp{
  1: required Status status
  2: required bool hasResult
  // only for query
  3: optional list<string> columnsList
  4: optional list<list<string>> rowList
}

service IService {
  GetTimeResp getTime(1: GetTimeReq req);
  ConnectResp connect(1: ConnectReq req);
  DisconnectResp disconnect(1: DisconnectReq req);
  ExecuteStatementResp executeStatement(1: ExecuteStatementReq req);
}
