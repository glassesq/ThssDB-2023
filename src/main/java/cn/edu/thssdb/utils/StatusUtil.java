/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.thssdb.utils;

import cn.edu.thssdb.rpc.thrift.Status;

public class StatusUtil {

  public static Status success(String msg) {
    Status status = new Status(Global.SUCCESS_CODE);
    status.setMsg(msg);
    return status;
  }

  public static Status success() {
    Status status = new Status(Global.SUCCESS_CODE);
    status.setMsg("The statement is executed successfully.");
    return status;
  }

  public static Status fail(String msg) {
    Status status = new Status(Global.FAILURE_CODE);
    status.setMsg(msg);
    return status;
  }
}
