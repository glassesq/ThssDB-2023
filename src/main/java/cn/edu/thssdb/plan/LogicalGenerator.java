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
package cn.edu.thssdb.plan;

import cn.edu.thssdb.parser.SQLParseError;
import cn.edu.thssdb.parser.ThssDBSQLVisitor;
import cn.edu.thssdb.sql.SQLLexer;
import cn.edu.thssdb.sql.SQLParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

public class LogicalGenerator {

  public static LogicalPlan generate(String sql) throws ParseCancellationException {
    ThssDBSQLVisitor dbsqlVisitor = new ThssDBSQLVisitor();

    CharStream charStream1 = CharStreams.fromString(sql);

    SQLLexer lexer1 = new SQLLexer(charStream1);
    lexer1.removeErrorListeners();
    lexer1.addErrorListener(SQLParseError.INSTANCE);

    CommonTokenStream tokens1 = new CommonTokenStream(lexer1);

    SQLParser parser1 = new SQLParser(tokens1);
    parser1.getInterpreter().setPredictionMode(PredictionMode.SLL);
    parser1.removeErrorListeners();
    parser1.addErrorListener(SQLParseError.INSTANCE);

    ParseTree tree;
    try {
      // STAGE 1: try with simpler/faster SLL(*)
      tree = parser1.sqlStmt();
      // if we get here, there was no syntax error and SLL(*) was enough;
      // there is no need to try full LL(*)
    } catch (Exception ex) {
      CharStream charStream2 = CharStreams.fromString(sql);

      SQLLexer lexer2 = new SQLLexer(charStream2);
      lexer2.removeErrorListeners();
      lexer2.addErrorListener(SQLParseError.INSTANCE);

      CommonTokenStream tokens2 = new CommonTokenStream(lexer2);

      SQLParser parser2 = new SQLParser(tokens2);
      parser2.getInterpreter().setPredictionMode(PredictionMode.LL);
      parser2.removeErrorListeners();
      parser2.addErrorListener(SQLParseError.INSTANCE);

      // STAGE 2: parser with full LL(*)
      tree = parser2.sqlStmt();
      // if we get here, it's LL not SLL
    }
    return dbsqlVisitor.visit(tree);
  }
}
