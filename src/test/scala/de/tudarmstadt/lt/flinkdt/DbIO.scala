package de.tudarmstadt.lt.flinkdt

object DbIO extends App {
  
// INPUT
  
//// Read data from a relational database using the JDBC input format
//DataSet<Tuple2<String, Integer> dbData =
//    env.createInput(
//      // create and configure input format
//      JDBCInputFormat.buildJDBCInputFormat()
//                     .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
//                     .setDBUrl("jdbc:derby:memory:persons")
//                     .setQuery("select name, age from persons")
//                     .finish(),
//      // specify type information for DataSet
//      new TupleTypeInfo(Tuple2.class, STRING_TYPE_INFO, INT_TYPE_INFO)
//    );
//
//// Note: Flink's program compiler needs to infer the data types of the data items which are returned
//// by an InputFormat. If this information cannot be automatically inferred, it is necessary to
//// manually provide the type information as shown in the examples above.
  
  
  
// OUTPUT
  
//DataSet<Tuple3<String, Integer, Double>> myResult = [...]
//
//// write Tuple DataSet to a relational database
//myResult.output(
//    // build and configure OutputFormat
//    JDBCOutputFormat.buildJDBCOutputFormat()
//                    .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
//                    .setDBUrl("jdbc:derby:memory:persons")
//                    .setQuery("insert into persons (name, age, height) values (?,?,?)")
//                    .finish()
//    );
  
  
}