# Bronze Models

Bronze models are source-aligned and minimally processed.

They preserve the Spark-produced Parquet datasets exposed through Spark ThriftServer or Hive, and they only apply safe column naming and basic type casting.

Bronze models do not perform business joins or business deduplication.
