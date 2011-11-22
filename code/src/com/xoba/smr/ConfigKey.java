package com.xoba.smr;

public enum ConfigKey {

	AWS_KEYID, AWS_SECRETKEY,

	MAP_QUEUE, REDUCE_QUEUE,

	SIMPLEDB_DOM,

	THREADS_PER_MACHINE,

	SHUFFLE_COMPARATOR,

	MAP_INPUTS_BUCKET, SHUFFLE_BUCKET, REDUCE_OUTPUTS_BUCKET,

	CLASSPATH_JAR,

	MAPPER, REDUCER,

	HASH_CARDINALITY,

	IS_INPUT_COMPRESSED,

	RUNNABLE_JARFILE_URI,

	SNS_ARN,

	MAP_READER, SHUFFLEWRITER, SHUFFLEREADER, REDUCEWRITER;

}