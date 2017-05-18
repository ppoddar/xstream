# User Manual

A timeseries is represented by `xstream.TimeSeries` class.
To create a timeseries

	String seriesURI = "nosql://localhost:5000/kvstore/traffic";
	String[] fieldDefs = {"x DOUBLE", "y DOUBLE"};
	TimeSeries series = TimeSeries.openOrCreate(seriesURI, fieldDefs);

The first argument to `openOrCreate()` is an URI (Uniform
Resource Identifier) of a timeseries. The URI show here specifies a timeseries
named `traffic` stored or to be stored on a NoSQL database named 
`kvstore` 
running at `localhost` and listening at port `5000`



