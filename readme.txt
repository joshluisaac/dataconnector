DataConnector is the module in PowerCollect responsible to interface with other external systems in batch mode.
It consist of 2 sub-modules, DataLoader and DataExtractor.

DataLoader is responsible for transformation and loading of data from external systems (host, 3rd party) into PowerCollect's database.

DataExtractor is responsible for extracting information out of PowerCollect's database to be provided to external systems.  DataExtractor is capable of extracting data not only from PowerCollect's database, but also any other RDBMS that supports jdbc.

DataConnector is a complete ETL tool for a data interchange needs in PowerCollect.

Details regarding installing, configuring and running DataConnector is available in the DataConnector documentation provided by Profitera.