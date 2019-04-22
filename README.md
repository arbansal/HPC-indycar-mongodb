# HPC-indycar-mongodb

The goal of the project is to introduce a data persistence layer (MongoDB) in the IndyCar Project (uses the logs generated during the Indycar race) and can be used to store data (by parsing the log records containing telemetry, weather, driver and other race related information and then segregating records based on header and field information) and help in retrieving relevant information from the MongoDB instance which can be served to the different modules of the project. 

Basically serves as a Data API for the Indycar project and can be imported at any other layer of Indycar application (Storm bolts, Dashboard Server etc.)
