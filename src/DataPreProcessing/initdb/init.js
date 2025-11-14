db = db.getSiblingDB("test_database");

db.createCollection("rawData");

db.createCollection("standardizerStep");
db.createCollection("removeDuplicatesStep");
db.createCollection("extractMetaDataStep");
db.createCollection("qualityFilterStep");

db.createCollection("finalProcessedData");


print("Database initialized successfully!");
