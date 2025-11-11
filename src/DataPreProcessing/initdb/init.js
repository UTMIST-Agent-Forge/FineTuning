db = db.getSiblingDB("test_database");

db.createCollection("collection1");

db.collection1.insertMany([
    { name: "Alice", email: "alice@example.com" },
    { name: "Bob", email: "bob@example.com" }
]);

print("Database initialized successfully!");
