Climate normals service for MET API
==============================================

This module implements a service for retrieving IDF values for observations (IDF = Intensity Duration Frequency).

# Run

To be able to use the system, you will usually want to modify the
configuration files. For development purposes, you can instead create a file
`conf/development.conf` with the following entries:
```
db.kdvh.driver = org.postgresql.Driver
db.kdvh.url = "jdbc:postgresql://<dev-kdvh-proxy>:5432/<database name>"
db.kdvh.username = "<your-user-name>
db.kdvh.password = "<your-password>"
db.kdvh.logStatements = true
play.http.router = climatenormals.Routes
mail.override.address = "<your-email>"
play.evolutions.db.authorization.autoApply=true
auth.active=false
```

## Tests

To run the tests, do: `activator test`. To run tests with coverage report,
use: `activator coverage test coverageReport`.

## Running with Mock

To run the application with mock database connections, do: `activator run`

## Running in Test Production

To run the application in test production, you will need a working database
for the system to interact with.

This is specified in the db.kdvh.url field in the conf/development.conf example above.

Once the database is set up, you can run test production using `activator testProd`.
