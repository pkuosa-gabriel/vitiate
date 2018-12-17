# VITIATE: A RESTful API server built with Ballerina

Before running or building, you will need to install Ballerina 0.990.0.

## To run locally

```sh
# Rename the example configuration file
mv ballerina.conf.example ballerina.conf

# Replace with your local information
# If the PostgreSQL server is running, you can start the server
ballerina run poem_service.bal
```

The server will be running at localhost:9090, and you can use tools like curl, Postman or Insomnia to test it.

## To build

```sh
ballerina build poem_service.bal
```