import ballerina/http;
import ballerina/log;
import ballerinax/docker;

@docker:Config {
    registry:"pkuosa-gabriel",
    name:"vitiate",
    tag:"v1.0"
}
@docker:Expose{}

listener http:Listener httpListener = new(9090);

// Order management is done using an in-memory map.
// Add some sample orders to 'ordersMap' at startup.
map<json> ordersMap = {};

// RESTful service.
@http:ServiceConfig { basePath: "/api" }
service orderMgt on httpListener {

    // Resource that handles the HTTP GET requests that are directed to a specific
    // order using path '/order/<orderId>'.
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/order/{orderId}"
    }
    resource function findOrder(http:Caller caller, http:Request req, string orderId) {
        // Find the requested order from the map and retrieve it in JSON format.
        json? payload = ordersMap[orderId];
        http:Response response = new;
        if (payload == null) {
            payload = "Order : " + orderId + " cannot be found.";
        }

        // Set the JSON payload in the outgoing response message.
        response.setJsonPayload(untaint payload);

        // Send response to the client.
        var result = caller->respond(response);
        if (result is error) {
            log:printError("Error sending response", err = result);
        }
    }

    // Resource that handles the HTTP POST requests that are directed to the path
    // '/order' to create a new Order.
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/order"
    }
    resource function addOrder(http:Caller caller, http:Request req) {
        http:Response response = new;
        var orderReq = req.getJsonPayload();
        if (orderReq is json) {
            string orderId = orderReq.Order.ID.toString();
            ordersMap[orderId] = orderReq;

            // Create response message.
            json payload = { status: "Order Created.", orderId: orderId };
            response.setJsonPayload(untaint payload);

            // Set 201 Created status code in the response message.
            response.statusCode = 201;
            // Set 'Location' header in the response message.
            // This can be used by the client to locate the newly added order.
            response.setHeader("Location", 
                "http://localhost:9090/api/order/" + orderId);

            // Send response to the client.
            var result = caller->respond(response);
            if (result is error) {
                log:printError("Error sending response", err = result);
            }
        } else {
            response.statusCode = 400;
            response.setPayload("Invalid payload received");
            var result = caller->respond(response);
            if (result is error) {
                log:printError("Error sending response", err = result);
            }
        }
    }

    // Resource that handles the HTTP PUT requests that are directed to the path
    // '/order/<orderId>' to update an existing Order.
    @http:ResourceConfig {
        methods: ["PUT"],
        path: "/order/{orderId}"
    }
    resource function updateOrder(http:Caller caller, http:Request req, string orderId) {
        var updatedOrder = req.getJsonPayload();
        http:Response response = new;
        if (updatedOrder is json) {
            // Find the order that needs to be updated and retrieve it in JSON format.
            json existingOrder = ordersMap[orderId];

            // Updating existing order with the attributes of the updated order.
            if (existingOrder != null) {
                existingOrder.Order.Name = updatedOrder.Order.Name;
                existingOrder.Order.Description = updatedOrder.Order.Description;
                ordersMap[orderId] = existingOrder;
            } else {
                existingOrder = "Order : " + orderId + " cannot be found.";
            }
            // Set the JSON payload to the outgoing response message to the client.
            response.setJsonPayload(untaint existingOrder);
            // Send response to the client.
            var result = caller->respond(response);
            if (result is error) {
                log:printError("Error sending response", err = result);
            }
        } else {
            response.statusCode = 400;
            response.setPayload("Invalid payload received");
            var result = caller->respond(response);
            if (result is error) {
                log:printError("Error sending response", err = result);
            }
        }
    }

    // Resource that handles the HTTP DELETE requests, which are directed to the path
    // '/order/<orderId>' to delete an existing Order.
    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/order/{orderId}"
    }
    resource function cancelOrder(http:Caller caller, http:Request req, string orderId) {
        http:Response response = new;
        // Remove the requested order from the map.
        _ = ordersMap.remove(orderId);

        json payload = "Order : " + orderId + " removed.";
        // Set a generated payload with order status.
        response.setJsonPayload(untaint payload);

        // Send response to the client.
        var result = caller->respond(response);
        if (result is error) {
            log:printError("Error sending response", err = result);
        }
    }
}