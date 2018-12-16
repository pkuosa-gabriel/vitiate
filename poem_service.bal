import ballerina/config;
import ballerina/http;
import ballerina/io;
import ballerina/log;
import ballerinax/docker;
import ballerinax/jdbc;

@docker:Config {
    registry:"pkuosa-gabriel",
    name:"vitiate",
    tag:"v1.0"
}
@docker:Expose{}

listener http:Listener httpListener = new(9090);

type Poem record {
    string id;
    string title;
    string author;
    string content;
    string created_at;
    string updated_at;
};

jdbc:Client testDB = new({
    url: config:getAsString("DATABASE_URL", default = "jdbc:postgresql://localhost:5432/vitiate_dev"),
    username: config:getAsString("DATABASE_USERNAME", default = "gabriel"),
    password: config:getAsString("DATABASE_PASSWORD", default = ""),
    poolOptions: { maximumPoolSize: 5 },
    dbOptions: { useSSL: false }
});

// Poem management is done using an in-memory map.
// Add some sample poems to 'poemsMap' at startup.
map<json> poemsMap = {};

// RESTful service.
@http:ServiceConfig { basePath: "/api" }
service poemMgt on httpListener {

    // Resource that handles the HTTP GET requests that are directed to a specific
    // poem using path '/poem/<poemId>'.
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/poem/{poemId}"
    }
    resource function findPoem(http:Caller caller, http:Request req, string poemId) {
        // Find the requested poem from the map and retrieve it in JSON format.
        json? payload = null;
        http:Response response = new;

        var selectRet = testDB->select("SELECT * FROM poem WHERE id = CAST((?) AS uuid)", Poem, loadToMemory = true, poemId);
        if (selectRet is table<Poem>) {
            var jsonConvertRet = json.convert(selectRet);
            if (jsonConvertRet is json) {
                payload = jsonConvertRet;
            } else {
                payload = { "Status": "Poem Not Found", "Error": "Error occurred in data conversion" };
                log:printError("Error occurred in data conversion", err = jsonConvertRet);
            }
        } else if (selectRet is error) {
            io:println("Select data from poem table failed: " + <string>selectRet.detail().message);
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
    // '/poem' to create a new Poem.
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/poem"
    }
    resource function addPoem(http:Caller caller, http:Request req) {
        http:Response response = new;
        var poemReq = req.getJsonPayload();
        string poemId = "";
        if (poemReq is json) {
            var returned = testDB->updateWithGeneratedKeys("INSERT INTO poem(title, author, content) values (?, ?, ?)", (), poemReq.Poem.title.toString(), poemReq.Poem.author.toString(), poemReq.Poem.content.toString());
            if (returned is (int, string[])) {
                var (count, ids) = returned;
                poemId = ids[0];
                log:printInfo("Inserted row count to Poems table: " + count);
                log:printInfo("Generated key: " + poemId);
            } else if (returned is error) {
                log:printError("Insert to Poems table failed: " + <string>returned.detail().message);
            }

            // Create response message.
            json payload = { status: "Poem Created.", poemId: poemId};
            response.setJsonPayload(untaint payload);

            // Set 201 Created status code in the response message.
            response.statusCode = 201;
            // Set 'Location' header in the response message.
            // This can be used by the client to locate the newly added poem.
            response.setHeader("Location", 
                "http://localhost:9090/api/poem/" + poemId);

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
    // '/poem/<poemId>' to update an existing Poem.
    @http:ResourceConfig {
        methods: ["PUT"],
        path: "/poem/{poemId}"
    }
    resource function updatePoem(http:Caller caller, http:Request req, string poemId) {
        var updatedPoem = req.getJsonPayload();
        http:Response response = new;
        json payload;
        if (updatedPoem is json) {
            var ret = testDB->update("UPDATE poem SET content = (?) WHERE id = CAST((?) AS uuid)", updatedPoem.content.toString(), poemId);
            if (ret is int) {
                if (ret > 0) {
                    payload = { "Status": "Poem Updated Successfully" };
                    log:printInfo("Poem updated successfully");
                } else {
                    payload = { "Status": "Poem Not Updated" };
                    log:printError("Error occurred during update operation");
                }
            } else {
                payload = { "Status": "Poem Not Updated",  "Error": "Error occurred during update operation" };
                log:printError("Error occurred during update operation", err = ret);
            }
            response.setJsonPayload(untaint payload);
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
    // '/poem/<poemId>' to delete an existing Poem.
    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/poem/{poemId}"
    }
    resource function deletePoem(http:Caller caller, http:Request req, string poemId) {
        http:Response response = new;
        json payload = "";
        var ret = testDB->update("DELETE FROM poem WHERE id = CAST((?) AS uuid)", poemId);
        if (ret is int) {
            payload = { "Status": "Poem : " + poemId + " removed." };
            log:printInfo("Poem : " + poemId + " removed.");
        } else {
            payload = { "Status": "Poem : " + poemId + " Not Deleted",  "Error": "Error occurred during delete operation" };
            log:printError("Error occurred during delete operation", err = ret);
        }

        // Set a generated payload with poem status.
        response.setJsonPayload(untaint payload);

        // Send response to the client.
        var result = caller->respond(response);
        if (result is error) {
            log:printError("Error sending response", err = result);
        }
    }
}

// Main function
public function main() {
    var returned = testDB->update("CREATE TABLE poem(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), title VARCHAR(255), author VARCHAR(255), content TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())");
    if (returned is int) {
        log:printInfo("Poems table create status in DB: " + returned);
    } else if (returned is error) {
        log:printError("Poems table create failed: " + <string>returned.detail().message);
    }
}