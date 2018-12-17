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
@docker:CopyFiles {
    files: [
        {
            source:"./lib/postgresql-42.2.5.jar", 
            target:"/ballerina/runtime/bre/lib"
        },
        {
            source:"./ballerina.conf",
            target:"/home/ballerina"      
        }
    ]
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

jdbc:Client mainDB = new({
    url: config:getAsString("DATABASE_URL", default = "jdbc:postgresql://localhost:5432/vitiate_dev"),
    username: config:getAsString("DATABASE_USERNAME", default = "postgres"),
    password: config:getAsString("DATABASE_PASSWORD", default = ""),
    poolOptions: { maximumPoolSize: 5 },
    dbOptions: { useSSL: false }
});

// RESTful service.
@http:ServiceConfig { basePath: "/api" }
service poem on httpListener {
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/poems"
    }
    resource function getPoems(http:Caller caller, http:Request req) {
        json? payload = null;
        http:Response response = new;

        // Read pagination param from request
        var queryParams = req.getQueryParams();
        int|error queryPage = int.convert(queryParams["page"]?: "");
        int page;
        if (queryPage is int) {
            page = queryPage;
        } else {
            page = 0;
        }
        page = page * 10;

        var poems = mainDB->select("SELECT * FROM poem ORDER BY updated_at DESC LIMIT 10 OFFSET (?)", Poem, loadToMemory = true, page);
        if (poems is table<Poem>) {
            var poemsJson = json.convert(poems);
            if (poemsJson is json) {
                payload = poemsJson;
            } else {
                payload = { "Status": "No poems", "Error": "Error occurred in data conversion" };
                log:printError("Error occurred in data conversion", err = poemsJson);
            }
        } else if (poems is error) {
            io:println("Select data from Table poem failed: " + <string>poems.detail().message);
        }

        // Set the JSON payload in the outgoing response message.
        response.setJsonPayload(untaint payload);

        // Send response to the client.
        var result = caller->respond(response);
        if (result is error) {
            log:printError("Error sending response", err = result);
        }
    }

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

        var poems = mainDB->select("SELECT * FROM poem WHERE id = CAST((?) AS uuid)", Poem, loadToMemory = true, poemId);
        if (poems is table<Poem>) {
            var poemsJson = json.convert(poems);
            if (poemsJson is json) {
                payload = poemsJson;
            } else {
                payload = { "Status": "Poem Not Found", "Error": "Error occurred in data conversion" };
                log:printError("Error occurred in data conversion", err = poemsJson);
            }
        } else if (poems is error) {
            io:println("Select data from poem table failed: " + <string>poems.detail().message);
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
            var dbRet = mainDB->updateWithGeneratedKeys("INSERT INTO poem(title, author, content) values (?, ?, ?)", (), poemReq.Poem.title.toString(), poemReq.Poem.author.toString(), poemReq.Poem.content.toString());
            if (dbRet is (int, string[])) {
                var (count, ids) = dbRet;
                poemId = ids[0];
                log:printInfo("Inserted row count to Poems table: " + count);
                log:printInfo("Generated key: " + poemId);
            } else if (dbRet is error) {
                log:printError("Insert to Poems table failed: " + <string>dbRet.detail().message);
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
            var dbRet = mainDB->update("UPDATE poem SET content = (?) WHERE id = CAST((?) AS uuid)", updatedPoem.content.toString(), poemId);
            if (dbRet is int) {
                if (dbRet > 0) {
                    payload = { "Status": "Poem Updated Successfully" };
                    log:printInfo("Poem updated successfully");
                } else {
                    payload = { "Status": "Poem Not Updated" };
                    log:printError("Error occurred during update operation");
                }
            } else {
                payload = { "Status": "Poem Not Updated",  "Error": "Error occurred during update operation" };
                log:printError("Error occurred during update operation", err = dbRet);
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
        var dbRet = mainDB->update("DELETE FROM poem WHERE id = CAST((?) AS uuid)", poemId);
        if (dbRet is int) {
            payload = { "Status": "Poem : " + poemId + " removed." };
            log:printInfo("Poem : " + poemId + " removed.");
        } else {
            payload = { "Status": "Poem : " + poemId + " Not Deleted",  "Error": "Error occurred during delete operation" };
            log:printError("Error occurred during delete operation", err = dbRet);
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
    // Create Table poem if it does not exist
    var dbRet = mainDB->update("CREATE TABLE IF NOT EXISTS poem(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), title VARCHAR(255), author VARCHAR(255), content TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())");
    if (dbRet is int) {
        log:printInfo("Table poem in DB: " + dbRet);
    } else if (dbRet is error) {
        log:printError("Table poem creation failed: " + <string>dbRet.detail().message);
    }
}