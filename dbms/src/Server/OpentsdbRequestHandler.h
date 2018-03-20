#pragma once

#include "IServer.h"

#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

class OpentsdbPutRequestHandler : public Poco::Net::HTTPRequestHandler
{
private:
    IServer & server;
public:
    explicit OpentsdbPutRequestHandler(IServer & server_) : server(server_)
    {
    }

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override;
};
class OpentsdbSuggestRequestHandler : public Poco::Net::HTTPRequestHandler
{
private:
    IServer & server;

public:
    explicit OpentsdbSuggestRequestHandler(IServer & server_) : server(server_)
    {
    }

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override;
};

class OpentsdbQueryRequestHandler : public Poco::Net::HTTPRequestHandler
{
private:
    IServer & server;

public:
    explicit OpentsdbQueryRequestHandler(IServer & server_) : server(server_)
    {
    }

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override;
};

}
