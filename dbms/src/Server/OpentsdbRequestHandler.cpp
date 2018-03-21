#include "OpentsdbRequestHandler.h"

#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>

#include <Common/Exception.h>
#include <Common/HTMLForm.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Struct.h>

#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTInsertQuery.h>

#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>

#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>

using namespace Poco::JSON;
using namespace Poco::Dynamic;
using Poco::DynamicStruct;

namespace DB
{
void parseParam(Poco::Net::HTTPServerRequest & request)
{
    try
    {//wheather POST or GET?
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
        {
            HTMLForm params(request);
            std::string type = params.get("type","");
            std::string q = params.get("q","");
            std::string m = params.get("max","");
            std::cout << "type:" << type << "\t q:" << q << "\t max:" << m << std::endl;
        }
        else if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
        {
            std::istream & istr = request.stream();
            std::unique_ptr<ReadBuffer> in_post_raw = std::make_unique<ReadBufferFromIStream>(istr);
            /*std::unique_ptr<ReadBuffer> in_post;
            String http_request_compression_method_str = request.get("Content-Encoding", "");
            if (!http_request_compression_method_str.empty())
            {
                ZlibCompressionMethod method;
                if (http_request_compression_method_str == "gzip")
                {
                    method = ZlibCompressionMethod::Gzip;
                }
                else if (http_request_compression_method_str == "deflate")
                {
                    method = ZlibCompressionMethod::Zlib;
                }
                else
                    throw Exception("Unknown Content-Encoding of HTTP request: " + http_request_compression_method_str,
                        ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
                in_post = std::make_unique<ZlibInflatingReadBuffer>(*in_post_raw, method);
            }
            else
                in_post = std::move(in_post_raw);*/

            if (in_post_raw->buffer().size() == 0)
                in_post_raw->next();
            const char * begin;
            const char * end;
            begin = in_post_raw->position();
            end = in_post_raw->buffer().end();
            String ctx = String(begin, end);
            std::cout << ctx << std::endl;
        }
    }
    catch (...)
    {
        tryLogCurrentException("parse param in Request");
    }
}
void OpentsdbSuggestRequestHandler::handleRequest(
    Poco::Net::HTTPServerRequest & request,
    Poco::Net::HTTPServerResponse & response)
{
    try
    {
        const auto & config = server.config();
        setResponseDefaultHeaders(response, config.getUInt("keep_alive_timeout", 10));
        //parse the params from uri
        HTMLForm params(request);
        std::string type = params.get("type","");
        std::string q = params.get("q","");
        std::string max = params.get("max","");
        //make a sql 
        std::string sql;
        if (type == "metrics"){
            sql="select distinct metric from opentsdb.metrics where metric like '" + q + "%' limit " + max;
        }
        else if (type == "tagv"){
            sql="select ";
        }
        else if (type == "tagk"){
            sql="select distinct tagk from opentsdb.metrics where tagk like '" + q + "%' limit " + max;
        }
        else
        {
            //add some exception
        }
        //execute the sql and return the result
        const char* end = &*sql.end();
        ParserQuery parser(end);
        ASTPtr ast;
        ast = parseQuery(parser, sql, "");
        BlockIO res;
        Context context = server.context();
        auto interpreter = InterpreterFactory::get(ast, context, QueryProcessingStage::Complete);
        res = interpreter->execute();
        
        WriteBufferPtr out_buf = std::make_shared<WriteBufferFromHTTPServerResponse>(
            request, response, 10);
        BlockOutputStreamPtr out = context.getOutputFormat("OpentsdbSuggest", *out_buf, res.in->getHeader());
        //BlockOutputStreamPtr out = context.getOutputFormat(context.getDefaultFormat(), *out_buf, res.in->getHeader());
        copyData(*res.in, *out);
    }
    catch (...)
    {
        tryLogCurrentException("OpentsdbSuggestRequestHandler");
    }
}
void OpentsdbPutRequestHandler::handleRequest(
    Poco::Net::HTTPServerRequest & request,
    Poco::Net::HTTPServerResponse & response)
{
    try
    {
        Context context = server.context();
        auto insert = std::make_shared<ASTInsertQuery>();
        insert->database = "opentsdb";
        insert->table = "all_data";
        InterpreterInsertQuery interpreter{insert, context};
        auto block_io = interpreter.execute();

        std::istream & istr = request.stream();
        ReadBufferPtr in_post_raw = std::make_unique<ReadBufferFromIStream>(istr);
        const String format_name = "OpentsdbJSON";
        size_t max_block_size = 1;

        BlockInputStreamPtr in = context.getInputFormat(format_name, *in_post_raw, block_io.out->getHeader(), max_block_size);
        copyData(*in, *block_io.out);
        const char * data = "204";
        response.sendBuffer(data, strlen(data));
    }
    catch (...)
    {
        tryLogCurrentException("OpentsdbPutRequestHandler");
    }
}
void OpentsdbQueryRequestHandler::handleRequest(
    Poco::Net::HTTPServerRequest & request,
    Poco::Net::HTTPServerResponse & response)
{
    try
    {
        const auto & config = server.config();
        setResponseDefaultHeaders(response, config.getUInt("keep_alive_timeout", 10));
        std::istream & istr = request.stream();
        std::unique_ptr<ReadBuffer> in_post_raw = std::make_unique<ReadBufferFromIStream>(istr);
        if (in_post_raw->buffer().size() == 0)
            in_post_raw->next();
        String rqst_body = String(in_post_raw->position(), in_post_raw->buffer().end());
        //parse json string
        Parser parser;
        Var result;
        result = parser.parse(rqst_body);
        //Query query(result);
        Object::Ptr object = result.extract<Object::Ptr>();
        //required
        if ( object->get("start").isEmpty() || object->get("queries").isEmpty())
        {
            const char * data = "lack of required params\n";
            response.sendBuffer(data, strlen(data));
        }
        else
        {   
            Var vstart = object->get("start");
            uint64_t start = 0;
            if (vstart.isNumeric())
                start = vstart;
            else if (vstart.isString())
            {//TODO 1h-ago or 5m-ago
                String str_start = vstart;
                sscanf(str_start.data(), "%lu", &start);
            }

            Var vend = object->get("end");
            uint64_t end = 9999999999;
            if (!vend.isEmpty() && vend.isNumeric())
                end = vend;
            else if (!vend.isEmpty() && vend.isString())
            {
                String str_end = vend;
                sscanf(str_end.data(), "%lu", &end);
            }

            Poco::JSON::Array::Ptr queries_arr = object->getArray("queries");
            for ( auto & query : *queries_arr)
            {
                Object::Ptr sub_obj_query = query.extract<Object::Ptr>();
                String agg = sub_obj_query->get("aggregator");
                String metric = sub_obj_query->get("metric");
                Object::Ptr sub_obj_tags = sub_obj_query->getObject("tags");
                for (Object::Iterator it_tag = sub_obj_tags->begin(); it_tag != sub_obj_tags->end(); it_tag++)
                {
                    std::cout << "metric:" << metric << "\taggregator:" << agg << "tag:" 
                        << it_tag->first << ":" << it_tag->second.convert<String>() << std::endl;
                }
            }
            String sql = "select * from opentsdb.all_data";
            
            ParserQuery parser(&*sql.end());
            ASTPtr ast = parseQuery(parser, sql, "");
            Context context = server.context();
            auto interpreter = InterpreterFactory::get(ast, context, QueryProcessingStage::Complete);
            BlockIO res = interpreter->execute();
        
            WriteBufferPtr out_buf = std::make_shared<WriteBufferFromHTTPServerResponse>(
                request, response, 10);
            BlockOutputStreamPtr out = context.getOutputFormat("OpentsdbJSON", *out_buf, res.in->getHeader());
            copyData(*res.in, *out);
        }
        //optional
    }
    catch (...)
    {
        tryLogCurrentException("OpentsdbQueryRequestHandler");
    }
}


}
