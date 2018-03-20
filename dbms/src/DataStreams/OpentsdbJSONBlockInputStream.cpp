#include <DataStreams/OpentsdbJSONBlockInputStream.h>
#include <DataStreams/FormatFactory.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeFactory.h>

#include <IO/ReadBufferFromString.h>

#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTInsertQuery.h>

#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>

#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Struct.h>

using namespace Poco::JSON;
using namespace Poco::Dynamic;
using Poco::DynamicStruct;

namespace DB
{
OpentsdbJSONBlockInputStream::OpentsdbJSONBlockInputStream(
    ReadBuffer & istr_, 
    const Context & context_)
    :istr(istr_), context(context_)
{
}

void OpentsdbJSONBlockInputStream::processOneObj(Object::Ptr& obj, Block& block)
{
    //
    bool is_empty = block.rows()? 0 : 1;
    Context & ctx = const_cast<Context &>(context.getGlobalContext());
    ColumnWithTypeAndName column;
    MutableColumnPtr read_column;
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
    String metric = obj->getValue<String>("metric");
    ReadBufferPtr metric_buf = std::make_unique<ReadBufferFromString>(metric);

    if (!is_empty)
    {
        column = block.getByName("metric");
        read_column = column.type->createColumn();
        column.type->deserializeTextEscaped(*read_column, *metric_buf);
        const ColumnPtr src = read_column;
        column.column->insertFrom(*src, 0);
    }
    else
    {
        column.name = "metric";
        column.type = data_type_factory.get("String");
        read_column = column.type->createColumn();
        column.type->deserializeTextEscaped(*read_column, *metric_buf);
        column.column = std::move(read_column);
        block.insert(std::move(column));
    }

    char tmp[64];
    for(int i=0; i<64; i++) tmp[i] = 0;
    const uint64_t timestamp = obj->getValue<uint64_t>("timestamp");
    sprintf(tmp, "%lu", timestamp);
    ReadBufferPtr ts_buf = std::make_unique<ReadBufferFromString>(String(tmp));
    if (!is_empty)
    {
        column = block.getByName("timestamp");
        read_column = column.type->createColumn();
        column.type->deserializeTextEscaped(*read_column, *ts_buf);
        //column.column->insertFrom(*read_column, 0);
    }
    else
    {
        column.name = "timestamp";
        column.type = data_type_factory.get("UInt64");
        read_column = column.type->createColumn();
        column.type->deserializeTextEscaped(*read_column, *ts_buf);
        column.column = std::move(read_column);
        block.insert(std::move(column));
    }

    const float value = obj->getValue<float>("value");
    for(int i=0; i<64; i++) tmp[i] = 0;
    sprintf(tmp, "%f", value);
    ReadBufferPtr val_buf = std::make_unique<ReadBufferFromString>(String(tmp));
    if (!is_empty)
    {   column = block.getByName("value");
        read_column = column.type->createColumn();
        column.type->deserializeTextEscaped(*read_column, *val_buf);
        //const IColumn src = *read_column;
        //column.column->insertFrom(src, 0);
    }
    else
    {   column.name = "value";
        column.type = data_type_factory.get("Float64");
        read_column = column.type->createColumn();
        column.type->deserializeTextEscaped(*read_column, *val_buf);
        column.column = std::move(read_column);
        block.insert(std::move(column));
    }

    Object::Ptr tags = obj->getObject("tags");
    for (Object::Iterator it = tags->begin(); it != tags->end(); it++)
    {
        String tag_key = it->first;
        String sql = "select col_id from opentsdb.metrics where metric='"+metric+"' and tagk='"+tag_key+"'";
        const char* end = &*sql.end();
        ParserQuery parser(end);
        ASTPtr ast;
        ast = parseQuery(parser, sql, "");
        BlockIO res;
        auto interpreter = InterpreterFactory::get(ast, ctx, QueryProcessingStage::Complete);
        res = interpreter->execute();
        Block query = res.in->read();
        char col_name[8];
       
        if(query.rows()>0)
        {
            ColumnWithTypeAndName col_id = query.getByPosition(0);
            uint64_t id = col_id.column->getUInt(0);
            sprintf(col_name, "col%lu", id);
            std::cout << "found the kind of tag:" << tag_key << ":" << col_name  << std::endl;
        }
        else //not found then need insert
        {
            //exist the kind of metric?
            String count_sql = "select count() from opentsdb.metrics where metric='" + metric +"'";
            const char* end = &*count_sql.end();
            ParserQuery count_parser(end);
            ast = parseQuery(count_parser, count_sql, "");
            BlockIO count_res;
       
            auto interpreter = InterpreterFactory::get(ast, ctx, QueryProcessingStage::Complete);
            count_res = interpreter->execute();
            Block count = count_res.in->read();
            uint64_t cols = count.getByPosition(0).column->getUInt(0);
            //finish lookup and get the col_id
            std::cout << "new col id:" << cols << std::endl;
            char cols_str[8];
            for(int i=0; i<8; i++) cols_str[i]=0;
            sprintf(cols_str, "%lu", cols);
            String insert_csv = "\""+metric+"\", \"" + tag_key + "\", " + cols_str;
            auto insert_buf = std::make_unique<ReadBufferFromString>(insert_csv);
            auto insert = std::make_shared<ASTInsertQuery>();
            insert->database = "opentsdb";
            insert->table = "metrics";
            InterpreterInsertQuery interpreter_insert{insert, ctx};
            auto block_io = interpreter_insert.execute();
            const String format_name = "CSV";
            auto in = FormatFactory().getInput(format_name, *insert_buf, block_io.out->getHeader(), context, 1);
            copyData(*in, *block_io.out);
            sprintf(col_name, "col%lu", cols);
            std::cout << "col_name:" << col_name << std::endl;
            std::cout << "not found tag_key:" << tag_key << " new insert col:" << col_name << std::endl;
        }
        //size_t index = block.rows();
        if(is_empty){
            column.name = col_name;
            column.type = data_type_factory.get("String");
            read_column = column.type->createColumn();
            ReadBufferPtr col_buf = std::make_shared<ReadBufferFromString>(it->second.convert<String>());
            column.type->deserializeTextEscaped(*read_column, *col_buf);
            column.column = std::move(read_column);
            block.insert(std::move(column));
        }
        else
        {
            //column = block.getByName(col_name);
            //if(column)
        }
    }
}

Block OpentsdbJSONBlockInputStream::readImpl()
{
    Block res;
    if(istr.eof())
        return res;
    //from istr ReadBuffer to String
    String json(istr.position(), istr.buffer().end());
    istr.position() = istr.buffer().end();
    std::cout << json << std::endl;
    Parser parser;
    Var result;
    result = parser.parse(json);

    if(result.isArray()){
        Poco::JSON::Array::Ptr arr = result.extract<Poco::JSON::Array::Ptr>();
        std::size_t size = arr->size();
        for(std::size_t i = 0; i < size; i++)
        {
            Object::Ptr object = arr->getObject(i);
            processOneObj(object, res);
        }
    }
    else if (result.type() == typeid(Object::Ptr))
    {
        Object::Ptr object = result.extract<Object::Ptr>();
        processOneObj(object, res);
    }

    return res;
}
}
