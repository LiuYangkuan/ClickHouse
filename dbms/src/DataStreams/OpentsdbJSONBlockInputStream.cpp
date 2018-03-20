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
//if not exist then add
uint64_t OpentsdbJSONBlockInputStream::lookupTagId(String& metric, String& tag_key)
{
    Context & ctx = const_cast<Context &>(context.getGlobalContext());
    String sql = "select col_id from opentsdb.metrics where metric='"+metric+"' and tagk='"+tag_key+"'";
    //const char* end = &*sql.end();
    ParserQuery parser(&*sql.end());
    ASTPtr ast;
    ast = parseQuery(parser, sql, "");
    BlockIO res;
    auto interpreter = InterpreterFactory::get(ast, ctx, QueryProcessingStage::Complete);
    res = interpreter->execute();
    Block query = res.in->read();
       
    if(query.rows()>0)
        return query.getByPosition(0).column->getUInt(0);
    else
    {
        String count_sql = "select count() from opentsdb.metrics where metric='" + metric +"'";
        //const char* end = &*count_sql.end();
        ParserQuery count_parser(&*count_sql.end());
        ast = parseQuery(count_parser, count_sql, "");
        BlockIO count_res;
       
        auto interpreter = InterpreterFactory::get(ast, ctx, QueryProcessingStage::Complete);
        count_res = interpreter->execute();
        Block count = count_res.in->read();
        uint64_t cols = count.getByPosition(0).column->getUInt(0);
        //insert a new record
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
        return cols;
    }
}
//add every subobject in JSON into Field Array
void OpentsdbJSONBlockInputStream::processOneObj(Object::Ptr& obj)
{
    String metric = obj->getValue<String>("metric");
    metric_farray.push_back(metric);
    ts_farray.push_back(UInt64(obj->getValue<uint64_t>("timestamp")));
    val_farray.push_back(Float64(obj->getValue<float>("value")));
    for (uint64_t i=0; i<COLS; i++)
    {
        tagv_farray[i].push_back(String());
    }
    Object::Ptr tags = obj->getObject("tags");
    for (Object::Iterator it = tags->begin(); it != tags->end(); it++)
    {
        String tag_key = it->first;
        uint64_t id = lookupTagId(metric, tag_key);
        tagv_farray[id].pop_back(); 
        tagv_farray[id].push_back(it->second.convert<String>());
    }
}
void OpentsdbJSONBlockInputStream::addColumns(Block& block)
{
    ColumnWithTypeAndName column;
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
    MutableColumnPtr read_column;

    column.name = "metric";
    column.type = data_type_factory.get("String");
    read_column = column.type->createColumn();
    for (auto & field : metric_farray)
    {
        read_column->insert(field);
    }
    column.column = std::move(read_column);
    block.insert(std::move(column));

    column.name = "timestamp";
    column.type = data_type_factory.get("UInt64");
    read_column = column.type->createColumn();
    for (auto & field : ts_farray)
    {   
        read_column->insert(field);
    }
    column.column = std::move(read_column);
    block.insert(std::move(column));

    column.name = "value";
    column.type = data_type_factory.get("Float64");
    read_column = column.type->createColumn();
    for (auto & field : val_farray)
    {   
        read_column->insert(field);
    }
    column.column = std::move(read_column);
    block.insert(std::move(column));
    for (size_t i=0; i<COLS; i++)
    {
        char tmp[4];
        sprintf(tmp, "%lu", i);
        column.name = "col" + String(tmp);
        column.type = data_type_factory.get("String");
        read_column = column.type->createColumn();
        for (auto & field : tagv_farray[i])
        {
            read_column->insert(field);
        }
        column.column = std::move(read_column);
        block.insert(std::move(column));
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
            processOneObj(object);
        }
    }
    else if (result.type() == typeid(Object::Ptr))
    {
        Object::Ptr object = result.extract<Object::Ptr>();
        processOneObj(object);
    }
    addColumns(res);
    return res;
}
}
