#pragma once
#include <Core/Field.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Struct.h>

using namespace Poco::JSON;
using namespace Poco::Dynamic;
using Poco::DynamicStruct;

namespace DB
{
const size_t COLS = 21;
class OpentsdbJSONBlockInputStream : public IProfilingBlockInputStream
{
public:
    OpentsdbJSONBlockInputStream(ReadBuffer & istr_, const Context & context_);
    Block getHeader() const override { return Block(); }
    //Block read() override { return Block(); }
    String getName() const override { return "OpentsdbJSON"; }
    void addColumns(Block& block);//processOneObj(Object::Ptr& obj, Block& block);
    void processOneObj(Object::Ptr& obj);
    uint64_t lookupTagId(String& metric, String& tag_key);
protected:
    Block readImpl() override;
private:
    ReadBuffer & istr;
    const Context & context;
    Array metric_farray;
    Array ts_farray;
    Array val_farray;
    Array tagv_farray[COLS];
};
}
