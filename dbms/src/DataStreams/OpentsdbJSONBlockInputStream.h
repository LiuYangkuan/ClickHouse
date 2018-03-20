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
class OpentsdbJSONBlockInputStream : public IProfilingBlockInputStream
{
public:
    OpentsdbJSONBlockInputStream(ReadBuffer & istr_, const Context & context_);
    Block getHeader() const override { return Block(); }
    //Block read() override { return Block(); }
    String getName() const override { return "OpentsdbJSON"; }
    void processOneObj(Object::Ptr& obj, Block& block);
protected:
    Block readImpl() override;
private:
    ReadBuffer & istr;
    const Context & context;
    Array metric_farray;
    Array ts_farray;
    Array val_farray;
    Array tagv_farray[21];
};
}
