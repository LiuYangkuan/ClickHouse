#pragma once

#include <iostream>
#include <sstream>
#include <unordered_set>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ArenaAllocator.h>
#include <ext/range.h>
#include <bitset>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

struct AggregateFunctionTSgroupSumData
{
    using Allocator = MixedArenaAllocator<4096>;
    using Array = PODArray<UInt64, 64, Allocator>;
    using Values = PODArray<Float64, 64, Allocator>;

    Array uid, ts;
    Values agg_value;

    void add(UInt64 t, Float64)
    {
        ts.push_back(t);
    }

    size_t size()
    {
        return ts.size();
    }
    void merge(const AggregateFunctionTSgroupSumData & other)
    {
        ts.insert(other.ts.begin(), other.ts.end());
    }

    void serialize(WriteBuffer & buf) const
    {
        size_t size = ts.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(ts.data()), size * 8);
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size = 0;
        readVarUInt(size, buf);
	buf.read(reinterpret_cast<char *>(ts.data()), size * sizeof(ts[0]));
    }
};

/**
  * The max size of events is 32, that's enough for retention analytics
  *
  * Usage:
  * - retention(cond1, cond2, cond3, ....)
  * - returns [cond1_flag, cond1_flag && cond2_flag, cond1_flag && cond3_flag, ...]
  */
class AggregateFunctionTSgroupSum final
        : public IAggregateFunctionDataHelper<AggregateFunctionTSgroupSumData, AggregateFunctionTSgroupSum>
{
private:

public:
    String getName() const override
    {
        return "TSgroupSum";
    }

    AggregateFunctionTSgroupSum(const DataTypes &) // arguments)
    {
/*        if(!isUInt64(arguments[0].get())
            throw Exception{"Illegal type " + arguments[0].get()->getName() + " of argument 1 of aggregate function "
                        + getName() + ", must be UInt64",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if(!isUInt64(arguments[1].get())
            throw Exception{"Illegal type " + arguments[1].get()->getName() + " of argument 2 of aggregate function "
                        + getName() + ", must be UInt64",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if(!isFloat64(arguments[2].get())
            throw Exception{"Illegal type " + arguments[2].get()->getName() + " of argument 3 of aggregate function "
                        + getName() + ", must be Float64",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
  */  }


    DataTypePtr getReturnType() const override
    {
	//auto datatypes = std::vector<DataTypePtr>();
        //datatypes.push_back(std::make_shared<DataTypeUInt64>());
        //datatypes.push_back(std::make_shared<DataTypeFloat64>());

        //return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(datatypes));
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        auto ts = static_cast<const ColumnVector<UInt64> *>(columns[1])->getData()[row_num];
        auto val = static_cast<const ColumnVector<UInt64> *>(columns[2])->getData()[row_num];
        if (ts && val)
        {
            this->data(place).add(ts, val);
        }
        /*for (const auto i : ext::range(0, events_size))
        {
            auto event = static_cast<const ColumnVector<UInt8> *>(columns[i])->getData()[row_num];
            if (event)
            {
                this->data(place).add(i);
                break;
            }
        }*/
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto & data_to = static_cast<ColumnUInt8 &>(static_cast<ColumnArray &>(to).getData()).getData();
        auto & offsets_to = static_cast<ColumnArray &>(to).getOffsets();

        size_t curr_size = this->data(place).size();
        ColumnArray::Offset current_offset = data_to.size();
        data_to.resize(current_offset + curr_size);

        for (size_t i = 0; i < curr_size; ++i)
        {
            data_to[current_offset] = this->data(place).ts[i];
            ++current_offset;
        }

        offsets_to.push_back(current_offset);
    }

    const char * getHeaderFilePath() const override
    {
        return __FILE__;
    }
};

}
