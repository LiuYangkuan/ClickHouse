#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// arrayEnumerate(arr) - Returns the array [1,2,3,..., length(arr)]
class FunctionArrayInterPolation : public IFunction
{
public:
    static constexpr auto name = "arrayInterPolation";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionArrayInterPolation>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception("First argument for function " + getName() + " must be an array but it has type "
                + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (array_type->getNestedType()->getName() != "UInt64")
            throw Exception("First argument for function " + getName() + " must be an array and its nest type is UInt64 but it has type "
                + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!array_type)
            throw Exception("Secone argument for function " + getName() + " must be an array but it has type "
                + arguments[1]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (array_type->getNestedType()->getName() != "Float64")
            throw Exception("Second argument for function " + getName() + " must be an array and its nest type is Float64 but it has type "
                + arguments[1]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypeUInt64 * down_sample = checkAndGetDataType<DataTypeUInt64>(arguments[2].get());
        if (!down_sample)
            throw Exception("Third argument for function " + getName() + " must be an UInt64 but it has type "
                + arguments[2]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto datatypes = std::vector<DataTypePtr>();
        datatypes.push_back(std::make_shared<DataTypeUInt64>());
        datatypes.push_back(std::make_shared<DataTypeFloat64>());
        datatypes.push_back(std::make_shared<DataTypeUInt8>());
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(datatypes));
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        const ColumnArray * array_ts = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());
        const ColumnArray * array_val = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[1]).column.get());
        const ColumnConst *down_sample1 = checkAndGetColumn<ColumnConst>(block.getByPosition(arguments[2]).column.get());
        const ColumnUInt64 *down_sample2 = checkAndGetColumn<ColumnUInt64>(block.getByPosition(arguments[2]).column.get());
        UInt64 downsample = 0;

        if(!down_sample1 && down_sample2)
            downsample = down_sample2->getUInt(0);
        else
            downsample = down_sample1->getUInt(0);

        if (array_ts && array_val && downsample)
        {
            auto ret_offset = ColumnUInt64::create();
            ColumnUInt64::Container & ret_off = ret_offset->getData();

            auto res_nested_ts = ColumnUInt64::create();
            ColumnUInt64::Container & res_ts = res_nested_ts->getData();
            auto res_nested_val = ColumnFloat64::create();
            ColumnFloat64::Container & res_val = res_nested_val->getData();
            auto res_nested_flag = ColumnUInt8::create();
            ColumnUInt8::Container & res_flag = res_nested_flag->getData();

            const ColumnArray::Offsets & offsets = array_ts->getOffsets();
            ColumnArray::Offset prev_off = 0;
            UInt64 ts_min=9999999999999, ts_max=0;
            for (ColumnArray::Offset i = 0; i < offsets.size(); ++i)
            {
                ColumnArray::Offset off = offsets[i];
                for (ColumnArray::Offset j = prev_off; j < off; ++j) {
                    if (ts_min > array_ts->getData()[j].get<UInt64>()) ts_min = array_ts->getData()[j].get<UInt64>();
                    if (ts_max < array_ts->getData()[j].get<UInt64>()) ts_max = array_ts->getData()[j].get<UInt64>();
                }
            }
            size_t enlarge = (ts_max - ts_min) / downsample + 1;
            res_ts.reserve(enlarge * offsets.size());
            res_val.reserve(enlarge * offsets.size());
            res_flag.reserve(enlarge * offsets.size());
            ret_off.resize(offsets.size());

            prev_off = 0;
            size_t idx = 0;
            for (ColumnArray::Offset i = 0; i < offsets.size(); ++i)
            {
                ColumnArray::Offset off = offsets[i];
                UInt64 curr_ts = array_ts->getData()[prev_off].get<UInt64>();
                size_t item_count = 0;
                for (ColumnArray::Offset j = prev_off; j < off; ++j){
                    UInt64 ts = array_ts->getData()[j].get<UInt64>();
                    Float64 inc = 0.0, delta = 0.0;
                    UInt64 step = 0;

                    if (j > prev_off) {
                        inc = array_val->getData()[j].get<Float64>() - array_val->getData()[j-1].get<Float64>();
                        step = array_ts->getData()[j].get<UInt64>() - array_ts->getData()[j-1].get<UInt64>();
                        delta = inc / Float64(step);
                    }
                    while (curr_ts <= ts) {
                        if (curr_ts < ts){
                            res_flag.push_back(0);
                            res_val.push_back(res_val[idx-1] + delta * downsample);
                        }
                        else {
                            res_flag.push_back(1);
                            res_val.push_back(array_val->getData()[j].get<Float64>());
                        }
                        res_ts.push_back(curr_ts);
                        curr_ts += downsample;
                        idx++;
                        item_count++;
                    }
                }
                prev_off = off;
                ret_off[i] = item_count;
                if( i > 0 )
                    ret_off[i] += ret_off[i-1];
            }

            Columns tuple_item(3);
            tuple_item[0] = res_nested_ts->assumeMutable();
            tuple_item[1] = res_nested_val->assumeMutable();
            tuple_item[2] = res_nested_flag->assumeMutable();
            auto tuple_pair = ColumnTuple::create(tuple_item);

            block.getByPosition(result).column = ColumnArray::create(std::move(tuple_pair), ret_offset->assumeMutable());
        }
        else
        {
            if (!array_ts)
                throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                        + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
            if (!array_val)
               throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
                        + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
            if (!down_sample1)
                throw Exception("Illegal column " + block.getByPosition(arguments[2]).column->getName()
                        + " of third argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
            if (!down_sample2)
                throw Exception("Illegal column " + block.getByPosition(arguments[2]).column->getName()
                        + " of third argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};


void registerFunctionArrayInterPolation(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayInterPolation>();
}

}


