#pragma once

#include <iostream>
#include <sstream>
#include <map>
#include <unordered_set>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
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

    struct AggregateFunctionTSgroupRateSumData
    {
        struct Points {
            using Dps = std::map<UInt64, Float64>;
            Dps dps;
            Dps::const_iterator curr_it;
            void reset() { curr_it = dps.begin(); }
            UInt64 getTS() { return curr_it == dps.end()? 0 : curr_it->first; }
            UInt64 popTS() { UInt64 ret = 0; ++curr_it; return ret; }
            Float64 getIPval(UInt64 t, bool rate = false) {//before use it, check getTS() > 0
                Dps::const_iterator c_it = curr_it;
    	        UInt64 t1 = c_it->first;
                Float64 v1 = c_it->second;
                UInt64 t2 ;
                Float64 v2 ;
                if (rate) {
                    if (t >= getTS()) {
                        if (++c_it == dps.end()) return 0.0;//current is last
                        t2 = c_it->first;
                        v2 = c_it->second;
                    } else {
                        if (c_it == dps.begin()) return 0.0;
                        t2 = (--c_it)->first;
                        v2 = (--c_it)->second;
                    }
                    return (v1-v2)/(Float64)(t1-t2);
                } else {
                    if (t == getTS()) {
                        return c_it->second;
                    } else if (t > getTS()) {
                        if (++c_it == dps.end()) return 0.0;
                        t2 = c_it->first;
                        v2 = c_it->second;
                    } else {
                        if (c_it == dps.begin()) return 0.0;
                        t2 = (--c_it)->first;
                        v2 = (--c_it)->second;
                    }
                    return v2 + (v1-v2)*((Float64)(t-t2))/(Float64)(t1-t2);
                }
            }
        };
        using Series = std::map<UInt64, Points>;
        using Allocator = MixedArenaAllocator<4096>;
        using Array = PODArray<UInt64, 64, Allocator>;
        using Values = PODArray<Float64, 64, Allocator>;

        Series ss;
        Array ts;
        Values agg_value;
        /*void fillTsAndVal(ColumnVector<UInt64> & ts2, ColumnVector<Float64> & value2, bool rate = false) const {
            bool again = true;
            do {
                again = false;
                Series::const_iterator it_tmp;
                UInt64 min_ts = 999999999999999999, last_ts = 0;
                Float64 agg_val = 0.0;
                //find min_ts
                for (Series::const_iterator it = ss.begin(); it != ss.end(); it++) {
                    it->second.reset();
                    if (it->second.getTS() > 0 && it->second.getTS() < min_ts) {
                        min_ts = it->second.getTS();
                        it_tmp = it;
                    }
                }
                if (min_ts != last_ts) {
                    ts2.insertValue(min_ts);
                    for (Series::const_iterator it = ss.begin(); it != ss.end(); it++) {
                        agg_val += it->second.getIPval(min_ts, rate);
                    }
                    val2.insertValue(agg_val);
                }
                last_ts = min_ts;
                it_tmp->second.popTS();
                if (it_tmp->second.getTS() > 0) again = true;
            } while (again);
            
        }*/
        Float64 getAggValue(UInt64 t) const {
            Float64 ret = 0.0;
            //printf("%lu\t", t);
            for (Series::const_iterator it = ss.begin(); it != ss.end(); it++) {
                Points::Dps::const_iterator pit = it->second.dps.lower_bound(t);
                if (pit == it->second.dps.end()) continue;
                UInt64 t1 = pit->first;
                Float64 v1 = pit->second;
                if (pit == it->second.dps.begin())
                   continue;
                else
                   pit--;
                UInt64 t2 = pit->first;
                Float64 v2 = pit->second;
		Float64 rate = (v1-v2)/(Float64)(t1-t2);
                ret += rate;
                //printf("new value:%lu,%lf,%lu,%lf\t, rate:%.10lf\t", t1, v1, t2, v2, rate);
            }
            //printf("%.10lf\n", ret);
            return ret;
        }

        void add(UInt64 uid, UInt64 t, Float64 v, Arena * arena)
        {//suppose t is coming asc
            if (ss.count(uid) == 0){//time series not exist, insert new one
                Points tmp;
                tmp.dps.emplace(t, v);
                ss.emplace(uid, tmp);
            } else {
                Series::iterator it_ss = ss.find(uid);
                it_ss->second.dps.emplace(t, v);
            }
            if (ts.size() > 0 && t < ts.back())
                printf("Error because of timestamp decease! \n");
            if (ts.size() > 0 && t == ts.back()) {
                return;
            }
            ts.push_back(t, arena);
            //TODO: need aggregate value real time back-ward
            //agg_value.push_back(0.0, arena);
        }

        void merge(const AggregateFunctionTSgroupRateSumData & other, Arena * arena)
        {
            printf("call merge\n");
            if (ts.back() > other.ts.front()) printf("Error because of timestamp decease in merge! \n");
            ts.insert(other.ts.begin(), other.ts.end(), arena);
            agg_value.insert(other.agg_value.begin(), other.agg_value.end(), arena);
        }

        void serialize(WriteBuffer & buf) const
        {
            printf("call ser\n");
            size_t size = ts.size();
            writeVarUInt(size, buf);
            buf.write(reinterpret_cast<const char *>(ts.data()), size * 8);

            writeVarUInt(size, buf);
            buf.write(reinterpret_cast<const char *>(agg_value.data()), size * 8);
        }

        void deserialize(ReadBuffer & buf, Arena *arena)
        {
            printf("call deser\n");
            size_t size = 0;
            readVarUInt(size, buf);
            ts.resize(size, arena);
            buf.read(reinterpret_cast<char *>(ts.data()), size * sizeof(ts[0]));

            readVarUInt(size, buf);
            agg_value.resize(size, arena);
            buf.read(reinterpret_cast<char *>(agg_value.data()), size * sizeof(agg_value[0]));
        }
    };

    class AggregateFunctionTSgroupRateSum final
            : public IAggregateFunctionDataHelper<AggregateFunctionTSgroupRateSumData, AggregateFunctionTSgroupRateSum>
    {
    private:

    public:
        String getName() const override
        {
            return "TSgroupSum";
        }

        AggregateFunctionTSgroupRateSum(const DataTypes & arguments)
        {
            if(!WhichDataType(arguments[0].get()).isUInt64())
                throw Exception{"Illegal type " + arguments[0].get()->getName() + " of argument 1 of aggregate function "
                                + getName() + ", must be UInt64",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            if(!WhichDataType(arguments[1].get()).isUInt64())
                throw Exception{"Illegal type " + arguments[1].get()->getName() + " of argument 2 of aggregate function "
                                + getName() + ", must be UInt64",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            if(!WhichDataType(arguments[2].get()).isFloat64())
                throw Exception{"Illegal type " + arguments[2].get()->getName() + " of argument 3 of aggregate function "
                                + getName() + ", must be Float64",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }


        DataTypePtr getReturnType() const override
        {
            auto datatypes = std::vector<DataTypePtr>();
            datatypes.push_back(std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()));
            datatypes.push_back(std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>()));

            return std::make_shared<DataTypeTuple>(datatypes);
        }

        void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num, Arena *arena) const override
        {
            auto uid = static_cast<const ColumnVector<UInt64> *>(columns[0])->getData()[row_num];
            auto ts = static_cast<const ColumnVector<UInt64> *>(columns[1])->getData()[row_num];
            auto val = static_cast<const ColumnVector<Float64> *>(columns[2])->getData()[row_num];
            if (ts && val)
            {
                this->data(place).add(uid, ts, val, arena);
            }
        }

        void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
        {
            this->data(place).merge(this->data(rhs), arena);
        }

        void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
        {
            this->data(place).serialize(buf);
        }

        void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *arena) const override
        {
            this->data(place).deserialize(buf, arena);
        }

        void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
        {
            ColumnArray & ts_arr_to = static_cast<ColumnArray &>(static_cast<ColumnTuple &>(to).getColumn(0));
            ColumnArray & val_arr_to = static_cast<ColumnArray &>(static_cast<ColumnTuple &>(to).getColumn(1));
            ColumnArray::Offsets & ts_off_to = ts_arr_to.getOffsets();
            ColumnArray::Offsets & val_off_to = val_arr_to.getOffsets();

            ts_off_to.push_back((ts_off_to.size() == 0 ? 0 : ts_off_to.back()) + this->data(place).ts.size());
            //ColumnVector<UInt64> & ts_to = static_cast<ColumnVector<UInt64> &>(ts_arr_to.getData());
            typename ColumnVector<UInt64>::Container & ts_to = static_cast<ColumnVector<UInt64> &>(ts_arr_to.getData()).getData();
            ts_to.insert(this->data(place).ts.begin(), this->data(place).ts.end());

            val_off_to.push_back((val_off_to.size() == 0 ? 0 : val_off_to.back()) + this->data(place).ts.size());
            ColumnVector<Float64> & val_to = static_cast<ColumnVector<Float64> &>(val_arr_to.getData());
            //this->data(place).fillTsAndVal(ts_to, val_to);
            // fill agg values
            size_t size = this->data(place).ts.size();
            for (size_t i = 0; i < size; i++) {
                UInt64 t = this->data(place).ts[i];
                val_to.insertValue(this->data(place).getAggValue(t));
            }
            //val_to.insert(this->data(place).agg_value.begin(), this->data(place).agg_value.end());
        }

        bool allocatesMemoryInArena() const override
        {
            return true;
        }

        const char * getHeaderFilePath() const override
        {
            return __FILE__;
        }
    };

}
