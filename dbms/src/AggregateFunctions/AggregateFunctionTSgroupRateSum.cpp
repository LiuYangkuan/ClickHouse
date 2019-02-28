#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTSgroupRateSum.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionTSgroupRateSum(const std::string & name, const DataTypes & arguments, const Array & params)
{
    assertNoParameters(name, params);

    if (arguments.size() < 3)
        throw Exception("Not enough event arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionTSgroupRateSum>(arguments);
}

}

void registerAggregateFunctionTSgroupRateSum(AggregateFunctionFactory & factory)
{
    factory.registerFunction("TSgroupRateSum", createAggregateFunctionTSgroupRateSum, AggregateFunctionFactory::CaseInsensitive);
}

}
