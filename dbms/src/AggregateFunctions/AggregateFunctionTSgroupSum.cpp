#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTSgroupSum.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionTSgroupSum(const std::string & name, const DataTypes & arguments, const Array & params)
{
    assertNoParameters(name, params);

    if (arguments.size() < 3)
        throw Exception("Not enough event arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionTSgroupSum>(arguments);
}

}

void registerAggregateFunctionTSgroupSum(AggregateFunctionFactory & factory)
{
    factory.registerFunction("TSgroupSum", createAggregateFunctionTSgroupSum, AggregateFunctionFactory::CaseInsensitive);
}

}
