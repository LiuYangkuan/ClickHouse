#include <DataStreams/OpentsdbSuggestRowOutputStream.h>

#include <IO/WriteHelpers.h>


namespace DB
{


OpentsdbSuggestRowOutputStream::OpentsdbSuggestRowOutputStream(WriteBuffer & ostr_)
    : ostr(ostr_)
{
}


void OpentsdbSuggestRowOutputStream::flush()
{
    ostr.next();
}


void OpentsdbSuggestRowOutputStream::writePrefix()
{
    writeChar('[', ostr); 
    writeChar('\n', ostr); 
}


void OpentsdbSuggestRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    writeChar(' ', ostr);
    writeChar(' ', ostr);
    type.serializeTextCSV(column, row_num, ostr);
    writeChar(',', ostr);
}


void OpentsdbSuggestRowOutputStream::writeFieldDelimiter()
{
    writeChar(',', ostr);
}


void OpentsdbSuggestRowOutputStream::writeRowEndDelimiter()
{
    writeChar('\n', ostr);
}


void OpentsdbSuggestRowOutputStream::writeSuffix()
{
    ostr.position() -= 2;
    writeChar('\n', ostr);
    writeChar(']', ostr); 
}


}
