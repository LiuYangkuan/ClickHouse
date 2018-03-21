#pragma once

#include <Core/Block.h>
#include <DataStreams/IRowOutputStream.h>


namespace DB
{

class WriteBuffer;


/** The stream for outputting data in csv format.
  * Does not conform with https://tools.ietf.org/html/rfc4180 because it uses LF, not CR LF.
  */
class OpentsdbSuggestRowOutputStream : public IRowOutputStream
{
public:
    /** with_names - output in the first line a header with column names
      * with_types - output in the next line header with the names of the types
      */
    OpentsdbSuggestRowOutputStream(WriteBuffer & ostr_);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowEndDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;

    void flush() override;

    /// https://www.iana.org/assignments/media-types/text/csv

protected:
    WriteBuffer & ostr;
};

}

