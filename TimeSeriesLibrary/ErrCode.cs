using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    public static class ErrCode
    {
        // All error codes must be negative
        public enum Enum : int
        {
            None = 0,
            Internal_Error = -1,

            Requested_Dates_Outside_Of_Range = -5,

            Connection_Not_Found = -200,
            Connection_Failed = -201,

            Could_Not_Open_Table = -100,
            Record_Not_Found_Table = -101,
            Missing_Fields_From_Table = -102,
            Could_Not_Open_Values_Table = -103,
            Sql_Syntax_Error = -104,
            Record_Not_Regular = -105,
            Record_Not_Irregular = -106,

            Array_Length_Less_Than_One = -300,
            End_Date_Precedes_Start_Date = -301,

            Xml_File_Incomplete = -401,
            Xml_File_Empty = -401,
            Xml_Memory_File_Exclusion = -402,
            Xml_Connection_Not_Initialized = -403,
            Xml_File_Malformed = -404
        }
    }
}
