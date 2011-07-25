using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    public static class ErrCodes
    {
        // All error codes must be negative
        public enum Enum : int
        {
            None = 0,

            Connection_Not_Found = -200,
            Connection_Failed = -201,

            Could_Not_Open_Main_Table = -100,
            Record_Not_Found_Main_Table = -101,
            Missing_Fields_From_Main_Table = -102,
            Could_Not_Open_Values_Table = -103,
            Missing_Fields_From_Values_Table = -104,
            Record_Not_Found_Values_Table = -105
        }
    }
}
