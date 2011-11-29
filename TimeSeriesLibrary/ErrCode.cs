using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class is used to contain codes that are sent when a TSLibraryException
    /// object is thrown, which concisely identify the type of error that occurred.
    /// </summary>
    public static class ErrCode
    {
        /// <summary>
        /// This Enum lists the different codes that are sent when a TSLibraryException
        /// object is thrown, concisely identifying the type of error that occurred.
        /// All of the values of the Enum are negative integers.
        /// </summary>
        public enum Enum : int
        {
            None = 0,
            /// <summary>
            /// This error code can be used when the error clearly indicates that there
            /// was a coding error within TimeSeriesLibrary.
            /// </summary>
            Internal_Error = -1,

            /// <summary>
            /// This error code indicates that a TimeSeriesLibrary method was given a database
            /// connection number that is not in use.
            /// </summary>
            Connection_Not_Found = -200,

            // not used?
            Connection_Failed = -201,

            /// <summary>
            /// This error code indicates that a TimeSeriesLibrary method was given a table name
            /// which can not be opened using a SQL query.  This may indicate that the table
            /// does not exist in the database, or that the table does not have the proper fields.
            /// </summary>
            Could_Not_Open_Table = -100,
            /// <summary>
            /// This error code indicates that a TimeSeriesLibrary method was given the GUID Id
            /// number of a time series record, but the record number can not be found in the given table.
            /// </summary>
            Record_Not_Found_Table = -101,
            /// <summary>
            /// This error code indicates that a TimeSeriesLibrary method was given a SQL command
            /// or part of a SQL command, but .Net System threw a SqlException.  Most likely this
            /// indicates that the given SQL command or part of a SQL command was malformed.  The
            /// SqlException is found as the InnerException of the TSLibraryException.
            /// </summary>
            Sql_Syntax_Error = -104,
            /// <summary>
            /// This error code indicates that a TimeSeriesLibrary method was asked to read a regular
            /// time step time series, but the time series was not found to be regular.
            /// </summary>
            Record_Not_Regular = -105,
            /// <summary>
            /// This error code indicates that a TimeSeriesLibrary method was asked to read an irregular
            /// time step time series, but the time series was not found to be irregular.
            /// </summary>
            Record_Not_Irregular = -106,

            // not used?
            Array_Length_Less_Than_One = -300,
            // not used?
            End_Date_Precedes_Start_Date = -301,
            /// <summary>
            /// This error code indicates that a TimeSeriesLibrary method was asked to compute an MD5
            /// checksum using TimeStepUnit=Irregular and TimeStepQuantity!=0.  If TimeStepUnit=Irregular,
            /// then the TimeStepQuantity must equal zero in order to ensure consistency in the checksum.
            /// </summary>
            Checksum_Quantity_Nonzero = -302,
            /// <summary>
            /// This error code indicates that a TimeSeriesLibrary method was asked to compute an MD5
            /// checksum where the TimeStepCount parameter did not match the length of the input list or array.
            /// </summary>
            Checksum_Improper_Count = -303,
            /// <summary>
            /// This error code indicates that a TimeSeriesLibrary method was asked to compute an MD5
            /// checksum where the StartDate parameter did not match the first date in the input list or array.
            /// </summary>
            Checksum_Improper_StartDate = -304,
            /// <summary>
            /// This error code indicates that a TimeSeriesLibrary method was asked to compute an MD5
            /// checksum where the EndDate parameter did not match the last date in the input list or array.
            /// </summary>
            Checksum_Improper_EndDate = -305,

            /// <summary>
            /// This error code indicates that the XML file was missing one or more elements that are
            /// required in order for TimeSeriesLibrary to create a proper database record.
            /// </summary>
            Xml_File_Incomplete = -401,
            /// <summary>
            /// This error indicates that TimeSeriesLibrary was unable to find any time series in the
            /// XML file.  This may indicate that the file is empty, or it might indicate that the file
            /// is malformed.
            /// </summary>
            Xml_File_Empty = -401,
            /// <summary>
            /// This error is thrown if the TimeSeriesLibrary method is given both an XML file name and the
            /// text of an XML file to read.  The method should be given either an XML file name, or the
            /// text of an XML file, but not both.
            /// </summary>
            Xml_Memory_File_Exclusion = -402,
            /// <summary>
            /// This error indicates that the TimeSeriesLibrary method was asked to store the contents of
            /// an XML file to the database, but the class that contains the method was not properly initialized
            /// with the information of how to store the time series to the database.  This may be the result
            /// of initializing the class with the wrong constructor.
            /// </summary>
            Xml_Connection_Not_Initialized = -403,
            /// <summary>
            /// This error indicates that an XmlException object was thrown sometime during the attempt to 
            /// read from the XML file.  Most likely this indicates there was an error in how the XML file
            /// is formed.  The XmlException is included as an inner exception to the TSLibraryException.
            /// </summary>
            Xml_File_Malformed = -404
        }
    }
}
