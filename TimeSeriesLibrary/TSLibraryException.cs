using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class holds information about errors that occur during execution of
    /// TimeSeriesLibrary methods.  When an error is handled, an instance of this 
    /// class is thrown using the C# throw statement.  This class inherits from
    /// the System.Exception base class.
    /// </summary>
    [Serializable()]
    public class TSLibraryException : Exception
    {
        /// <summary>
        /// This enum encodes what type of error occurred.
        /// </summary>
        public ErrCode.Enum ErrCode;

        
        // Following the examples provided in MSDN documentation of the System.Exception class,
        // the following constructors are overrides of the constructor in the base class.

        public TSLibraryException(ErrCode.Enum errCode) : base() { ErrCode = errCode; }
        public TSLibraryException(ErrCode.Enum errCode, string message) : base(message) { ErrCode = errCode; }
        public TSLibraryException(ErrCode.Enum errCode, string message, Exception inner) : base(message, inner) { ErrCode = errCode; }

        // A constructor is needed for serialization when an
        // exception propagates from a remoting server to the client. 
        protected TSLibraryException(System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context) { }
    }


}
