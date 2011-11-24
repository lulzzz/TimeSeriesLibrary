using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    [Serializable()]
    public class TSLibraryException : Exception
    {
        public ErrCode.Enum ErrCode;

        public TSLibraryException(ErrCode.Enum errCode) : base() { ErrCode = errCode; }
        public TSLibraryException(ErrCode.Enum errCode, string message) : base(message) { ErrCode = errCode; }
        public TSLibraryException(ErrCode.Enum errCode, string message, Exception inner) : base(message, inner) { ErrCode = errCode; }

        // A constructor is needed for serialization when an
        // exception propagates from a remoting server to the client. 
        protected TSLibraryException(System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context) { }
    }


}
