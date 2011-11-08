using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    [Serializable()]
    public class TSLibraryException : Exception
    {
        public TSLibraryException(ErrCode.Enum errCode) : base() { }
        public TSLibraryException(ErrCode.Enum errCode, string message) : base(message) { }
        public TSLibraryException(ErrCode.Enum errCode, string message, Exception inner) : base(message, inner) { }

        // A constructor is needed for serialization when an
        // exception propagates from a remoting server to the client. 
        protected TSLibraryException(System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context) { }
    }


}
