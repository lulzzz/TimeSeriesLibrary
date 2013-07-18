using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class houses the meta-parameters that TimeSeriesLibrary is responsible
    /// for writing to the database along with the BLOB (this class contains the meta
    /// parameters but not the BLOB itself).
    /// </summary>
    public class TSParameters
    {
        /// <summary>
        /// code for the units that measure the regular time step (e.g. hour, day, month)
        /// </summary>
        public TSDateCalculator.TimeStepUnitCode TimeStepUnit;
        /// <summary>
        /// number of units per time step (e.g. Quantity=6 for 6-hour time steps)
        /// </summary>
        public short TimeStepQuantity;
        /// <summary>
        /// Date of the first time step stored in the database
        /// </summary>
        public DateTime BlobStartDate;
        /// <summary>
        /// The compression code that indicates what compression algorithm is used to compress the BLOB
        /// </summary>
        public int CompressionCode;


        /// <summary>
        /// TimeSeriesLibrary is responsible for ensuring that a certain set of meta-parameters (which
        /// are saved as database fields) are coordinated with the BLOB of timeseries data. This method
        /// records all of the meta-parameters of a regular timeseries into the fields of this TSParameters
        /// object, using the input parameters given to the method.
        /// </summary>
        public void SetParametersRegular(
                    TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                    int timeStepCount, DateTime blobStartDate, int compressionCode)
        {
            // Most of the parameters are straightforward
            TimeStepUnit = timeStepUnit;
            TimeStepQuantity = timeStepQuantity;
            BlobStartDate = blobStartDate;
            CompressionCode = compressionCode;
        }

        /// <summary>
        /// TimeSeriesLibrary is responsible for ensuring that a certain set of meta-parameters (which
        /// are saved as database fields) are coordinated with the BLOB of timeseries data. This method
        /// records all of the meta-parameters of an irregular timeseries into the fields of this 
        /// TSParameters object, using the input parameters given to the method.
        /// </summary>
        public void SetParametersIrregular(
                    int timeStepCount, DateTime blobStartDate, DateTime blobEndDate, int compressionCode)
        {
            // Most of the parameters are straightforward
            TimeStepUnit = TSDateCalculator.TimeStepUnitCode.Irregular;
            TimeStepQuantity = 0;
            BlobStartDate = blobStartDate;
            CompressionCode = compressionCode;
        }
    
    
    }


}
