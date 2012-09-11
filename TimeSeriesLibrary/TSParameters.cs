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
        // code for the units that measure the regular time step (e.g. hour, day, month)
        public TSDateCalculator.TimeStepUnitCode TimeStepUnit;
        // number of units per time step (e.g. Quantity=6 for 6-hour time steps)
        public short TimeStepQuantity;
        // Date of the first time step stored in the database
        public DateTime BlobStartDate;
        // Date of the last time step stored in the database
        public DateTime BlobEndDate;
        // The number of time steps stored in the database
        public int TimeStepCount;


        /// <summary>
        /// TimeSeriesLibrary is responsible for ensuring that a certain set of meta-parameters (which
        /// are saved as database fields) are coordinated with the BLOB of timeseries data. This method
        /// records all of the meta-parameters of a regular timeseries into the fields of this TSParameters
        /// object, using the input parameters given to the method.
        /// </summary>
        public void SetParametersRegular(
                    TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                    int timeStepCount, DateTime blobStartDate)
        {
            // Most of the parameters are straightforward
            TimeStepUnit = timeStepUnit;
            TimeStepQuantity = timeStepQuantity;
            TimeStepCount = timeStepCount;
            BlobStartDate = blobStartDate;
            // We must calculate the date of the last time step
            BlobEndDate = TSDateCalculator.IncrementDate(BlobStartDate, TimeStepUnit, TimeStepQuantity, TimeStepCount - 1);
        }

        /// <summary>
        /// TimeSeriesLibrary is responsible for ensuring that a certain set of meta-parameters (which
        /// are saved as database fields) are coordinated with the BLOB of timeseries data. This method
        /// records all of the meta-parameters of an irregular timeseries into the fields of this 
        /// TSParameters object, using the input parameters given to the method.
        /// </summary>
        public void SetParametersIrregular(
                    int timeStepCount, DateTime blobStartDate, DateTime blobEndDate)
        {
            // Most of the parameters are straightforward
            TimeStepUnit = TSDateCalculator.TimeStepUnitCode.Irregular;
            TimeStepQuantity = 0;
            TimeStepCount = timeStepCount;
            BlobStartDate = blobStartDate;
            BlobEndDate = blobEndDate;
        }
    
    
    }


}
